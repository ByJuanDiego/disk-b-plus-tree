//
// Created by juan diego on 9/15/23.
//


#include "bplustree.hpp"
#include <iostream>

template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::load_metadata() -> void {
    metadata_file >> metadata_json;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::save_metadata() -> void {
    metadata_file << metadata_json;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::open(std::fstream &file, const std::string &file_name,
                                                          std::ios::openmode mode_flags) -> void {

    file.open(file_name, mode_flags);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::close(std::fstream &file) -> void {
    file.close();
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::seek_all(std::fstream &file, int64 pos,
                                                              std::ios::seekdir offset) -> void {
    file.seekg(pos, offset);
    file.seekp(pos, offset);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::locate_data_page(const KeyType &key) -> int64 {
    switch (metadata_json[ROOT_STATUS].asInt()) {
        case emptyPage: {
            throw KeyNotFound();
        }
        case dataPage: {
            return metadata_json[SEEK_ROOT].asInt();
        }
        case indexPage: {
            // iterates through the index pages and descends the B+ in order to locate the first data page
            // that may contain the key to search.
            int64 seek_page = metadata_json[SEEK_ROOT].asInt();
            IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());

            do {
                seek_all(b_plus_index_file, seek_page);
                index_page.read(b_plus_index_file);
                int child_pos = 0;
                while ((child_pos < index_page.num_keys) && greater_to(key, index_page.keys[child_pos])) {
                    ++child_pos;
                }
                seek_page = index_page.children[child_pos];
            } while (!index_page.points_to_leaf);

            return seek_page;
        }
    }
    throw KeyNotFound();
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::create_index() -> void {
    // first verifies if the directory path exists and creates it if not exists.
    if (!directory_exists(metadata_json[DIRECTORY_PATH].asString())) {
        bool successfully_created = create_directory(metadata_json[DIRECTORY_PATH].asString());
        if (!successfully_created) {
            throw CreateDirectoryError();
        }
    }

    // then, opens the metadata file and writes the JSON metadata.
    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::out);
    if (!metadata_file.is_open()) {
        throw CreateFileError();
    }
    save_metadata();
    close(metadata_file);

    // finally, creates an empty file for the B+
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::out);
    if (!b_plus_index_file.is_open()) {
        throw CreateFileError();
    }

    close(b_plus_index_file);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::insert(int64 seek_page, PageType type,
                                                            RecordType &record) -> InsertStatus {
    if (type == dataPage) {
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        data_page.template sorted_insert<KeyType, Greater, Index>(record, greater_to, get_indexed_field);
        seek_all(b_plus_index_file, seek_page);
        data_page.write(b_plus_index_file);
        return InsertStatus {data_page.num_records};
    }

    IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
    seek_all(b_plus_index_file, seek_page);
    index_page.read(b_plus_index_file);

    int child_pos = 0;
    while (child_pos < index_page.num_keys && greater_to(get_indexed_field(record), index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    int64 child_seek = index_page.children[child_pos];
    InsertStatus prev_page_status = this->insert(child_seek, children_type, record);

    // Conditionally splits a data page if it's full
    if (children_type == dataPage && (prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
        // Load the full data page from disk
        DataPage<RecordType> full_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, child_seek);
        full_page.read(b_plus_index_file);

        // Create a new data page to accommodate the split
        DataPage<RecordType> new_page = full_page.split(metadata_json[MINIMUM_DATA_PAGE_RECORDS].asInt());


        // Seek to the end of the B+Tree index file to append the new page
        seek_all(b_plus_index_file, 0, std::ios::end);
        int64 new_page_seek = b_plus_index_file.tellp();

        // Set the previous leaf pointer of the new page
        new_page.prev_leaf = child_seek;

        // Write the new data page to the B+Tree index file
        new_page.write(b_plus_index_file);

        // Update the next leaf pointer of the old page to point to the new page
        full_page.next_leaf = new_page_seek;

        // Seek to the location of the old page in the index file and update it
        seek_all(b_plus_index_file, child_seek);
        full_page.write(b_plus_index_file);

        // Calculate the key to insert in the parent index page
        RecordType max_record = full_page.max_record();
        KeyType new_index_page_key = get_indexed_field(max_record);

        // Update references to data pages in the parent index page
        index_page.reallocate_references_to_data_pages(child_pos, new_index_page_key, new_page_seek);

        // Seek to the location of the parent index page in the index file and update it
        seek_all(b_plus_index_file, seek_page);
        index_page.write(b_plus_index_file);
    }
    else if (children_type == indexPage && (prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
        IndexPage<KeyType> full_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, child_seek);
        full_page.read(b_plus_index_file);

        KeyType new_index_page_key {};
        IndexPage<KeyType> new_page = full_page.split(metadata_json[MINIMUM_INDEX_PAGE_KEYS].asInt(), new_index_page_key);

        seek_all(b_plus_index_file, 0, std::ios::end);
        int64 new_page_seek = b_plus_index_file.tellp();
        new_page.write(b_plus_index_file);

        index_page.template sorted_insert<Greater>(new_index_page_key, new_page_seek, greater_to);

        seek_all(b_plus_index_file, child_seek);
        full_page.write(b_plus_index_file);

        index_page.reallocate_references_to_index_pages(child_pos, new_index_page_key, new_page_seek);

        // Seek to the location of the parent index page in the index file and update it
        seek_all(b_plus_index_file, seek_page);
        index_page.write(b_plus_index_file);
    }

    return InsertStatus {index_page.num_keys};
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
BPlusTree<KeyType, RecordType, Greater, Index>::BPlusTree(const Property &property, Index index, Greater greater)
        : greater_to(greater), get_indexed_field(index) {
    metadata_json = property.json_value();
    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::in);

    // if the metadata file cannot be opened, creates the index
    if (!metadata_file.good()) {
        close(metadata_file);
        create_index();
        return;
    }
    // otherwise, just loads the metadata in RAM
    load_metadata();
    close(metadata_file);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::insert(RecordType &record) -> void {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);
    auto root_page_type = static_cast<PageType>(metadata_json[ROOT_STATUS].asInt());

    if (root_page_type == emptyPage) {
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        data_page.push_back(record);
        seek_all(b_plus_index_file, 0);
        data_page.write(b_plus_index_file);
        metadata_json[SEEK_ROOT] = 0;
        metadata_json[ROOT_STATUS] = dataPage;
    }
    else {
        // Attempt to insert the new record into the B+ tree.
        InsertStatus status = this->insert(metadata_json[SEEK_ROOT].asLargestInt(), root_page_type, record);

        // Check if the root page is full and needs to be split.
        if (status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt()) {
            // Save the seek position of the old root page for reference.
            int64 old_root_seek = metadata_json[SEEK_ROOT].asLargestInt();

            // Create a new index page with the defined maximum capacity.
            IndexPage<KeyType> full_root(metadata_json[INDEX_PAGE_CAPACITY].asInt());

            // Read the contents of the old root page from the index file.
            seek_all(b_plus_index_file, old_root_seek);
            full_root.read(b_plus_index_file);

            // Initialize variables for the new root page and key.
            KeyType new_root_key {};

            // Split the old root page into two pages, with new data in 'new_page'.
            IndexPage<KeyType> new_page = full_root.split(metadata_json[MINIMUM_INDEX_PAGE_KEYS].asInt(), new_root_key);

            // Save the seek position of the new page in the index file.
            seek_all(b_plus_index_file, 0, std::ios::end);
            int64 new_page_seek = b_plus_index_file.tellp();

            // Write the new page to the index file.
            new_page.write(b_plus_index_file);

            // Write the updated old root page back to the index file.
            seek_all(b_plus_index_file, old_root_seek);
            full_root.write(b_plus_index_file);

            // Create a new root page with the same maximum capacity.
            IndexPage<KeyType> new_root(metadata_json[INDEX_PAGE_CAPACITY].asInt());

            // Update the new root page with information about the new root key and children.
            new_root.num_keys = 1;
            new_root.keys[0] = new_root_key;
            new_root.children[0] = old_root_seek;
            new_root.children[1] = new_page_seek;

            // Save the seek position of the new root page in the index file.
            seek_all(b_plus_index_file, 0, std::ios::end);
            int64 new_root_seek = b_plus_index_file.tellp();

            // Write the new root page to the index file.
            new_root.write(b_plus_index_file);

            // Update metadata to point to the seek position of the new root page.
            metadata_json[SEEK_ROOT] = new_root_seek;
        }
    }

    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::out);
    save_metadata();
    close(metadata_file);

    close(b_plus_index_file);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::search(const KeyType &key) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    int64 seek_page = locate_data_page(key);

    std::vector<RecordType> located_records;
    DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());

    do {
        seek_all(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);

        for (int i = 0; i < data_page.num_records; ++i) {
            if (greater_to(key, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (greater_to(get_indexed_field(data_page.records[i])), key) {
                if (located_records.empty()) {
                    throw KeyNotFound();
                }
                close(b_plus_index_file);
                return located_records;
            }
            located_records.push_back(data_page.records[i]);
        }
        seek_page = data_page.next_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::between(const KeyType &lower_bound,
                                                             const KeyType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    int64 seek_page = locate_data_page(lower_bound);

    std::vector<RecordType> located_records;
    DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());

    do {
        seek_all(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        for (int i = 0; i < data_page.num_records; ++i) {
            if (greater_to(lower_bound, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (greater_to(get_indexed_field(data_page.records[i]), upper_bound)) {
                b_plus_index_file.close();
                return located_records;
            }
            located_records.push_back(data_page.records[i]);
        }
        seek_page = data_page.next_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::remove(KeyType &key) -> void {
    // TODO
}
