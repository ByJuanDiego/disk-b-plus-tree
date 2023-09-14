#include "bplustree.hpp"

template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::insert(int64 seek_page, DataPageType type,
                                                            RecordType &record) -> InsertStatus {
    if (type == dataPage) {
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        data_page.template sorted_insert<KeyType, Greater>(record, get_indexed_field(record), greater_to);
        return { data_page.num_records };
    }

    IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
    seek_all(b_plus_index_file, seek_page);
    index_page.read(b_plus_index_file);

    int child_pos = 0;
    while (child_pos < index_page.num_keys && greater_to(get_indexed_field(record), index_page.keys[child_pos])) {
        ++child_pos;
    }

    DataPageType children_type = index_page.points_to_leaf;
    int64 child_seek = index_page.children[child_pos];
    InsertStatus prev_page_status = this->insert(child_seek, children_type, record);

    // Conditionally splits a data page if it's full
    if (children_type == dataPage && (prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
        // Load the full data page from disk
        DataPage<RecordType> full_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, child_seek);
        full_page.read(b_plus_index_file);

        // Create a new data page to accommodate the split
        DataPage<RecordType> new_page = split_data_page(full_page);

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
        KeyType new_index_page_key = get_indexed_field(full_page.max_record);

        // Update references to data pages in the parent index page
        reallocate_references_to_data_pages(index_page, child_pos, new_index_page_key, new_page_seek);

        // Seek to the location of the parent index page in the index file and update it
        seek_all(b_plus_index_file, seek_page);
        index_page.write(b_plus_index_file);
    }
    else if (children_type == indexPage && (prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
        IndexPage<KeyType> full_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, child_seek);
        full_page.read(b_plus_index_file);

        KeyType new_index_page_key {};
        IndexPage<KeyType> new_page = split_index_page(full_page, new_index_page_key);

        seek_all(b_plus_index_file, 0, std::ios::end);
        int64 new_page_seek = b_plus_index_file.tellp();
        new_page.write(b_plus_index_file);

        index_page.template sorted_insert<KeyType, Greater>(new_index_page_key, new_page_seek, greater_to);

        seek_all(b_plus_index_file, child_seek);
        full_page.write(b_plus_index_file);

        reallocate_references_to_index_pages(index_page, child_pos, new_index_page_key, new_page_seek);

        // Seek to the location of the parent index page in the index file and update it
        seek_all(b_plus_index_file, seek_page);
        index_page.write(b_plus_index_file);
    }

    return { indexPage, seek_page, index_page.num_keys };
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
BPlusTree<KeyType, RecordType, Greater, Index>::BPlusTree(const Property &property, Index index, Greater greater)

        : greater_to(greater), get_indexed_field(index) {
    metadata_json = property.json_value();
    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::in);

    // if the metadata file cannot be opened, creates the index
    if (!metadata_file.good()) {
        b_plus_index_file.close();
        create_index();
        return;
    }
    // otherwise, just loads the metadata in RAM
    load_metadata();
    close(b_plus_index_file);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::insert(RecordType &record) -> void {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);

    auto root_page_type = static_cast<DataPageType>(metadata_json[ROOT_STATUS].asInt());

    if (root_page_type == emptyPage) {
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        data_page.push_back(record);
        metadata_json[SEEK_ROOT] = INITIAL_PAGE;
        b_plus_index_file.seekp(INITIAL_PAGE);
        data_page.write(b_plus_index_file);
    } else {
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
            IndexPage<KeyType> new_page = split_index_page(full_root, new_root_key);

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
auto BPlusTree<KeyType, RecordType, Greater, Index>::search(KeyType &key) -> std::vector<RecordType> {

    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    int64 seek_page = locate_data_page(key);

    std::vector<RecordType> located_records;
    DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());

    while (seek_page != NULL_PAGE) {
        b_plus_index_file.seekg(seek_page);
        data_page.read(b_plus_index_file);

        for (int i = 0; i < data_page.num_records; ++i) {
            if (greater_to(key, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (greater_to(get_indexed_field(data_page.records[i])), key) {
                if (located_records.empty()) {
                    throw KeyNotFound();
                }
                b_plus_index_file.close();
                return located_records;
            }
            located_records.push_back(data_page.records[i]);
        }
        seek_page = data_page.next_leaf;
    }

    close(b_plus_index_file);
    return located_records;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto BPlusTree<KeyType, RecordType, Greater, Index>::between(KeyType &lower_bound,
                                                             KeyType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH], std::ios::in);
    int64 seek_page = locate_data_page(lower_bound);

    std::vector<RecordType> located_records;
    DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());

    while (seek_page != NULL_PAGE) {
        b_plus_index_file.seekg(seek_page);
        data_page.read(b_plus_index_file);

        for (int i = 0; i < data_page.num_records; ++i) {
            if (greater_to(lower_bound, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (greater_to(get_indexed_field(data_page.records[i])), upper_bound) {
                b_plus_index_file.close();
                return located_records;
            }
            located_records.push_back(data_page.records[i]);
        }
        seek_page = data_page.next_leaf;
    }

    close(b_plus_index_file);
    return located_records;
}
