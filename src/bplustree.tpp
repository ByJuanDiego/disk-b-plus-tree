//
// Created by juan diego on 9/15/23.
//


#include "bplustree.hpp"


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::create_index() -> void {
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


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::load_metadata() -> void {
    metadata_file >> metadata_json;
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::save_metadata() -> void {
    Json::StyledWriter styledWriter;
    metadata_file << styledWriter.write(metadata_json);
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::locate_data_page(const KeyType &key) -> std::streampos {
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
            std::streampos seek_page = metadata_json[SEEK_ROOT].asInt();
            IndexPage<INDEX_TYPE> index_page(this);

            do {
                seek(b_plus_index_file, seek_page);
                index_page.read(b_plus_index_file);
                std::int32_t child_pos = 0;
                while ((child_pos < index_page.num_keys) && gt(key, index_page.keys[child_pos])) {
                    ++child_pos;
                }
                seek_page = index_page.children[child_pos];
            } while (!index_page.points_to_leaf);

            return seek_page;
        }
    }
    throw KeyNotFound();
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::insert(std::streampos seek_page, PageType type,
                                                            RecordType &record) -> InsertResult {
    // When a data page is found, proceeds to insert the record in-order and then resend the page to disk
    if (type == dataPage) {
        DataPage<INDEX_TYPE> data_page(this);
        seek(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        data_page.sorted_insert(record);
        seek(b_plus_index_file, seek_page);
        data_page.write(b_plus_index_file);
        return InsertResult { data_page.num_records };
    }

    // Otherwise, the index page is iterated to locate the right child to descend the tree.
    // This procedure is often done recursively since the height of the tree increases when inserting records.
    IndexPage<INDEX_TYPE> index_page(this);
    seek(b_plus_index_file, seek_page);
    index_page.read(b_plus_index_file);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.num_keys && gt(get_indexed_field(record), index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    InsertResult prev_page_status = this->insert(child_seek, children_type, record);

    // Conditionally splits a page if it's full
    if (children_type == dataPage && (prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
        balance_data_page(index_page, child_pos, seek_page, child_seek);
    } else if (children_type == indexPage && (prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
        balance_index_page(index_page, child_pos, seek_page, child_seek);
    }

    // We track the current number of keys to the upcoming state, so it can handle the logic for the page split
    return InsertResult { index_page.num_keys };
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::balance_index_page(IndexPage<INDEX_TYPE>& index_page,
                                                                        std::int32_t child_pos,
                                                                        std::streampos seek_page,
                                                                        std::streampos child_seek) -> void {
    IndexPage<INDEX_TYPE> full_page(this);
    seek(b_plus_index_file, child_seek);
    full_page.read(b_plus_index_file);

    SplitResult<INDEX_TYPE> split = full_page.split(metadata_json[INDEX_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<IndexPage<INDEX_TYPE>>(split.new_page);

    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = b_plus_index_file.tellp();
    new_page->write(b_plus_index_file);

    seek(b_plus_index_file, child_seek);
    full_page.write(b_plus_index_file);

    index_page.reallocate_references_after_split(child_pos, split.split_key, new_page_seek);

    // Seek to the location of the parent index page in the index file and update it
    seek(b_plus_index_file, seek_page);
    index_page.write(b_plus_index_file);
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::balance_data_page(IndexPage<INDEX_TYPE>& index_page,
                                                                       std::int32_t child_pos,
                                                                       std::streampos seek_page,
                                                                       std::streampos child_seek) -> void {
    // Load the full data page from disk
    DataPage<INDEX_TYPE> full_page(this);
    seek(b_plus_index_file, child_seek);
    full_page.read(b_plus_index_file);

    // Create a new data page to accommodate the split
    SplitResult<INDEX_TYPE> split = full_page.split(metadata_json[DATA_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<DataPage<INDEX_TYPE>>(split.new_page);

    // Seek to the end of the B+Tree index file to append the new page
    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = b_plus_index_file.tellp();

    // Set the previous leaf pointer of the new page
    new_page->prev_leaf = child_seek;

    std::streampos next_leaf_seek = full_page.next_leaf;
    if (next_leaf_seek != emptyPage) {
        new_page->next_leaf = next_leaf_seek;

        DataPage<INDEX_TYPE> next_page(this);
        seek(b_plus_index_file, next_leaf_seek);
        next_page.read(b_plus_index_file);
        next_page.prev_leaf = new_page_seek;
        seek(b_plus_index_file, next_leaf_seek);
        next_page.write(b_plus_index_file);
    }

    // Write the new data page to the B+Tree index file
    seek(b_plus_index_file, new_page_seek);
    new_page->write(b_plus_index_file);

    // Update the next leaf pointer of the old page to point to the new page
    full_page.next_leaf = new_page_seek;

    // Seek to the location of the old page in the index file and update it
    seek(b_plus_index_file, child_seek);
    full_page.write(b_plus_index_file);

    // Update references to data pages in the parent index page
    index_page.reallocate_references_after_split(child_pos, split.split_key, new_page_seek);

    // Seek to the location of the parent index page in the index file and update it
    seek(b_plus_index_file, seek_page);
    index_page.write(b_plus_index_file);
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::balance_root_data_page() -> void {
    DataPage<INDEX_TYPE> old_root(this);

    std::streampos old_root_seek = metadata_json[SEEK_ROOT].asLargestInt();
    seek(b_plus_index_file, old_root_seek);
    old_root.read(b_plus_index_file);

    SplitResult<INDEX_TYPE> split = old_root.split(metadata_json[DATA_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<DataPage<INDEX_TYPE>>(split.new_page);

    new_page->prev_leaf = old_root_seek;
    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = b_plus_index_file.tellp();
    new_page->write(b_plus_index_file);

    old_root.next_leaf = new_page_seek;
    seek(b_plus_index_file, old_root_seek);
    old_root.write(b_plus_index_file);

    IndexPage<INDEX_TYPE> new_root(this);
    new_root.keys[0] = split.split_key;
    new_root.children[0] = old_root_seek;
    new_root.children[1] = new_page_seek;
    new_root.num_keys = 1;

    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_root_seek = b_plus_index_file.tellp();
    new_root.write(b_plus_index_file);

    metadata_json[SEEK_ROOT] = static_cast<Json::Int64>(new_root_seek);
    metadata_json[ROOT_STATUS] = indexPage;
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::balance_root_index_page() -> void {
    std::streampos old_root_seek = metadata_json[SEEK_ROOT].asLargestInt();
    IndexPage<INDEX_TYPE> old_root(this);
    seek(b_plus_index_file, old_root_seek);
    old_root.read(b_plus_index_file);

    SplitResult<INDEX_TYPE> split = old_root.split(metadata_json[INDEX_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<IndexPage<INDEX_TYPE>>(split.new_page);
    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = b_plus_index_file.tellp();
    new_page->write(b_plus_index_file);

    seek(b_plus_index_file, old_root_seek);
    old_root.write(b_plus_index_file);

    IndexPage<INDEX_TYPE> new_root(this, false);
    new_root.num_keys = 1;
    new_root.keys[0] = split.split_key;
    new_root.children[0] = old_root_seek;
    new_root.children[1] = new_page_seek;

    seek(b_plus_index_file, 0, std::ios::end);
    std::streampos new_root_seek = b_plus_index_file.tellp();
    new_root.write(b_plus_index_file);

    metadata_json[SEEK_ROOT] = static_cast<Json::Int64>(new_root_seek);
}


template<DEFINE_INDEX_TYPE>
BPlusTree<INDEX_TYPE>::BPlusTree(const Property &property, Index index, Greater greater)
        : gt(greater), get_indexed_field(index) {
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


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::insert(RecordType &record) -> void {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);
    auto root_page_type = static_cast<PageType>(metadata_json[ROOT_STATUS].asInt());

    if (root_page_type == emptyPage) {
        DataPage<INDEX_TYPE> data_page(this);
        data_page.push_back(record);
        seek(b_plus_index_file, 0);
        data_page.write(b_plus_index_file);
        metadata_json[SEEK_ROOT] = 0;
        metadata_json[ROOT_STATUS] = dataPage;
    } else {
        // Attempt to insert the new record into the B+ tree.
        std::streampos seek_root = metadata_json[SEEK_ROOT].asLargestInt();
        InsertResult result = this->insert(seek_root, root_page_type, record);

        // At the end of the recursive calls generated above, we must check (as a base case) if the root page is full
        // and needs to be split.
        if (root_page_type == dataPage && (result.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
            balance_root_data_page();
        }
        else if (root_page_type == indexPage && (result.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
            balance_root_index_page();
        }
    }

    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::out);
    save_metadata();

    close(metadata_file);
    close(b_plus_index_file);
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::search(const KeyType &key) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    std::streampos seek_page = locate_data_page(key);

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        seek(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);

        for (int i = 0; i < data_page.num_records; ++i) {
            if (gt(key, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_indexed_field(data_page.records[i]), key)) {
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


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::between(const KeyType &lower_bound,
                                    const KeyType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    std::streampos seek_page = locate_data_page(lower_bound);

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        seek(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        for (std::int32_t i = 0; i < data_page.num_records; ++i) {
            if (gt(lower_bound,  get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_indexed_field(data_page.records[i]), upper_bound)) {
                close(b_plus_index_file);
                if (located_records.empty()) {
                    throw KeyNotFound();
                }
                return located_records;
            }
            located_records.push_back(data_page.records[i]);
        }
        seek_page = data_page.next_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    if (located_records.empty()) {
        throw KeyNotFound();
    }
    return located_records;
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::remove(const KeyType &key) -> void {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);
    auto root_page_type = static_cast<PageType>(metadata_json[ROOT_STATUS].asInt());

    if (root_page_type == emptyPage) {
        close(b_plus_index_file);
        throw KeyNotFound();
    }

    std::streampos seek_root = metadata_json[SEEK_ROOT].asInt();
    RemoveResult<KeyType> result = this->remove(seek_root, root_page_type, key);

    if (result.size == 0) {
        std::shared_ptr<Page<INDEX_TYPE>> root;
        if (root_page_type == indexPage) {
            root = std::make_shared<IndexPage<INDEX_TYPE>>(this);
        } else {
            root = std::make_shared<DataPage<INDEX_TYPE>>(this);
        }
        root->deallocate_root();
    }

    open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::out);
    save_metadata();

    close(metadata_file);
    close(b_plus_index_file);
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::remove(std::streampos seek_page, PageType type,
                                                            const KeyType& key) -> RemoveResult<KeyType> {
    if (type == dataPage) {
        DataPage<INDEX_TYPE> data_page(this);
        seek(b_plus_index_file, seek_page);
        data_page.read(b_plus_index_file);
        std::shared_ptr<KeyType> predecessor = data_page.remove(key);
        seek(b_plus_index_file, seek_page);
        data_page.write(b_plus_index_file);
        return RemoveResult<KeyType> { data_page.len(), predecessor };
    }

    IndexPage<INDEX_TYPE> index_page(this);
    seek(b_plus_index_file, seek_page);
    index_page.read(b_plus_index_file);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.num_keys && gt(key, index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    RemoveResult<KeyType> result = remove(child_seek, children_type, key);

    if (child_pos < index_page.num_keys && result.predecessor && !gt(index_page.keys[child_pos], key)) {
        index_page.keys[child_pos] = *result.predecessor;
        seek(b_plus_index_file, seek_page);
        index_page.write(b_plus_index_file);
        result.predecessor = nullptr;
    }

    std::shared_ptr<Page<INDEX_TYPE>> child;
    if (children_type == dataPage) {
        child = std::make_shared<DataPage<INDEX_TYPE>>(this);
    } else {
        child = std::make_shared<IndexPage<INDEX_TYPE>>(this);
    }

    seek(b_plus_index_file, child_seek);
    child->read(b_plus_index_file);
    child->balance(seek_page, index_page, child_pos);
    return RemoveResult<KeyType> { index_page.len(), result.predecessor };
}
