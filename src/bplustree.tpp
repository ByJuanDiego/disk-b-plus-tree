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
            return emptyPage;
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
                index_page.load(seek_page);
                std::int32_t child_pos = 0;
                while ((child_pos < index_page.len()) && gt(key, index_page.keys[child_pos])) {
                    ++child_pos;
                }
                seek_page = index_page.children[child_pos];
            } while (!index_page.points_to_leaf);

            return seek_page;
        }
    }
    throw LogicError();
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::insert(std::streampos seek_page, PageType type,
                                                            RecordType &record) -> InsertResult {
    // When a data page is found, proceeds to insert the record in-order and then resend the page to disk
    if (type == dataPage) {
        DataPage<INDEX_TYPE> data_page(this);
        data_page.load(seek_page);
        data_page.sorted_insert(record);
        data_page.save(seek_page);
        return InsertResult { data_page.len() };
    }

    // Otherwise, the index page is iterated to locate the right child to descend the tree.
    // This procedure is often done recursively since the height of the tree increases when inserting records.
    IndexPage<INDEX_TYPE> index_page(this);
    index_page.load(seek_page);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.len() && gt(get_indexed_field(record), index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    InsertResult prev_page_status = this->insert(child_seek, children_type, record);

    // Conditionally splits a page if it's full
    std::shared_ptr<Page<INDEX_TYPE>> child = nullptr;
    if (children_type == dataPage && (prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
        child = std::make_shared<DataPage<INDEX_TYPE>>(this);
    } else if (children_type == indexPage && (prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
        child = std::make_shared<IndexPage<INDEX_TYPE>>(this);
    }

    if (child) {
        child->load(child_seek);
        child->balance_page_insert(seek_page, index_page, child_pos);
    }

    // We track the current number of keys to the upcoming state, so it can handle the logic for the page split
    return InsertResult { index_page.len() };
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
        data_page.save(std::ios::beg);

        metadata_json[SEEK_ROOT] = 0;
        metadata_json[ROOT_STATUS] = dataPage;
    } else {
        // Attempt to insert the new record into the B+ tree.
        std::streampos seek_root = metadata_json[SEEK_ROOT].asLargestInt();
        InsertResult result = this->insert(seek_root, root_page_type, record);

        // At the end of the recursive calls generated above, we must check (as a base case) if the root page is full
        // and needs to be split.
        std::shared_ptr<Page<INDEX_TYPE>> root = nullptr;
        if (root_page_type == dataPage && (result.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
            root = std::make_shared<DataPage<INDEX_TYPE>>(this);
        }
        else if (root_page_type == indexPage && (result.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
            root = std::make_shared<IndexPage<INDEX_TYPE>>(this);
        }

        if (root) {
            std::streampos old_root_seek = metadata_json[SEEK_ROOT].asLargestInt();
            root->load(old_root_seek);
            root->balance_root_insert(old_root_seek);
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
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        data_page.load(seek_page);

        for (int i = 0; i < data_page.len(); ++i) {
            if (gt(key, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_indexed_field(data_page.records[i]), key)) {
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
auto BPlusTree<INDEX_TYPE>::above(const KeyType &lower_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    std::streampos seek_page = locate_data_page(lower_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = 0; i < data_page.len(); ++i) {
            if (gt(lower_bound, get_indexed_field(data_page.records[i]))) {
                continue;
            }
            located_records.push_back(data_page.records[i]);
        }

        seek_page = data_page.next_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::below(const KeyType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    std::streampos seek_page = locate_data_page(upper_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = data_page.len() - 1; i >= 0; --i) {
            if (gt(get_indexed_field(data_page.records[i]), upper_bound)) {
                continue;
            }
            located_records.push_back(data_page.records[i]);
        }

        seek_page = data_page.prev_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::between(const KeyType &lower_bound,
                                    const KeyType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
    std::streampos seek_page = locate_data_page(lower_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<INDEX_TYPE> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = 0; i < data_page.len(); ++i) {
            if (gt(lower_bound,  get_indexed_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_indexed_field(data_page.records[i]), upper_bound)) {
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
        root->load(seek_root);
        root->balance_root_remove();
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
        data_page.load(seek_page);
        std::shared_ptr<KeyType> predecessor = data_page.remove(key);
        data_page.save(seek_page);
        return RemoveResult<KeyType> { data_page.len(), predecessor };
    }

    IndexPage<INDEX_TYPE> index_page(this);
    index_page.load(seek_page);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.len() && gt(key, index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    RemoveResult<KeyType> result = this->remove(child_seek, children_type, key);

    if (child_pos < index_page.len() && result.predecessor && !gt(index_page.keys[child_pos], key)) {
        index_page.keys[child_pos] = *result.predecessor;
        index_page.save(seek_page);
        result.predecessor = nullptr;
    }

    std::shared_ptr<Page<INDEX_TYPE>> child;
    if (children_type == dataPage) {
        child = std::make_shared<DataPage<INDEX_TYPE>>(this);
    } else {
        child = std::make_shared<IndexPage<INDEX_TYPE>>(this);
    }

    child->load(child_seek);
    child->balance_page_remove(seek_page, index_page, child_pos);
    return RemoveResult<KeyType> { index_page.len(), result.predecessor };
}
