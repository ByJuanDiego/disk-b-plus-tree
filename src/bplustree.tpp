//
// Created by juan diego on 9/15/23.
//


#include "bplustree.hpp"


template<TYPES(typename)>
auto BPlusTree<TYPES()>::create_index() -> void {
    // first verifies if the directory path exists and creates it if not exists.
    if (!directory_exists(properties.DIRECTORY_PATH)) {
        bool successfully_created = create_directory(properties.DIRECTORY_PATH);
        if (!successfully_created) {
            throw CreateDirectoryError();
        }
    }

    // then, opens the metadata file and writes the JSON metadata.
    open(metadata_file, properties.METADATA_FULL_PATH, std::ios::out);
    if (!metadata_file.is_open()) {
        throw CreateFileError();
    }

    properties.save(metadata_file);
    close(metadata_file);

    // finally, creates an empty file for the B+
    open(b_plus_index_file,properties.INDEX_FULL_PATH, std::ios::out);
    if (!b_plus_index_file.is_open()) {
        throw CreateFileError();
    }

    close(b_plus_index_file);
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::locate_data_page(const FieldType &key) -> std::streampos {
    switch (properties.ROOT_STATUS) {
        case emptyPage: {
            return emptyPage;
        }
        case dataPage: {
            return properties.SEEK_ROOT;
        }
        case indexPage: {
            // iterates through the index pages and descends the B+ in order to locate the first data page
            // that may contain the key to search.
            std::streampos seek_page = properties.SEEK_ROOT;
            IndexPage<TYPES()> index_page(this);

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


template<TYPES(typename)>
auto BPlusTree<TYPES()>::insert(std::streampos seek_page, PageType type,
                                                            RecordType &record) -> InsertResult {
    // When a data page is found, proceeds to insert the record in-order and then resend the page to disk
    if (type == dataPage) {
        DataPage<TYPES()> data_page(this);
        data_page.load(seek_page);
        data_page.sorted_insert(record);
        data_page.save(seek_page);
        return InsertResult { data_page.len() };
    }

    // Otherwise, the index page is iterated to locate the right child to descend the tree.
    // This procedure is often done recursively since the height of the tree increases when inserting records.
    IndexPage<TYPES()> index_page(this);
    index_page.load(seek_page);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.len() && gt(get_search_field(record), index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    InsertResult prev_page_status = this->insert(child_seek, children_type, record);

    // Conditionally splits a page if it's full
    std::shared_ptr<Page<TYPES()>> child = nullptr;
    if (children_type == dataPage && (prev_page_status.size == properties.MAX_DATA_PAGE_CAPACITY)) {
        child = std::make_shared<DataPage<TYPES()>>(this);
    } else if (children_type == indexPage && (prev_page_status.size == properties.MAX_INDEX_PAGE_CAPACITY)) {
        child = std::make_shared<IndexPage<TYPES()>>(this);
    }

    if (child) {
        child->load(child_seek);
        child->balance_page_insert(seek_page, index_page, child_pos);
    }

    // We track the current number of keys to the upcoming state, so it can handle the logic for the page split
    return InsertResult { index_page.len() };
}


template<TYPES(typename)>
BPlusTree<TYPES()>::BPlusTree(Property property, FieldMapping search_field, Compare greater)
        : gt(greater), get_search_field(search_field), properties(std::move(property)) {
    open(metadata_file, properties.METADATA_FULL_PATH, std::ios::in);

    // if the metadata file cannot be opened, creates the index
    if (!metadata_file.good()) {
        close(metadata_file);
        create_index();
        return;
    }
    // otherwise, just loads the metadata in RAM
    properties.load(metadata_file);
    close(metadata_file);
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::insert(RecordType &record) -> void {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in | std::ios::out);
    auto root_page_type = static_cast<PageType>(properties.ROOT_STATUS);

    if (root_page_type == emptyPage) {
        DataPage<TYPES()> data_page(this);
        data_page.push_back(record);
        data_page.save(std::ios::beg);

        properties.SEEK_ROOT = 0;
        properties.ROOT_STATUS = dataPage;
    } else {
        // Attempt to insert the new record into the B+ tree.
        std::streampos seek_root = properties.SEEK_ROOT;
        InsertResult result = this->insert(seek_root, root_page_type, record);

        // At the end of the recursive calls generated above, we must check (as a base case) if the root page is full
        // and needs to be split.
        std::shared_ptr<Page<TYPES()>> root = nullptr;
        if (root_page_type == dataPage && (result.size == properties.MAX_DATA_PAGE_CAPACITY)) {
            root = std::make_shared<DataPage<TYPES()>>(this);
        }
        else if (root_page_type == indexPage && (result.size == properties.MAX_INDEX_PAGE_CAPACITY)) {
            root = std::make_shared<IndexPage<TYPES()>>(this);
        }

        if (root) {
            root->load(seek_root);
            root->balance_root_insert(seek_root);
        }
    }

    open(metadata_file, properties.METADATA_FULL_PATH, std::ios::out);
    properties.save(metadata_file);

    close(metadata_file);
    close(b_plus_index_file);
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::search(const FieldType &key) -> std::vector<RecordType> {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in);
    std::streampos seek_page = locate_data_page(key);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<TYPES()> data_page(this);

    do {
        data_page.load(seek_page);

        for (int i = 0; i < data_page.len(); ++i) {
            if (gt(key, get_search_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_search_field(data_page.records[i]), key)) {
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


template<TYPES(typename)>
auto BPlusTree<TYPES()>::above(const FieldType &lower_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in);
    std::streampos seek_page = locate_data_page(lower_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<TYPES()> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = 0; i < data_page.len(); ++i) {
            if (gt(lower_bound, get_search_field(data_page.records[i]))) {
                continue;
            }
            located_records.push_back(data_page.records[i]);
        }

        seek_page = data_page.next_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::below(const FieldType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in);
    std::streampos seek_page = locate_data_page(upper_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<TYPES()> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = data_page.len() - 1; i >= 0; --i) {
            if (gt(get_search_field(data_page.records[i]), upper_bound)) {
                continue;
            }
            located_records.push_back(data_page.records[i]);
        }

        seek_page = data_page.prev_leaf;
    } while (seek_page != emptyPage);

    close(b_plus_index_file);
    return located_records;
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::between(const FieldType &lower_bound,
                                    const FieldType &upper_bound) -> std::vector<RecordType> {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in);
    std::streampos seek_page = locate_data_page(lower_bound);
    if (seek_page == emptyPage) {
        return {};
    }

    std::vector<RecordType> located_records;
    DataPage<TYPES()> data_page(this);

    do {
        data_page.load(seek_page);

        for (std::int32_t i = 0; i < data_page.len(); ++i) {
            if (gt(lower_bound,  get_search_field(data_page.records[i]))) {
                continue;
            }
            if (gt(get_search_field(data_page.records[i]), upper_bound)) {
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


template<TYPES(typename)>
auto BPlusTree<TYPES()>::remove(const FieldType &key) -> void {
    open(b_plus_index_file, properties.INDEX_FULL_PATH, std::ios::in | std::ios::out);
    auto root_page_type = static_cast<PageType>(properties.ROOT_STATUS);

    if (root_page_type == emptyPage) {
        close(b_plus_index_file);
        throw KeyNotFound();
    }

    std::streampos seek_root = properties.SEEK_ROOT;
    RemoveResult<FieldType> result = this->remove(seek_root, root_page_type, key);

    if (result.size == 0) {
        std::shared_ptr<Page<TYPES()>> root;
        if (root_page_type == indexPage) {
            root = std::make_shared<IndexPage<TYPES()>>(this);
        } else {
            root = std::make_shared<DataPage<TYPES()>>(this);
        }
        root->load(seek_root);
        root->balance_root_remove();
    }

    open(metadata_file, properties.METADATA_FULL_PATH, std::ios::out);
    properties.save(metadata_file);

    close(metadata_file);
    close(b_plus_index_file);
}


template<TYPES(typename)>
auto BPlusTree<TYPES()>::remove(std::streampos seek_page, PageType type,
                                                            const FieldType& key) -> RemoveResult<FieldType> {
    if (type == dataPage) {
        DataPage<TYPES()> data_page(this);
        data_page.load(seek_page);
        std::shared_ptr<FieldType> predecessor = data_page.remove(key);
        data_page.save(seek_page);
        return RemoveResult<FieldType> { data_page.len(), predecessor };
    }

    IndexPage<TYPES()> index_page(this);
    index_page.load(seek_page);

    std::int32_t child_pos = 0;
    while (child_pos < index_page.len() && gt(key, index_page.keys[child_pos])) {
        ++child_pos;
    }

    auto children_type = static_cast<PageType>(index_page.points_to_leaf);
    std::streampos child_seek = index_page.children[child_pos];
    RemoveResult<FieldType> result = this->remove(child_seek, children_type, key);

    if (child_pos < index_page.len() && result.predecessor && !gt(index_page.keys[child_pos], key)) {
        index_page.keys[child_pos] = *result.predecessor;
        index_page.save(seek_page);
        result.predecessor = nullptr;
    }

    std::shared_ptr<Page<TYPES()>> child;
    if (children_type == dataPage) {
        child = std::make_shared<DataPage<TYPES()>>(this);
    } else {
        child = std::make_shared<IndexPage<TYPES()>>(this);
    }

    child->load(child_seek);
    child->balance_page_remove(seek_page, index_page, child_pos);
    return RemoveResult<FieldType> { index_page.len(), result.predecessor };
}
