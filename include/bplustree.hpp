//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_HPP
#define B_PLUS_TREE_BPLUSTREE_HPP

#include <functional>
#include <vector>

#include "pages.hpp"
#include "property.hpp"
#include "file_utils.hpp"

struct InsertStatus {
    int32 size;
};

template <
    typename KeyType,
    typename RecordType,
    typename Greater = std::greater<KeyType>,
    typename Index = std::function<KeyType(RecordType&)>
> class BPlusTree {

private:

    std::fstream metadata_file;
    std::fstream b_plus_index_file;

    Json::Value metadata_json;

    Greater greater_to;
    Index get_indexed_field;

    void load_metadata() {
        metadata_file >> metadata_json;
    }

    void save_metadata() {
        metadata_file << metadata_json;
    }

    void open(std::fstream & file, const std::string& file_name, std::ios::openmode mode_flags) {
        file.open(file_name, mode_flags);
    }

    void close(std::fstream& file) {
        file.close();
    }

    void seek_all(std::fstream& file, int64 pos, std::ios::seekdir offset = std::ios::beg) {
        file.seekg(pos, offset);
        file.seekp(pos, offset);
    }

    auto locate_data_page(KeyType& key) -> int64 {
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
                    b_plus_index_file.seekg(seek_page);
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

    void create_index() {
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

    auto split_data_page(DataPage<RecordType>& full_data_page) -> DataPage<RecordType> {
        DataPage<RecordType> new_data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
        int32 min_data_page_records = metadata_json[MINIMUM_DATA_PAGE_RECORDS].asInt();
        new_data_page.num_records = std::floor(metadata_json[DATA_PAGE_CAPACITY].asInt() / 2.0);

        for (int i = 0; i < new_data_page.num_records; ++i) {
            new_data_page.push_back(full_data_page.records[i + min_data_page_records + 1]);
        }

        full_data_page.num_records = min_data_page_records + 1;
        return new_data_page;
    }

    auto split_index_page(IndexPage<KeyType>& full_index_page, KeyType& new_index_page_key) -> IndexPage<KeyType> {
        IndexPage<KeyType> new_index_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
        int32 min_index_page_keys = metadata_json[MINIMUM_INDEX_PAGE_KEYS].asInt();
        new_index_page.num_keys = min_index_page_keys;

        for (int i = 0; i < new_index_page.num_keys; ++i) {
            int const index = min_index_page_keys + i + 1;
            new_index_page.push_back(
                    full_index_page.keys[index],
                    full_index_page.children[index]
                    );
        }

        int32 new_key_pos = std::floor(metadata_json[INDEX_PAGE_CAPACITY].asInt() / 2);
        new_index_page_key = full_index_page.keys[new_key_pos];
        new_index_page.children[new_index_page.num_keys] = full_index_page.children[full_index_page.num_keys];
        full_index_page.num_keys = new_key_pos;
        return new_index_page;
    }

    auto insert(int64 seek_page, DataPageType type, RecordType& record) -> InsertStatus;

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater());

    auto insert(RecordType& record) -> void;

    auto search(KeyType& key) -> std::vector<RecordType>;

    auto between(KeyType& lower_bound, KeyType& upper_bound) -> std::vector<RecordType>;

    auto remove(KeyType& key) -> void;
};

#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
