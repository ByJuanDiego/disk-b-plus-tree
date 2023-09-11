//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_H
#define B_PLUS_TREE_BPLUSTREE_H

#include <functional>
#include <vector>

#include "pages.h"
#include "property.h"
#include "file_utils.h"


template <
    typename KeyType,
    typename RecordType,
    typename Greater = std::greater<KeyType>,
    typename Index = std::function<KeyType(RecordType&)>
> class BPlusTree {

private:

    std::fstream metadata_file;
    std::fstream b_plus_file;

    Json::Value metadata_json;

    Greater greater_to;
    Index get_indexed_field;

    void load_metadata() {
        metadata_file >> metadata_json;
    }

    void save_metadata() {
        metadata_file << metadata_json;
    }

    void open_metadata(std::ios::openmode mode) {
        metadata_file.open(metadata_json[METADATA_FULL_PATH].asString(), mode);
    }

    void open_index(std::ios::openmode mode) {
        b_plus_file.open(metadata_json[INDEX_FULL_PATH].asString(), mode);
    }

    bool is_empty() {
        return metadata_json[ROOT_STATUS].asInt() == Root::empty;
    }

    bool points_to_data_page() {
        return metadata_json[ROOT_STATUS].asInt() == Root::points_to_data;
    }

    bool points_to_index_page() {
        return metadata_json[ROOT_STATUS].asInt() == Root::points_to_index;
    }

    int64 locate_data_page(KeyType& key) {
        switch (metadata_json[ROOT_STATUS].asInt()) {
            case Root::empty: {
                throw KeyNotFound();
            }
            case Root::points_to_data: {
                return metadata_json[SEEK_ROOT].asInt();
            }
            case Root::points_to_index: {
                // iterates through the index pages and descends the B+ in order to locate the first data page
                // that may contain the key to search.
                int64 seek_page = metadata_json[SEEK_ROOT].asInt();
                IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY]);
                do {
                    b_plus_file.seekg(seek_page);
                    index_page.read(b_plus_file);
                    int i;
                    for (i = 0; (i < index_page.num_keys) && greater_to(key, index_page.keys[i]); ++i);
                    seek_page = index_page.children[i];
                } while (!index_page.points_to_leaf);

                return seek_page;
            }
        }
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
        open_metadata(std::ios::out);
        if (!metadata_file.is_open()) {
            throw CreateFileError();
        }
        save_metadata();

        // finally, creates an empty file for the B+
        open_index(std::ios::out);
        if (!b_plus_file.is_open()) {
            throw CreateFileError();
        }
        b_plus_file.close();
    }

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater())
            : greater_to(greater), get_indexed_field(index) {
        metadata_json = property.json_value();
        open_metadata(std::ios::in);

        // if the metadata file cannot be opened, creates the index
        if (!metadata_file.good()) {
            b_plus_file.close();
            create_index();
            return;
        }
        // otherwise, just loads the metadata in RAM
        load_metadata();
        b_plus_file.close();
    }

    void insert(RecordType& record) {
        // TODO
    }

    std::vector<RecordType> search(KeyType& key) {
        open_index(std::ios::in);
        int64 seek_page = locate_data_page(key);

        std::vector<RecordType> located_records;
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY]);
        do {
            b_plus_file.seekg(seek_page);
            data_page.read(b_plus_file);

            for (int i = 0; i < data_page.num_records; ++i) {
                if (greater_to(key, get_indexed_field(data_page.records[i]))) {
                    continue;
                }
                if (greater_to(get_indexed_field(data_page.records[i])), key) {
                    if (located_records.empty()) {
                        throw KeyNotFound();
                    }
                    b_plus_file.close();
                    return located_records;
                }
                located_records.push_back(data_page.records[i]);
            }
            seek_page = data_page.next_leaf;
        } while (seek_page != NULL_PAGE);

        b_plus_file.close();
        return located_records;
    }

    std::vector<RecordType> between(KeyType& lower_bound, KeyType& upper_bound) {
        open_index(std::ios::in);
        int64 seek_page = locate_data_page(lower_bound);

        std::vector<RecordType> located_records;
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY]);
        do {
            b_plus_file.seekg(seek_page);
            data_page.read(b_plus_file);

            for (int i = 0; i < data_page.num_records; ++i) {
                if (greater_to(lower_bound, get_indexed_field(data_page.records[i]))) {
                    continue;
                }
                if (greater_to(get_indexed_field(data_page.records[i])), upper_bound) {
                    b_plus_file.close();
                    return located_records;
                }
                located_records.push_back(data_page.records[i]);
            }
            seek_page = data_page.next_leaf;
        } while (seek_page != NULL_PAGE);

        b_plus_file.close();
        return located_records;
    }

    void remove(KeyType& key) {
        // TODO
    }
};

#endif //B_PLUS_TREE_BPLUSTREE_H
