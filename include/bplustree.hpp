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
    DataPageType type;
    int64 seek_previous_page;
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

    int64 locate_data_page(KeyType& key) {
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
                IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY]);
                do {
                    b_plus_index_file.seekg(seek_page);
                    index_page.read(b_plus_index_file);
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
        open(metadata_file, metadata_json[METADATA_FULL_PATH].asString(), std::ios::out);
        if (!metadata_file.is_open()) {
            throw CreateFileError();
        }
        save_metadata();

        // finally, creates an empty file for the B+
        open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::out);
        if (!b_plus_index_file.is_open()) {
            throw CreateFileError();
        }

        close(b_plus_index_file);
    }

    InsertStatus insert(int64 seek_page, DataPageType type, RecordType& record) {
        if (type == dataPage) {
            DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
            seek_all(b_plus_index_file, seek_page);
            data_page.read(b_plus_index_file);
            data_page.template sorted_insert<KeyType, Greater>(record, get_indexed_field(record), greater_to);
            return { dataPage, seek_page, data_page.num_records };
        }

        IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY].asInt());
        seek_all(b_plus_index_file, seek_page);
        index_page.read(b_plus_index_file);

        int i;
        for (i = 0; i < index_page.num_keys && greater_to(get_indexed_field(record), index_page.keys[i]); ++i);
        InsertStatus prev_page_status = insert(index_page.children[i], index_page.points_to_leaf? dataPage: indexPage, record);

        if (prev_page_status.type == dataPage && prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt()) {
            // do some stuff
            // TODO
        }
        else if (prev_page_status.type == indexPage && prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt()) {
            // do some stuff
            // TODO
        }

        return { indexPage, seek_page, index_page.num_keys };
    }

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater())
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

    void insert(RecordType& record) {
        open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);

        auto data_page_type = (DataPageType) metadata_json[ROOT_STATUS].asInt();

        if (data_page_type == emptyPage) {
            DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
            data_page.push_back(record);
            metadata_json[SEEK_ROOT] = INITIAL_PAGE;
            b_plus_index_file.seekp(INITIAL_PAGE);
            data_page.write(b_plus_index_file);
        } else {
            InsertStatus status = this->insert(metadata_json[SEEK_ROOT].asLargestInt(), data_page_type, record);
            // do some stuff...
            // TODO
        }

        close(b_plus_index_file);
    }

    std::vector<RecordType> search(KeyType& key) {
        open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in);
        int64 seek_page = locate_data_page(key);

        std::vector<RecordType> located_records;
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY]);
        do {
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
        } while (seek_page != NULL_PAGE);

        close(b_plus_index_file);
        return located_records;
    }

    std::vector<RecordType> between(KeyType& lower_bound, KeyType& upper_bound) {
        open(b_plus_index_file, metadata_json[INDEX_FULL_PATH], std::ios::in);
        int64 seek_page = locate_data_page(lower_bound);

        std::vector<RecordType> located_records;
        DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY]);
        do {
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
        } while (seek_page != NULL_PAGE);

        close(b_plus_index_file);
        return located_records;
    }

    void remove(KeyType& key) {
        // TODO
    }
};

#endif //B_PLUS_TREE_BPLUSTREE_HPP
