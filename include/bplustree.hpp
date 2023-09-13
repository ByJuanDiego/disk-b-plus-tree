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
                IndexPage<KeyType> index_page(metadata_json[INDEX_PAGE_CAPACITY]);

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

    auto split(DataPage<RecordType>& full_page) -> DataPage<RecordType> {
        DataPage<RecordType> new_data_page(metadata_json[DATA_PAGE_CAPACITY]);
        int32 min_data_page_records = metadata_json[MINIMUM_DATA_PAGE_RECORDS].asInt();
        new_data_page.num_records = min_data_page_records;

        for (int i = 0; i < new_data_page.num_records; ++i) {
            new_data_page.push_back(full_page.records[i + min_data_page_records + 1]);
        }

        full_page.num_records = min_data_page_records + 1;
    }

    auto insert(int64 seek_page, DataPageType type, RecordType& record) -> InsertStatus {
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

        if (children_type == dataPage && (prev_page_status.size == metadata_json[DATA_PAGE_CAPACITY].asInt())) {
            DataPage<RecordType> full_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
            seek_all(b_plus_index_file, child_seek);
            full_page.read(b_plus_index_file);

            DataPage<RecordType> new_page = split(full_page);
            seek_all(b_plus_index_file, 0, std::ios::end);
            int64 new_page_seek = b_plus_index_file.tellp();
            new_page.prev_leaf = child_seek;
            new_page.write(b_plus_index_file);

            full_page.next_leaf = new_page_seek;
            seek_all(b_plus_index_file, child_seek);
            full_page.write(b_plus_index_file);

            for (int i = index_page.num_keys; i > child_pos; --i) {
                index_page.keys[i] = index_page.keys[i - 1];
            }

            for (int i = index_page.num_keys; i > child_pos; --i) {
                index_page.children[i + 1] = index_page.children[i];
            }

            index_page.children[child_pos + 1] = new_page_seek;
            index_page.keys[child_pos] = get_indexed_field(full_page.max_record());
            ++index_page.num_keys;

            seek_all(b_plus_index_file, seek_page);
            index_page.write(b_plus_index_file);
        }
        else if (children_type == indexPage && (prev_page_status.size == metadata_json[INDEX_PAGE_CAPACITY].asInt())) {
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

        auto root_page_type = static_cast<DataPageType>(metadata_json[ROOT_STATUS].asInt());

        if (root_page_type == emptyPage) {
            DataPage<RecordType> data_page(metadata_json[DATA_PAGE_CAPACITY].asInt());
            data_page.push_back(record);
            metadata_json[SEEK_ROOT] = INITIAL_PAGE;
            b_plus_index_file.seekp(INITIAL_PAGE);
            data_page.write(b_plus_index_file);
        } else {
            InsertStatus status = this->insert(metadata_json[SEEK_ROOT].asLargestInt(), root_page_type, record);
            // do some stuff...
            // TODO
        }

        close(b_plus_index_file);
    }

    auto search(KeyType& key) -> std::vector<RecordType> {
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

    auto between(KeyType& lower_bound, KeyType& upper_bound) -> std::vector<RecordType> {
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

    void remove(KeyType& key) {
        // TODO
    }
};

#endif //B_PLUS_TREE_BPLUSTREE_HPP
