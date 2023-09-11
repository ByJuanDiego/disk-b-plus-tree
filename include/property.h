//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_PROPERTY_H
#define B_PLUS_TREE_PROPERTY_H

#include <json/json.h>


const std::string DIRECTORY_PATH = "directory_path";
const std::string INDEX_PAGE_CAPACITY = "maximum_index_page_keys";
const std::string MINIMUM_INDEX_PAGE_KEYS = "minimum_index_page_keys";
const std::string DATA_PAGE_CAPACITY = "maximum_data_page_records";
const std::string MINIMUM_DATA_PAGE_RECORDS = "minimum_data_page_records";
const std::string INDEX_FULL_PATH = "index_full_path";
const std::string METADATA_FULL_PATH = "metadata_full_path";
const std::string PRIMARY_KEY = "primary_key";
const std::string ROOT_STATUS = "root_status";
const std::string SEEK_ROOT = "seek_root";
const std::string FIRST_DATA_PAGE = "seek_first_data_page";
const std::string LAST_DATA_PAGE = "seek_last_data_page";


using int32 = int32_t;
using int64 = int64_t;

enum Root {
    empty = -1,      // Empty Tree
    points_to_index, // IndexPage
    points_to_data   // DataPage
};

struct Property {
    std::string directory_path;
    std::string index_file_name;
    std::string metadata_file_name;
    int32 index_page_capacity;
    int32 data_page_capacity;
    bool primary_key;
    Root root;
    int64 seek_root;
    int64 seek_first_data_page;
    int64 seek_last_data_page;

    explicit Property(std::string directory_path,
                      std::string metadata_file_name,
                      std::string index_file_name,
                      int32 index_page_capacity,
                      int32 data_page_capacity,
                      bool primary_key)
            :   directory_path(std::move(directory_path)),
                metadata_file_name(std::move(metadata_file_name)),
                index_file_name(std::move(index_file_name)),
                index_page_capacity(index_page_capacity),
                data_page_capacity(data_page_capacity),
                primary_key(primary_key),
                root(Root::empty),
                seek_root(NULL_PAGE),
                seek_first_data_page(NULL_PAGE),
                seek_last_data_page(NULL_PAGE)
                {
    }

    [[nodiscard]] Json::Value json_value() const {
        Json::Value json_format;

        int M = index_page_capacity;
        int N = data_page_capacity;

        int m = static_cast<int>(std::ceil(M / 2.0)) - 1;
        int n =  static_cast<int>(std::ceil(N / 2.0)) - 1;

        json_format[DIRECTORY_PATH] = directory_path;
        json_format[INDEX_FULL_PATH] = directory_path  + index_file_name;
        json_format[METADATA_FULL_PATH] = directory_path + metadata_file_name;
        json_format[INDEX_PAGE_CAPACITY] = M;
        json_format[MINIMUM_INDEX_PAGE_KEYS] = m;
        json_format[DATA_PAGE_CAPACITY] = N;
        json_format[MINIMUM_DATA_PAGE_RECORDS] = n;
        json_format[PRIMARY_KEY] = primary_key;
        json_format[ROOT_STATUS] = static_cast<int>(root);
        json_format[SEEK_ROOT] = seek_root;
        json_format[FIRST_DATA_PAGE] = seek_first_data_page;
        json_format[LAST_DATA_PAGE] = seek_last_data_page;
        
        return json_format;
    }
};

#endif //B_PLUS_TREE_PROPERTY_H
