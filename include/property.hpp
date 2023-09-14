//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_PROPERTY_HPP
#define B_PLUS_TREE_PROPERTY_HPP

#include <json/json.h>


const std::string DIRECTORY_PATH = "directory_path";
const std::string INDEX_PAGE_CAPACITY = "maximum_index_page_keys";
const std::string MINIMUM_INDEX_PAGE_KEYS = "minimum_index_page_keys";
const std::string DATA_PAGE_CAPACITY = "maximum_data_page_records";
const std::string MINIMUM_DATA_PAGE_RECORDS = "minimum_data_page_records";
const std::string INDEX_FULL_PATH = "index_full_path";
const std::string METADATA_FULL_PATH = "metadata_full_path";
const std::string UNIQUE_KEY = "unique_key";
const std::string ROOT_STATUS = "root_status";
const std::string SEEK_ROOT = "seek_root";
const std::string FIRST_DATA_PAGE = "seek_first_data_page";
const std::string LAST_DATA_PAGE = "seek_last_data_page";


using int32 = int32_t;
using int64 = int64_t;

enum DataPageType {
    emptyPage = NULL_PAGE,  // Empty Tree
    indexPage = 0,          // IndexPage
    dataPage  = 1           // DataPage
};

struct Property {
private:
    Json::Value json;

public:
    explicit Property(const std::string& directory_path,
                      const std::string& metadata_file_name,
                      const std::string& index_file_name,
                      int32 index_page_capacity,
                      int32 data_page_capacity,
                      bool unique_key) {
        // Define constants for maximum number of keys in an index page and maximum number of records in a data page.
        int M = index_page_capacity;
        int N = data_page_capacity;

        // Calculate the minimum number of keys in an index page (half of M rounded up) and the minimum number
        // of records in a data page (half of N rounded up).
        int m = static_cast<int>(std::ceil(M / 2.0)) - 1;
        int n = static_cast<int>(std::ceil(N / 2.0)) - 1;

        // paths info.
        json[DIRECTORY_PATH] = directory_path;
        json[INDEX_FULL_PATH] = directory_path + index_file_name;
        json[METADATA_FULL_PATH] = directory_path + metadata_file_name;

        // pages size info.
        json[INDEX_PAGE_CAPACITY] = M;
        json[DATA_PAGE_CAPACITY] = N;
        json[MINIMUM_INDEX_PAGE_KEYS] = m;
        json[MINIMUM_DATA_PAGE_RECORDS] = n;

        // B+ tree info.
        json[UNIQUE_KEY] = unique_key;
        json[ROOT_STATUS] = emptyPage;
        json[SEEK_ROOT] = NULL_PAGE;
        json[FIRST_DATA_PAGE] = NULL_PAGE;
        json[LAST_DATA_PAGE] = NULL_PAGE;
    }

    [[nodiscard]] auto json_value() const -> Json::Value {
        return json;
    }
};

#endif //B_PLUS_TREE_PROPERTY_HPP
