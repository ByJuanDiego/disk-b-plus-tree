//
// Created by juan diego on 9/15/23.
//

#include "property.hpp"


Property::Property(const std::string &directory_path, const std::string &metadata_file_name,
                   const std::string &index_file_name, int32 index_page_capacity, int32 data_page_capacity,
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
    json[SEEK_ROOT] = emptyPage;
    json[FIRST_DATA_PAGE] = emptyPage;
    json[LAST_DATA_PAGE] = emptyPage;
}

auto Property::json_value() const -> Json::Value {
    return json;
}
