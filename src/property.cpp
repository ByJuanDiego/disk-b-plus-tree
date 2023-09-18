//
// Created by juan diego on 9/15/23.
//

#include "property.hpp"


Property::Property(const std::string &directory_path, const std::string &metadata_file_name,
                   const std::string &index_file_name, int32 index_page_capacity, int32 data_page_capacity,
                   bool unique_key) {
    // paths info.
    json[DIRECTORY_PATH] = directory_path;
    json[INDEX_FULL_PATH] = directory_path + index_file_name;
    json[METADATA_FULL_PATH] = directory_path + metadata_file_name;

    // pages size info.
    json[INDEX_PAGE_CAPACITY] = index_page_capacity;
    json[MINIMUM_INDEX_PAGE_KEYS] = static_cast<int32>(std::ceil(index_page_capacity / 2.0)) - 1;
    json[NEW_INDEX_PAGE_KEY_POS] = static_cast<int32>(std::floor(index_page_capacity / 2.0));

    json[DATA_PAGE_CAPACITY] = data_page_capacity;
    json[MINIMUM_DATA_PAGE_RECORDS] = static_cast<int32>(std::ceil(data_page_capacity / 2.0)) - 1;
    json[NEW_DATA_PAGE_NUM_RECORDS] = static_cast<int32>(std::floor(data_page_capacity / 2.0));

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
