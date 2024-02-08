//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_PROPERTY_HPP
#define B_PLUS_TREE_PROPERTY_HPP

#include <json/json.h>
#include <cmath>


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

struct Property {
private:
    Json::Value json;

public:
    explicit Property(const std::string& directory_path,
                      const std::string& metadata_file_name,
                      const std::string& index_file_name,
                      std::int32_t index_page_capacity,
                      std::int32_t data_page_capacity,
                      bool unique_key);

    [[nodiscard]] auto json_value() const -> Json::Value;
};

#endif //B_PLUS_TREE_PROPERTY_HPP
