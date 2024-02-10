//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_PROPERTY_HPP
#define B_PLUS_TREE_PROPERTY_HPP


#include <string>
#include <cmath>
#include <fstream>
#include <cstdint>


// Identifiers for pages type
enum PageType {
    emptyPage = -1,  // Empty Tree
    indexPage = 0,   // Index Page
    dataPage  = 1    // Data Page
};


struct Property {

    std::string DIRECTORY_PATH;
    std::string INDEX_FILE_NAME;
    std::string METADATA_FILE_NAME;

    std::int64_t SEEK_ROOT;

    std::int32_t MAX_INDEX_PAGE_CAPACITY;
    std::int32_t MIN_INDEX_PAGE_CAPACITY;
    std::int32_t MAX_DATA_PAGE_CAPACITY;
    std::int32_t MIN_DATA_PAGE_CAPACITY;
    std::int32_t SPLIT_POS_INDEX_PAGE;
    std::int32_t SPLIT_POS_DATA_PAGE;
    std::int32_t ROOT_STATUS;

    bool UNIQUE_KEY;

    std::string INDEX_FULL_PATH;
    std::string METADATA_FULL_PATH;

    explicit Property(std::string directory_path,
                      const std::string& metadata_file_name,
                      const std::string& index_file_name,
                      int32_t index_page_capacity,
                      int32_t data_page_capacity,
                      bool unique_key);

    void load(std::fstream &file);

    void save(std::fstream &file) const;
};

#endif //B_PLUS_TREE_PROPERTY_HPP
