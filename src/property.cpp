//
// Created by juan diego on 9/15/23.
//

#include "property.hpp"

Property::Property(std::string directory_path,
                   const std::string &metadata_file_name,
                   const std::string &index_file_name,
                   int32_t index_page_capacity,
                   int32_t data_page_capacity,
                   bool unique)
        : DIRECTORY_PATH(std::move(directory_path)),
          INDEX_FILE_NAME(index_file_name + ".tree"),
          METADATA_FILE_NAME(metadata_file_name + ".meta"),
          SEEK_ROOT(emptyPage),
          MAX_INDEX_PAGE_CAPACITY(index_page_capacity),
          MAX_DATA_PAGE_CAPACITY(data_page_capacity),
          ROOT_STATUS(emptyPage),
          UNIQUE(unique) {
    INDEX_FULL_PATH = DIRECTORY_PATH + INDEX_FILE_NAME;
    METADATA_FULL_PATH = DIRECTORY_PATH + METADATA_FILE_NAME;
    MIN_INDEX_PAGE_CAPACITY = static_cast<std::int32_t>(std::ceil(MAX_INDEX_PAGE_CAPACITY / 2.0)) - 1;
    MIN_DATA_PAGE_CAPACITY = static_cast<std::int32_t>(std::ceil(MAX_DATA_PAGE_CAPACITY / 2.0)) - 1;
    SPLIT_POS_INDEX_PAGE = static_cast<std::int32_t>(std::floor(MAX_INDEX_PAGE_CAPACITY / 2.0));
    SPLIT_POS_DATA_PAGE = static_cast<std::int32_t>(std::floor(MAX_DATA_PAGE_CAPACITY / 2.0));
}


void Property::load(std::fstream &file) {
    file >> DIRECTORY_PATH >> INDEX_FILE_NAME >> METADATA_FILE_NAME >> SEEK_ROOT
         >> MAX_INDEX_PAGE_CAPACITY >> MAX_DATA_PAGE_CAPACITY >> ROOT_STATUS >> UNIQUE;

    INDEX_FULL_PATH = DIRECTORY_PATH + INDEX_FILE_NAME;
    METADATA_FULL_PATH = DIRECTORY_PATH + METADATA_FILE_NAME;
    MIN_INDEX_PAGE_CAPACITY = static_cast<std::int32_t>(std::ceil(MAX_INDEX_PAGE_CAPACITY / 2.0)) - 1;
    MIN_DATA_PAGE_CAPACITY = static_cast<std::int32_t>(std::ceil(MAX_DATA_PAGE_CAPACITY / 2.0)) - 1;
    SPLIT_POS_INDEX_PAGE = static_cast<std::int32_t>(std::floor(MAX_INDEX_PAGE_CAPACITY / 2.0));
    SPLIT_POS_DATA_PAGE = static_cast<std::int32_t>(std::floor(MAX_DATA_PAGE_CAPACITY / 2.0));
}


void Property::save(std::fstream &file) const {
    file << DIRECTORY_PATH << "\n" << INDEX_FILE_NAME << "\n" << METADATA_FILE_NAME << "\n" << SEEK_ROOT << "\n"
         << MAX_INDEX_PAGE_CAPACITY << "\n" << MAX_DATA_PAGE_CAPACITY << "\n" << ROOT_STATUS << "\n" << UNIQUE;
}
