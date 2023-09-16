//
// Created by juan diego on 9/15/23.
//

#ifndef B_PLUS_TREE_DATA_PAGE_HPP
#define B_PLUS_TREE_DATA_PAGE_HPP


#include <cmath>
#include <fstream>
#include <sstream>
#include <cstring>
#include <utility>

#include "buffer_size.hpp"
#include "error_handler.hpp"
#include "types.hpp"


template <typename RecordType>
struct DataPage {
    int32 capacity;
    int32 num_records;
    int64 next_leaf;
    int64 prev_leaf;
    RecordType* records;

    auto static get_expected_capacity() -> int32;

    explicit DataPage(int32 records_capacity);

    DataPage(const DataPage& other);

    DataPage(DataPage&& other) noexcept;

    ~DataPage();

    auto size_of() -> int;

    void write(std::fstream & file);

    /**
     * @brief Read data from an input file stream and populate the object's attributes.
     *
     * This function reads data from the provided input file stream and populates the
     * object's attributes by copying the data from a binary buffer. It assumes that
     * the buffer contains serialized data in a specific format.
     *
     * @param file An input file stream from which data is read.
     */
    void read(std::fstream & file);

    void push_front(RecordType& record);

    void push_back(RecordType& record);

    auto max_record() -> RecordType;

    template <typename KeyType, typename Greater, typename Index>
    void sorted_insert(RecordType& record, Greater greater_to, Index get_indexed_field);

    auto split(int32 min_data_page_records);
};


#include "data_page.tpp"

#endif //B_PLUS_TREE_DATA_PAGE_HPP
