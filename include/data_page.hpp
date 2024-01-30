//
// Created by juan diego on 9/15/23.
//

#ifndef B_PLUS_TREE_DATA_PAGE_HPP
#define B_PLUS_TREE_DATA_PAGE_HPP


#include <cmath>
#include <vector>
#include <fstream>
#include <sstream>
#include <cstring>
#include <utility>

#include "page.hpp"
#include "buffer_size.hpp"
#include "error_handler.hpp"


template <typename KeyType, typename RecordType, typename Index>
struct DataPage: public Page<KeyType> {

    std::int32_t num_records;
    std::int64_t next_leaf;
    std::int64_t prev_leaf;
    std::vector<RecordType> records;
    Index get_indexed_field;

    explicit DataPage(std::int32_t capacity, const Index& index);

    DataPage(const DataPage& other);

    DataPage(DataPage&& other) noexcept;

    ~DataPage() override;

    auto write(std::fstream & file)                            -> void override;

    auto read(std::fstream & file)                             -> void override;

    auto size_of()                                             -> std::int32_t override;

    auto split(std::int32_t split_pos)                         -> SplitResult<KeyType> override;

    auto push_front(RecordType& record)                        -> void;

    auto push_back(RecordType& record)                         -> void;

    auto max_record()                                          -> RecordType;

    template <typename Greater>
    auto sorted_insert(RecordType& record, Greater gt)         -> void;

    template <typename Greater>
    auto remove(KeyType key, Greater gt)                       -> std::shared_ptr<KeyType>;
};



template <typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t;


#include "data_page.tpp"

#endif //B_PLUS_TREE_DATA_PAGE_HPP
