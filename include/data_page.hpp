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


template <DEFINE_INDEX_TYPE>
struct DataPage: public Page<INDEX_TYPE> {

    std::int32_t num_records;
    std::int64_t next_leaf;
    std::int64_t prev_leaf;
    std::vector<RecordType> records;

    explicit DataPage(std::int32_t capacity, BPlusTree<INDEX_TYPE>* b_plus);

    ~DataPage() override;

    auto write(std::fstream & file)                            -> void override;

    auto read(std::fstream & file)                             -> void override;

    auto size_of()                                             -> std::int32_t override;

    auto len()                                                 -> std::size_t override;

    auto split(std::int32_t split_pos)                         -> SplitResult<INDEX_TYPE> override;

    auto push_front(RecordType& record)                        -> void;

    auto push_back(RecordType& record)                         -> void;

    auto pop_front()                                           -> RecordType;

    auto pop_back()                                            -> RecordType;

    auto max_record()                                          -> RecordType;

    auto min_record()                                          -> RecordType;

    auto sorted_insert(RecordType& record)                     -> void;

    auto remove(KeyType key)                                   -> std::shared_ptr<KeyType>;

    auto balance(std::streampos seek_parent,
                 IndexPage<INDEX_TYPE>& parent,
                 std::int32_t child_pos)                       -> void override;

    auto merge(DataPage<INDEX_TYPE>& right_sibling)            -> std::shared_ptr<DataPage<INDEX_TYPE>>;
};


template <typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t;


#include "data_page.tpp"

#endif //B_PLUS_TREE_DATA_PAGE_HPP
