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


template<TYPES(typename)>
struct DataPage : public Page<TYPES()> {

    std::int32_t num_records;
    std::int64_t next_leaf;
    std::int64_t prev_leaf;
    std::vector<RecordType> records;

    explicit DataPage(BPlusTree<TYPES()> *tree);

    ~DataPage() override;

    auto write() -> void override;

    auto read() -> void override;

    auto bytes_len() -> std::int32_t override;

    auto len() -> std::size_t override;

    auto max_capacity() -> std::size_t override;

    auto split(std::int32_t split_pos) -> SplitResult<TYPES()> override;

    auto balance_page_insert(
            std::streampos seek_parent,
            IndexPage<TYPES()> &parent,
            std::int32_t child_pos) -> void override;

    auto balance_page_remove(
            std::streampos seek_parent,
            IndexPage<TYPES()> &parent,
            std::int32_t child_pos) -> void override;

    auto balance_root_insert(std::streampos old_root_seek) -> void override;

    auto balance_root_remove() -> void override;

    auto push_front(RecordType &record) -> void;

    auto push_back(RecordType &record) -> void;

    auto pop_front() -> RecordType;

    auto pop_back() -> RecordType;

    auto max_record() -> RecordType;

    auto sorted_insert(RecordType &record) -> void;

    auto remove(FieldType key) -> std::shared_ptr<FieldType>;

    auto merge(DataPage<TYPES()> &right_sibling) -> void;
};


template<typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t;


#include "data_page.tpp"

#endif //B_PLUS_TREE_DATA_PAGE_HPP
