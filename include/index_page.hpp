//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_INDEX_PAGE_HPP
#define B_PLUS_TREE_INDEX_PAGE_HPP


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
struct IndexPage : public Page<TYPES()> {

    std::int32_t num_keys;
    std::vector<FieldType> keys;
    std::vector<std::int64_t> children;
    bool points_to_leaf;

    explicit IndexPage(BPlusTree<TYPES()> *tree, bool points_to_leaf = true);

    ~IndexPage();

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

    auto push_front(FieldType &key, std::streampos child) -> void;

    auto push_back(FieldType &key, std::streampos child) -> void;

    auto pop_front() -> std::pair<FieldType, std::streampos>;

    auto pop_back() -> std::pair<FieldType, std::streampos>;

    auto reallocate_references_after_split(std::int32_t child_pos,
                                           FieldType &new_key,
                                           std::streampos new_page_seek) -> void;

    auto reallocate_references_after_merge(std::int32_t merged_child_pos) -> void;

    auto merge(IndexPage<TYPES()> &right_sibling, FieldType &new_key) -> void;
};


template<typename FieldType>
auto get_expected_index_page_capacity() -> std::int32_t;


#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
