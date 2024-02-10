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


template<DEFINE_INDEX_TYPE>
struct IndexPage : public Page<INDEX_TYPE> {

    std::int32_t num_keys;
    std::vector<KeyType> keys;
    std::vector<std::int64_t> children;
    bool points_to_leaf;

    explicit IndexPage(BPlusTree<INDEX_TYPE> *tree, bool points_to_leaf = true);

    ~IndexPage();

    auto write() -> void override;

    auto read() -> void override;

    auto bytes_len() -> std::int32_t override;

    auto len() -> std::size_t override;

    auto max_capacity() -> std::size_t override;

    auto split(std::int32_t split_pos) -> SplitResult<INDEX_TYPE> override;

    auto balance_page_insert(
            std::streampos seek_parent,
            IndexPage<INDEX_TYPE> &parent,
            std::int32_t child_pos) -> void override;

    auto balance_page_remove(
            std::streampos seek_parent,
            IndexPage<INDEX_TYPE> &parent,
            std::int32_t child_pos) -> void override;

    auto balance_root_insert(std::streampos old_root_seek) -> void override;

    auto balance_root_remove() -> void override;

    auto push_front(KeyType &key, std::streampos child) -> void;

    auto push_back(KeyType &key, std::streampos child) -> void;

    auto pop_front() -> std::pair<KeyType, std::streampos>;

    auto pop_back() -> std::pair<KeyType, std::streampos>;

    auto reallocate_references_after_split(std::int32_t child_pos,
                                           KeyType &new_key,
                                           std::streampos new_page_seek) -> void;

    auto reallocate_references_after_merge(std::int32_t merged_child_pos) -> void;

    auto merge(IndexPage<INDEX_TYPE> &right_sibling, KeyType &new_key) -> void;
};


template<typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t;


#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
