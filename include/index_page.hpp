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


template <typename KeyType>
struct IndexPage: public Page<KeyType> {

    std::int32_t num_keys;
    std::vector<KeyType> keys;
    std::vector<std::int64_t> children;
    bool points_to_leaf;

    explicit IndexPage(std::int32_t capacity, bool points_to_leaf = true);

    IndexPage(const IndexPage<KeyType>& other);

    IndexPage(IndexPage&& other) noexcept;

    ~IndexPage();

    auto write(std::fstream &file)                                           -> void override;

    auto read(std::fstream &file)                                            -> void override;

    auto size_of()                                                           -> std::int32_t override;

    auto split(std::int32_t split_position)                                  -> SplitResult<KeyType> override;

    auto push_front(KeyType& key, std::int64_t child)                        -> void;

    auto push_back(KeyType& key, std::int64_t child)                         -> void;

    auto reallocate_references(std::int32_t child_pos,
                               KeyType& new_key, std::int64_t new_page_seek) -> void;
};


template <typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t;


#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
