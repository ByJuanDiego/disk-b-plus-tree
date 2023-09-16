//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_INDEX_PAGE_HPP
#define B_PLUS_TREE_INDEX_PAGE_HPP


#include <cmath>
#include <fstream>
#include <sstream>
#include <cstring>
#include <utility>

#include "buffer_size.hpp"
#include "error_handler.hpp"
#include "types.hpp"


template <typename KeyType>
struct IndexPage {
    int32 capacity;           // The maximum capacity of keys and children arrays.
    int32 num_keys;           // The current number of keys stored in the page.
    KeyType* keys;            // An array of keys stored in the page.
    int64* children;          // An array of child pointers corresponding to the keys.
    bool points_to_leaf;      // Indicates whether this index page points to a leaf node.

    auto static get_expected_capacity() -> int32;

    explicit IndexPage(int32 children_capacity);

    ~IndexPage();

    auto size_of() -> int;

    auto write(std::fstream &file) -> void;

    auto read(std::fstream &file) -> void;

    auto push_front(KeyType& key, int64 child) -> void;

    auto push_back(KeyType& key, int64 child) -> void;

    auto reallocate_references_to_data_pages(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void;

    auto reallocate_references_to_index_pages(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void;

    template <typename Greater>
    auto sorted_insert(KeyType& key, int64 children_seek, Greater greater_to) -> void;

    auto split(int32 min_index_page_keys, KeyType& new_index_page_key) -> IndexPage<KeyType>;
};


#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
