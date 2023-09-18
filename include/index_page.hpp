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
#include <vector>

#include "buffer_size.hpp"
#include "error_handler.hpp"
#include "types.hpp"


template <typename KeyType>
struct IndexPage {
    int32 capacity;           // The maximum capacity of keys and children arrays.
    int32 num_keys;           // The current number of keys stored in the page.
    std::vector<KeyType> keys;
    std::vector<int64> children;
    bool points_to_leaf;      // Indicates whether this index page points to a leaf node.

    auto static get_expected_capacity() -> int32;

    explicit IndexPage(int32 children_capacity, bool points_to_leaf = true);

    IndexPage(const IndexPage<KeyType>& other);

    ~IndexPage();

    auto size_of() -> int;

    auto write(std::fstream &file) -> void;

    auto read(std::fstream &file) -> void;

    auto push_front(KeyType& key, int64 child) -> void;

    auto push_back(KeyType& key, int64 child) -> void;

    auto reallocate_references(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void;

    auto split(int32 new_key_pos, KeyType& new_index_page_key) -> IndexPage<KeyType>;
};



#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
