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


template <DEFINE_INDEX_TYPE>
struct IndexPage: public Page<INDEX_TYPE> {

    std::int32_t num_keys;
    std::vector<KeyType> keys;
    std::vector<std::int64_t> children;
    bool points_to_leaf;

    explicit IndexPage(BPlusTree<INDEX_TYPE>* b_plus, bool points_to_leaf = true);

    ~IndexPage();

    auto write(std::fstream &file)                                           -> void override;

    auto read(std::fstream &file)                                            -> void override;

    auto size_of()                                                           -> std::int32_t override;

    auto len()                                                               -> std::size_t override;

    auto split(std::int32_t split_position)                                  -> SplitResult<INDEX_TYPE> override;

    auto balance(std::streampos seek_parent,
                 IndexPage<INDEX_TYPE>& parent, std::int32_t child_pos)      -> void override;

    auto deallocate_root()                                                   -> void override;

    auto push_front(KeyType& key, std::int64_t child)                        -> void;

    auto push_back(KeyType& key, std::int64_t child)                         -> void;

    auto reallocate_references_after_split(std::int32_t child_pos,
                                            KeyType& new_key,
                                            std::int64_t new_page_seek)      -> void;

    auto reallocate_references_after_merge(std::int32_t merged_child_pos)    -> void;
};


template <typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t;


#include "index_page.tpp"

#endif //B_PLUS_TREE_INDEX_PAGE_HPP
