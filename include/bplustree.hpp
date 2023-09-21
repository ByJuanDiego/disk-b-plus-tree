//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_HPP
#define B_PLUS_TREE_BPLUSTREE_HPP


#include <vector>
#include <functional>
#include <queue>

#include "pages.hpp"
#include "property.hpp"
#include "file_utils.hpp"


template <
    typename KeyType,
    typename RecordType,
    typename Greater = std::greater<KeyType>,
    typename Index = std::function<KeyType(RecordType&)>
> class BPlusTree {

private:

    std::fstream metadata_file;
    std::fstream b_plus_index_file;

    Json::Value metadata_json;

    Greater greater_to;
    Index get_indexed_field;

    auto create_index() -> void;

    auto load_metadata() -> void;

    auto save_metadata() -> void;

    auto locate_data_page(const KeyType& key) -> int64;

    auto insert(int64 seek_page, PageType type, RecordType& record) -> InsertStatus;

    auto balance_data_page(IndexPage<KeyType>& index_page, int32 child_pos, int64 seek_page, int64 child_seek) -> void;

    auto balance_index_page(IndexPage<KeyType>& index_page, int32 child_pos, int64 seek_page, int64 child_seek) -> void;

    auto balance_root_data_page() -> void;

    auto balance_root_index_page() -> void;

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater());

    auto insert(RecordType& record) -> void;

    auto search(const KeyType& key) -> std::vector<RecordType>;

    auto between(const KeyType& lower_bound, const KeyType& upper_bound) -> std::vector<RecordType>;

    auto remove(KeyType& key) -> void;

    auto print(std::ostream & ostream) -> void;
};


#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
