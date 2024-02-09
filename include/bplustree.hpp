//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_HPP
#define B_PLUS_TREE_BPLUSTREE_HPP


#include <vector>
#include <functional>
#include <queue>

#include "data_page.hpp"
#include "index_page.hpp"


template <
    typename KeyType,
    typename RecordType,
    typename Greater = std::greater<KeyType>,
    typename Index = std::function<KeyType(RecordType&)>
> class BPlusTree {

    friend struct Page<INDEX_TYPE>;
    friend struct DataPage<INDEX_TYPE>;
    friend struct IndexPage<INDEX_TYPE>;

private:

    std::fstream b_plus_index_file;
    std::fstream metadata_file;

    Property properties;
    Greater gt;
    Index get_indexed_field;

    auto create_index() -> void;

    auto locate_data_page(const KeyType &key) -> std::streampos;

    auto insert(std::streampos seek_page, PageType type, RecordType &record) -> InsertResult;

    auto remove(std::streampos seek_page, PageType type, const KeyType &key) -> RemoveResult<KeyType>;

public:

    explicit BPlusTree(Property property, Index index, Greater greater = Greater());

    auto insert(RecordType &record) -> void;

    auto remove(const KeyType &key) -> void;

    auto search(const KeyType &key) -> std::vector<RecordType>;

    auto above(const KeyType &lower_bound) -> std::vector<RecordType>;

    auto below(const KeyType &upper_bound) -> std::vector<RecordType>;

    auto between(const KeyType &lower_bound, const KeyType &upper_bound) -> std::vector<RecordType>;
};


#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
