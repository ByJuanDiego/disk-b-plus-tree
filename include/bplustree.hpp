//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_HPP
#define B_PLUS_TREE_BPLUSTREE_HPP


#include <vector>
#include <functional>

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

    auto load_metadata() -> void;

    auto save_metadata() -> void;

    auto open(std::fstream & file, const std::string& file_name, std::ios::openmode mode_flags) -> void;

    auto close(std::fstream& file) -> void;

    auto seek_all(std::fstream& file, int64 pos, std::ios::seekdir offset = std::ios::beg) -> void;

    auto locate_data_page(const KeyType& key) -> int64;

    auto create_index() -> void;

    auto insert(int64 seek_page, PageType type, RecordType& record) -> InsertStatus;

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater());

    auto insert(RecordType& record) -> void;

    auto search(const KeyType& key) -> std::vector<RecordType>;

    auto between(const KeyType& lower_bound, const KeyType& upper_bound) -> std::vector<RecordType>;

    auto remove(KeyType& key) -> void;
};


#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
