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
    typename FieldType,
    typename RecordType,
    typename Compare = std::greater<FieldType>,
    typename FieldMapping = std::function<FieldType(RecordType&)>
> class BPlusTree {

    friend struct Page<TYPES()>;
    friend struct DataPage<TYPES()>;
    friend struct IndexPage<TYPES()>;

private:

    std::fstream b_plus_index_file;
    std::fstream metadata_file;

    Property properties;
    Compare gt;
    FieldMapping get_search_field;

    auto create_index() -> void;

    auto locate_data_page(const FieldType &key) -> std::streampos;

    auto insert(std::streampos seek_page, PageType type, RecordType &record) -> InsertResult;

    auto remove(std::streampos seek_page, PageType type, const FieldType &key) -> RemoveResult<FieldType>;

public:

    explicit BPlusTree(Property property, FieldMapping search_field, Compare greater = Compare());

    auto insert(RecordType &record) -> void;

    auto remove(const FieldType &key) -> void;

    auto search(const FieldType &key) -> std::vector<RecordType>;

    auto above(const FieldType &lower_bound) -> std::vector<RecordType>;

    auto below(const FieldType &upper_bound) -> std::vector<RecordType>;

    auto between(const FieldType &lower_bound, const FieldType &upper_bound) -> std::vector<RecordType>;
};


#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
