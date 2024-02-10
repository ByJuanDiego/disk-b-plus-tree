//
// Created by juan diego on 9/15/23.
//

#ifndef B_PLUS_TREE_PAGE_HPP
#define B_PLUS_TREE_PAGE_HPP

#include <memory>
#include <fstream>
#include <cstdint>

#include "property.hpp"
#include "file_utils.hpp"


#define TYPES(T) T FieldType, T RecordType, T Compare, T FieldMapping

template<TYPES(typename)>
struct SplitResult;

template<TYPES(typename)>
class BPlusTree;

template<TYPES(typename)>
struct IndexPage;


template<TYPES(typename)>
struct Page {
protected:
    BPlusTree<TYPES()> *tree;
public:

    explicit Page(BPlusTree<TYPES()> *tree);

    virtual ~Page();

    auto is_full() -> bool;

    auto is_empty() -> bool;

    auto save(std::streampos pos) -> void;

    auto load(std::streampos pos) -> void;

    virtual auto write() -> void = 0;

    virtual auto read() -> void = 0;

    virtual auto bytes_len() -> std::int32_t = 0;

    virtual auto len() -> std::size_t = 0;

    virtual auto max_capacity() -> std::size_t = 0;

    virtual auto split(std::int32_t split_pos) -> SplitResult<TYPES()> = 0;

    virtual auto balance_page_remove(
            std::streampos seek_parent,
            IndexPage<TYPES()> &parent,
            std::int32_t child_pos) -> void = 0;

    virtual auto balance_page_insert(
            std::streampos seek_parent,
            IndexPage<TYPES()> &parent,
            std::int32_t child_pos) -> void = 0;

    virtual auto balance_root_insert(std::streampos old_root_seek) -> void = 0;

    virtual auto balance_root_remove() -> void = 0;
};


template<TYPES(typename)>
struct SplitResult {
    std::shared_ptr<Page<TYPES()>> new_page;
    FieldType split_key;
};


// Gets the size of the previous page after inserting (it is used recursively)
struct InsertResult {
    std::size_t size;
};


template<typename FieldType>
struct RemoveResult {
    std::size_t size;
    std::shared_ptr<FieldType> predecessor;
};


#include "page.tpp"

#endif //B_PLUS_TREE_PAGE_HPP
