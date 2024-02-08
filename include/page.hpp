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


#define DEFINE_INDEX_TYPE typename KeyType, typename RecordType, typename Greater, typename Index
#define INDEX_TYPE KeyType, RecordType, Greater, Index


template <DEFINE_INDEX_TYPE>
struct SplitResult;

template <DEFINE_INDEX_TYPE>
class BPlusTree;

template <DEFINE_INDEX_TYPE>
struct IndexPage;


// Identifiers for pages type
enum PageType {
    emptyPage = -1,  // Empty Tree
    indexPage = 0,   // Index Page
    dataPage  = 1    // Data Page
};

template <DEFINE_INDEX_TYPE>
struct Page {
protected:
    std::int32_t capacity;
    BPlusTree<INDEX_TYPE>* tree;
public:

    explicit Page(std::int32_t capacity, BPlusTree<INDEX_TYPE>* b_plus): tree(b_plus), capacity(capacity) {}

    virtual ~Page() = default;

    auto save(std::streampos pos)                                  -> void;
    auto load(std::streampos pos)                                  -> void;

    virtual auto write(std::fstream & file)                        -> void = 0;
    virtual auto read(std::fstream & file)                         -> void = 0;

    virtual auto size_of()                                         -> std::int32_t = 0;
    virtual auto len()                                             -> std::size_t = 0;
    virtual auto split(std::int32_t split_pos)                     -> SplitResult<INDEX_TYPE> = 0;

    virtual auto balance_page_remove(
            std::streampos seek_parent,
            IndexPage<INDEX_TYPE>& parent,
            std::int32_t child_pos)                                -> void = 0;

    virtual auto balance_page_insert(
            std::streampos seek_parent,
            IndexPage<INDEX_TYPE>& parent,
            std::int32_t child_pos)                                -> void = 0;

    virtual auto balance_root_insert(std::streampos old_root_seek) -> void = 0;
    virtual auto balance_root_remove()                             -> void = 0;
};


template <DEFINE_INDEX_TYPE>
struct SplitResult {
    std::shared_ptr<Page<INDEX_TYPE>> new_page;
    KeyType split_key;
};


// Gets the size of the previous page after inserting (it is used recursively)
struct InsertResult {
    std::size_t size;
};


template <typename KeyType>
struct RemoveResult {
    std::size_t size;
    std::shared_ptr<KeyType> predecessor;
};


#include "page.tpp"

#endif //B_PLUS_TREE_PAGE_HPP
