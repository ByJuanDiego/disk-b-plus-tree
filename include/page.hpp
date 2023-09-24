//
// Created by juan diego on 9/15/23.
//

#ifndef B_PLUS_TREE_PAGE_HPP
#define B_PLUS_TREE_PAGE_HPP

#include <memory>
#include <fstream>
#include <cstdint>


template <typename KeyType>
struct SplitResult;



template <typename KeyType>
struct Page {
protected:
    std::int32_t capacity;

public:

    explicit Page(std::int32_t capacity): capacity(capacity) {}

    virtual ~Page() = default;

    virtual auto write(std::fstream & file)         -> void = 0;
    virtual auto read(std::fstream & file)          -> void = 0;

    virtual auto size_of()                          -> std::int32_t = 0;
    virtual auto split(std::int32_t split_position) -> SplitResult<KeyType> = 0;
};



// Identifiers for pages type
enum PageType {
    emptyPage = -1,  // Empty Tree
    indexPage = 0,   // Index Page
    dataPage  = 1    // Data Page
};



template <typename KeyType>
struct SplitResult {
    std::shared_ptr<Page<KeyType>> new_page;
    KeyType split_key;
};



// Gets the size of the previous page after inserting (it is used recursively)
struct InsertResult {
    std::int32_t size;
};



#endif //B_PLUS_TREE_PAGE_HPP
