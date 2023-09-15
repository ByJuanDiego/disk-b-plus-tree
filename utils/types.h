//
// Created by juan diego on 9/15/23.
//

#ifndef B_PLUS_TREE_TYPES_H
#define B_PLUS_TREE_TYPES_H

using int64 = int64_t;
using int32 = int32_t;

// Identifiers for pages type
enum PageType {
    emptyPage = -1,  // Empty Tree
    indexPage = 0,   // Index Page
    dataPage  = 1    // Data Page
};

// Gets the size of the previous page after inserting (it is used recursively)
struct InsertStatus {
    int32 size;
};


#endif //B_PLUS_TREE_TYPES_H
