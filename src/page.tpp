//
// Created by juan diego on 04/02/24.
//

#include "page.hpp"


template<DEFINE_INDEX_TYPE>
Page<INDEX_TYPE>::Page(BPlusTree<INDEX_TYPE> *tree): tree(tree) {
}


template<DEFINE_INDEX_TYPE>
Page<INDEX_TYPE>::~Page() = default;


template<DEFINE_INDEX_TYPE>
auto Page<INDEX_TYPE>::is_full() -> bool {
    return len() == max_capacity();
}


template<DEFINE_INDEX_TYPE>
auto Page<INDEX_TYPE>::is_empty() -> bool {
    return len() == 0;
}


template<DEFINE_INDEX_TYPE>
auto Page<INDEX_TYPE>::save(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    write();
}

template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto Page<KeyType, RecordType, Greater, Index>::load(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    read();
}
