//
// Created by juan diego on 04/02/24.
//

#include "page.hpp"


template<TYPES(typename)>
Page<TYPES()>::Page(BPlusTree<TYPES()> *tree): tree(tree) {
}


template<TYPES(typename)>
Page<TYPES()>::~Page() = default;


template<TYPES(typename)>
auto Page<TYPES()>::is_full() -> bool {
    return len() == max_capacity();
}


template<TYPES(typename)>
auto Page<TYPES()>::is_empty() -> bool {
    return len() == 0;
}


template<TYPES(typename)>
auto Page<TYPES()>::save(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    write();
}

template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto Page<KeyType, RecordType, Greater, Index>::load(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    read();
}
