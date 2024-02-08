//
// Created by juan diego on 04/02/24.
//

#include "page.hpp"


template<DEFINE_INDEX_TYPE>
auto Page<INDEX_TYPE>::save(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    write(this->tree->b_plus_index_file);
}

template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto Page<KeyType, RecordType, Greater, Index>::load(std::streampos pos) -> void {
    seek(this->tree->b_plus_index_file, pos);
    read(this->tree->b_plus_index_file);
}
