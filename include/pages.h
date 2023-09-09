//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_PAGES_H
#define B_PLUS_TREE_PAGES_H

#include <cmath>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <cstring>


#include "buffer_size.h"

using int64 = int64_t;
using int32 = int32_t;

const int LEAF_NULL_POINTER = -1;
const unsigned int BUFFER_SIZE = get_buffer_size();

template <typename KeyType>
struct IndexPage {
    int32 capacity;
    int32 num_keys;
    KeyType* keys;
    int64* children;
    bool points_to_leaf;

    explicit IndexPage(int32 keys_capacity)
    : capacity(keys_capacity), num_keys(0), points_to_leaf(false) {
        keys = new KeyType[keys_capacity];
        children = new int64[keys_capacity + 1];
    }

    ~IndexPage() {
        delete [] keys;
        delete [] children;
    }

    int size_of() {
        return 2 * sizeof(int32) + capacity * sizeof(KeyType) + (capacity + 1) * sizeof(int64) + sizeof(bool);
    }

    void write(std::fstream& file) {
        std::stringstream ss;
        ss.write((char *) &capacity, sizeof(int32));
        ss.write((char *) &num_keys, sizeof(int32));

        for (int i = 0; i < capacity; ++i)
            ss.write((char *) &keys[i], sizeof(KeyType));

        for (int i = 0; i <= capacity; ++i)
            ss.write((char *) &children[i], sizeof(int64));

        ss.write((char *) &points_to_leaf, sizeof(bool));
        file.write(ss.str().c_str(), size_of());
    }

    void read(std::fstream& file) {
        char* buffer = new char[size_of()];
        file.read(buffer, size_of());

        int offset = 0;
        memcpy((char *)& capacity, buffer + offset, sizeof(int32));
        offset += sizeof(int32);
        memcpy((char *)& num_keys, buffer + offset, sizeof(int32));
        offset += sizeof(int32);

        for (int i = 0; i < capacity; ++i) {
            memcpy((char *) &keys[i], buffer + offset, sizeof(KeyType));
            offset += sizeof(KeyType);
        }

        for (int i = 0; i <= capacity; ++i) {
            memcpy((char *) &children[i], buffer + offset, sizeof(int64));
            offset += sizeof(int64);
        }

        memcpy((char *) &points_to_leaf, buffer + offset, sizeof(bool));
        delete [] buffer;
    }
};

template <typename RecordType>
struct DataPage {
    int32 capacity;
    int32 num_records;
    int64 next_leaf;
    int64 prev_leaf;
    RecordType* records;

    explicit DataPage(int32 records_capacity)
    : capacity(records_capacity), num_records(0), next_leaf(LEAF_NULL_POINTER), prev_leaf(LEAF_NULL_POINTER) {
        records = new RecordType[records_capacity];
    }

    ~DataPage() {
        delete [] records;
    }


    void write(std::fstream& file) {

    }

    void read(std::fstream& file) {

    }
};


template <typename KeyType>
int32 get_expected_index_page_capacity() {
    return std::floor(
            static_cast<double>(BUFFER_SIZE - 2 * sizeof(int32) - sizeof(bool) - sizeof(int64))  /
            (sizeof(int64) + sizeof(KeyType))
            );
}

template <typename RecordType>
int32 get_expected_data_page_capacity() {
    return std::floor(
            static_cast<double>(BUFFER_SIZE - 2* sizeof(int64) - 2 * sizeof(int32)) /
            (sizeof(RecordType))
            );
}

#endif //B_PLUS_TREE_PAGES_H
