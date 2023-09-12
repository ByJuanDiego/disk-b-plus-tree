//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_PAGES_HPP
#define B_PLUS_TREE_PAGES_HPP

#include <cmath>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <cstring>

#include "buffer_size.hpp"
#include "error_handler.hpp"

using int64 = int64_t;
using int32 = int32_t;

const int NULL_PAGE = -1;
const int INITIAL_PAGE = 0;
const unsigned int BUFFER_SIZE = get_buffer_size();


template <typename KeyType>
int32 get_expected_index_page_capacity() {
    return std::floor(
            static_cast<double>(BUFFER_SIZE - 2 * sizeof(int32)  - sizeof(int64) - sizeof(bool))  /
            (sizeof(int64) + sizeof(KeyType))
    );
}

template <typename RecordType>
int32 get_expected_data_page_capacity() {
    return std::floor(
            static_cast<double>(BUFFER_SIZE - 2 * sizeof(int64) - 2 * sizeof(int32)) /
            (sizeof(RecordType))
    );
}

template <typename KeyType>
struct IndexPage {
    int32 capacity;           // The maximum capacity of keys and children arrays.
    int32 num_keys;           // The current number of keys stored in the page.
    KeyType* keys;            // An array of keys stored in the page.
    int64* children;          // An array of child pointers corresponding to the keys.
    bool points_to_leaf;      // Indicates whether this index page points to a leaf node.

    explicit IndexPage(int32 children_capacity)
    : capacity(children_capacity), num_keys(0), points_to_leaf(false) {
        // Allocate memory for the keys array with the specified capacity.
        // The last key serves as a sentinel to facilitate overflow handling.
        keys = new KeyType[children_capacity];
        // Allocate memory for the children array, allowing one extra for overflow handling.
        children = new int64[children_capacity + 1];
    }

    ~IndexPage() {
        delete [] keys;
        delete [] children;
    }

    int size_of()  {
        return 2 * sizeof(int32) + capacity * sizeof(KeyType) + (capacity + 1) * sizeof(int64) + sizeof(bool);
    }

    void write(std::ofstream &file) {
        char* buffer = new char[size_of()];

        int offset = 0;
        memcpy(buffer + offset, (char *)&capacity, sizeof(int32));
        offset += sizeof(int32);
        memcpy(buffer + offset, (char *)&num_keys, sizeof(int32));
        offset += sizeof(int32);

        for (int i = 0; i < capacity; ++i) {
            memcpy(buffer + offset, (char *)&keys[i], sizeof(KeyType));
            offset += sizeof(KeyType);
        }

        for (int i = 0; i <= capacity; ++i) {
            memcpy(buffer + offset, (char *)&children[i], sizeof(int64));
            offset += sizeof(int64);
        }

        memcpy(buffer + offset, (char *)&points_to_leaf, sizeof(bool));

        file.write(buffer, size_of());

        delete [] buffer;
    }

    void read(std::ifstream & file) {
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

    void push_front(KeyType& key, int64 child) {
        if (num_keys == capacity) {
            throw FullPage();
        }

        int i;
        for (i = num_keys - 1; i > 0; ) {
            // TODO
        }

        num_keys++;
    }

    void push_back(KeyType& key, int64 child) {
        if (num_keys == capacity) {
            throw FullPage();
        }

        keys[num_keys] = key;
        children[++num_keys] = child;
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
    : capacity(records_capacity), num_records(0), next_leaf(NULL_PAGE), prev_leaf(NULL_PAGE) {
        records = new RecordType[records_capacity];
    }

    ~DataPage() {
        delete [] records;
    }

    int size_of() {
        return 2 * sizeof(int32) + 2 * sizeof(int64) + capacity * sizeof(RecordType);
    }

    void write(std::ofstream & file) {
        char* buffer = new char[size_of()];
        int offset = 0;

        memcpy(buffer + offset, (char *) &capacity, sizeof(int32));
        offset += sizeof(int32);

        memcpy(buffer + offset, (char *) &num_records, sizeof(int32));
        offset += sizeof(int32);

        memcpy(buffer + offset, (char *) &next_leaf, sizeof(int64));
        offset += sizeof(int64);

        memcpy(buffer + offset, (char *) &prev_leaf, sizeof(int64));
        offset += sizeof(int64);

        for (int i = 0; i < num_records; ++i) {
            memcpy(buffer + offset, (char *) &records[i], sizeof(RecordType));
            offset += sizeof(RecordType);
        }

        file.write(buffer, size_of());
        delete [] buffer;
    }

    /**
     * @brief Read data from an input file stream and populate the object's attributes.
     *
     * This function reads data from the provided input file stream and populates the
     * object's attributes by copying the data from a binary buffer. It assumes that
     * the buffer contains serialized data in a specific format.
     *
     * @param file An input file stream from which data is read.
     */
    void read(std::ifstream & file) {
        char* buffer = new char[size_of()];
        int offset = 0;
        file.read(buffer, size_of());

        memcpy((char *) &capacity, buffer + offset, sizeof(int32));
        offset += sizeof(int32);

        memcpy((char *) &num_records, buffer + offset, sizeof(int32));
        offset += sizeof(int32);

        memcpy((char *) & next_leaf, buffer + offset, sizeof(int64));
        offset += sizeof(int64);

        memcpy((char *) & prev_leaf, buffer + offset, sizeof(int64));
        offset += sizeof(int64);


        for (int i = 0; i < num_records; ++i) {
            memcpy((char *) & records[i], buffer + offset, sizeof(RecordType));
            offset += sizeof(RecordType);
        }

        delete [] buffer;
    }

    void push_front(RecordType& record) {
        if (num_records == capacity) {
            throw FullPage();
        }

        int i;
        for (i = num_records - 1; i > 0; records[i + 1] = records[i--]);
        records[0] = record;
        num_records++;
    }

    void push_back(RecordType& record) {
        if (num_records == capacity) {
            throw FullPage();
        }

        records[(num_records++) - 1] = record;
    }

    template <typename KeyType, typename Greater>
    void sorted_insert(RecordType& record, KeyType key, Greater greater_to) {
        int i;
        for (i = num_records - 1; i >= 0 && greater_to(records[i], key); records[i + 1] = records[i--]);
        records[i] = record;
        ++num_records;
    }
};


#endif //B_PLUS_TREE_PAGES_HPP
