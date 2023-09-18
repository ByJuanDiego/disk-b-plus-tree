//
// Created by juan diego on 9/15/23.
//

#include "index_page.hpp"

template <typename KeyType>
IndexPage<KeyType>::IndexPage(int32 children_capacity, bool points_to_leaf)
    : capacity(children_capacity), num_keys(0), points_to_leaf(points_to_leaf),
      keys(capacity, KeyType()), children(capacity + 1, emptyPage) {
}


template<typename KeyType>
IndexPage<KeyType>::IndexPage(const IndexPage<KeyType> &other)
        : capacity(other.capacity), num_keys(other.num_keys),
          points_to_leaf(other.points_to_leaf), keys(capacity, KeyType()), children(capacity + 1, emptyPage) {

    for (int i = 0; i < num_keys; ++i) {
        keys[i] = other.keys[i];
    }

    for (int i = 0; i < num_keys + 1; ++i) {
        children[i] = other.children[i];
    }
}


template <typename KeyType>
IndexPage<KeyType>::~IndexPage() = default;


template <typename KeyType>
auto IndexPage<KeyType>::size_of() -> int {
    return 2 * sizeof(int32) + capacity * sizeof(KeyType) + (capacity + 1) * sizeof(int64) + sizeof(bool);
}


template <typename KeyType>
auto IndexPage<KeyType>::write(std::fstream &file) -> void {
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


template <typename KeyType>
auto IndexPage<KeyType>::read(std::fstream &file) -> void {
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


template <typename KeyType>
auto IndexPage<KeyType>::push_front(KeyType& key, int64 child) -> void {
    if (num_keys == capacity) {
        throw FullPage();
    }

    for (int i = num_keys; i > 0; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[0] = key;
    children[0] = child;
    ++num_keys;
}


template<typename KeyType>
auto IndexPage<KeyType>::push_back(KeyType &key, int64 child) -> void {
    if (num_keys == capacity) {
        throw FullPage();
    }

    keys[num_keys] = key;
    children[++num_keys] = child;
}


template <typename KeyType>
auto IndexPage<KeyType>::reallocate_references(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void {
    for (int i = num_keys; i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}

template <typename KeyType>
auto IndexPage<KeyType>::split(int32 new_key_pos, KeyType& new_index_page_key) -> IndexPage<KeyType> {
    IndexPage<KeyType> new_index_page(capacity, points_to_leaf);

    for (int i = new_key_pos + 1; i < num_keys; ++i) {
        new_index_page.push_back(keys[i], children[i]);
    }
    new_index_page.children[new_index_page.num_keys] = children[num_keys];
    new_index_page_key = keys[new_key_pos];
    num_keys -= (new_index_page.num_keys + 1);

    return new_index_page;
}


template<typename KeyType>
auto IndexPage<KeyType>::get_expected_capacity() -> int32  {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(int32)  - sizeof(int64) - sizeof(bool))  /
            (sizeof(int64) + sizeof(KeyType))
    );
}
