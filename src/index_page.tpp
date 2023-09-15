//
// Created by juan diego on 9/15/23.
//

#include "index_page.hpp"


template <typename KeyType>
IndexPage<KeyType>::IndexPage(int32 children_capacity) : capacity(children_capacity), num_keys(0), points_to_leaf(true) {
    keys = new KeyType[capacity];
    children = new int64[capacity + 1];
}


template <typename KeyType>
IndexPage<KeyType>::~IndexPage() {
    delete[] keys;
    delete[] children;
}


template <typename KeyType>
auto IndexPage<KeyType>::size_of() -> int {
    return sizeof(IndexPage<KeyType>);
}


template <typename KeyType>
auto IndexPage<KeyType>::write(std::fstream &file) -> void {
    file.write(reinterpret_cast<char *>(this), sizeof(IndexPage<KeyType>));
}


template <typename KeyType>
auto IndexPage<KeyType>::read(std::fstream &file) -> void {
    file.read(reinterpret_cast<char *>(this), sizeof(IndexPage<KeyType>));
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
    children[1] = child;
    ++num_keys;
}


template<typename KeyType>
auto IndexPage<KeyType>::push_back(KeyType &key, int64 child) -> void {
    keys[0] = key;
    children[0] = child;
    num_keys++;
}


template <typename KeyType>
auto IndexPage<KeyType>::reallocate_references_to_data_pages(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void {
    for (int i = num_keys; i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}

template <typename KeyType>
auto IndexPage<KeyType>::reallocate_references_to_index_pages(int64 child_pos, KeyType& new_key, int64 new_page_seek) -> void {
    for (int i = num_keys; i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}

template <typename KeyType>
template <typename Greater>
auto IndexPage<KeyType>::sorted_insert(KeyType& key, int64 children_seek, Greater greater_to) -> void {
    int key_pos = num_keys - 1;
    while (key_pos >= 0 && greater_to(keys[key_pos], key)) {
        keys[key_pos + 1] = keys[key_pos];
        children[key_pos + 2] = children[key_pos + 1];
        --key_pos;
    }
    keys[key_pos + 1] = key;
    children[key_pos + 2] = children_seek;
    ++num_keys;
}

template <typename KeyType>
auto IndexPage<KeyType>::split(int32 min_index_page_keys, KeyType& new_index_page_key) -> IndexPage<KeyType> {
    IndexPage<KeyType> new_index_page(this->capacity);
    new_index_page.num_keys = min_index_page_keys;

    for (int i = 0; i < new_index_page.num_keys; ++i) {
        int const index = min_index_page_keys + i + 1;
        new_index_page.push_back(this->keys[index], this->children[index]);
    }

    int32 new_key_pos = static_cast<int32>(std::floor(capacity / 2));
    new_index_page_key = this->keys[new_key_pos];
    new_index_page.children[new_index_page.num_keys] = this->children[num_keys];
    num_keys = new_key_pos;

    return new_index_page;
}



template<typename KeyType>
auto IndexPage<KeyType>::get_expected_capacity() -> int32  {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(int32)  - sizeof(int64) - sizeof(bool))  /
            (sizeof(int64) + sizeof(KeyType))
    );
}
