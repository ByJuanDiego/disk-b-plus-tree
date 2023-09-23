//
// Created by juan diego on 9/15/23.
//

#include "index_page.hpp"

template <typename KeyType>
IndexPage<KeyType>::IndexPage(std::int32_t capacity, bool points_to_leaf)
    : Page<KeyType>(capacity), num_keys(0), points_to_leaf(points_to_leaf),
      keys(this->capacity, KeyType()), children(this->capacity + 1, emptyPage) {
}


template<typename KeyType>
IndexPage<KeyType>::IndexPage(const IndexPage<KeyType> &other)
        : Page<KeyType>(other.capacity), num_keys(other.num_keys),
          points_to_leaf(other.points_to_leaf), keys(this->capacity, KeyType()), children(this->capacity + 1, emptyPage) {

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
    return 2 * sizeof(std::int32_t) + this->capacity * sizeof(KeyType) + (this->capacity + 1) * sizeof(std::int64_t) + sizeof(bool);
}


template <typename KeyType>
auto IndexPage<KeyType>::write(std::fstream &file) -> void {
    char* buffer = new char[size_of()];

    int offset = 0;
    memcpy(buffer + offset, (char *)& this->capacity, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);
    memcpy(buffer + offset, (char *) &num_keys, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    for (int i = 0; i < this->capacity; ++i) {
        memcpy(buffer + offset, (char *)&keys[i], sizeof(KeyType));
        offset += sizeof(KeyType);
    }

    for (int i = 0; i <= this->capacity; ++i) {
        memcpy(buffer + offset, (char *)&children[i], sizeof(std::int64_t));
        offset += sizeof(std::int64_t);
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
    memcpy((char *)& this->capacity, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);
    memcpy((char *)& num_keys, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    for (int i = 0; i < this->capacity; ++i) {
        memcpy((char *) &keys[i], buffer + offset, sizeof(KeyType));
        offset += sizeof(KeyType);
    }

    for (int i = 0; i <= this->capacity; ++i) {
        memcpy((char *) &children[i], buffer + offset, sizeof(std::int64_t));
        offset += sizeof(std::int64_t);
    }

    memcpy((char *) &points_to_leaf, buffer + offset, sizeof(bool));
    delete [] buffer;
}


template <typename KeyType>
auto IndexPage<KeyType>::push_front(KeyType& key, std::int64_t child) -> void {
    if (num_keys == this->capacity) {
        throw FullPage();
    }

    for (int i = num_keys; i > 0; --i) {
        keys[i] = keys[i - 1];
    }

    for (int i = num_keys + 1; i > 0; --i) {
        children[i] = children[i - 1];
    }

    keys[0] = key;
    children[0] = child;
    ++num_keys;
}


template<typename KeyType>
auto IndexPage<KeyType>::push_back(KeyType &key, std::int64_t child) -> void {
    if (num_keys == this->capacity) {
        throw FullPage();
    }

    keys[num_keys] = key;
    children[num_keys + 1] = child;
    ++num_keys;
}


template <typename KeyType>
auto IndexPage<KeyType>::reallocate_references(std::int32_t child_pos, KeyType& new_key, std::int64_t new_page_seek) -> void {
    for (int i = num_keys; i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}

template <typename KeyType>
auto IndexPage<KeyType>::split(std::int32_t new_key_pos) -> SplitResult<KeyType> {
    auto new_index_page = std::make_shared<IndexPage<KeyType>>(this->capacity, points_to_leaf);

    for (int i = new_key_pos + 1; i < num_keys; ++i) {
        new_index_page->push_back(keys[i], children[i + 1]);
    }
    new_index_page->children[0] = children[new_key_pos + 1];

    num_keys -= (new_index_page->num_keys + 1);
    return SplitResult<KeyType> { new_index_page, keys[new_key_pos] };
}


template <typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int32_t)  - sizeof(std::int64_t) - sizeof(bool))  /
            (sizeof(std::int64_t) + sizeof(KeyType))
    );
}
