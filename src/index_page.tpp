//
// Created by juan diego on 9/15/23.
//

#include "index_page.hpp"

template <DEFINE_INDEX_TYPE>
IndexPage<INDEX_TYPE>::IndexPage(BPlusTree<INDEX_TYPE>* b_plus, bool points_to_leaf)
    : Page<INDEX_TYPE>(b_plus->metadata_json[INDEX_PAGE_CAPACITY].asInt(), b_plus), num_keys(0), points_to_leaf(points_to_leaf),
      keys(this->capacity, KeyType()), children(this->capacity + 1, emptyPage) {
}

template <DEFINE_INDEX_TYPE>
IndexPage<INDEX_TYPE>::~IndexPage() = default;


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::size_of() -> int {
    return 2 * sizeof(std::int32_t) + this->capacity * sizeof(KeyType) + (this->capacity + 1) * sizeof(std::int64_t) + sizeof(bool);
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::len() -> std::size_t {
    return this->num_keys;
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::write(std::fstream &file) -> void {
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


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::read(std::fstream &file) -> void {
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


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::split(std::int32_t split_position) -> SplitResult<INDEX_TYPE> {
    auto new_index_page = std::make_shared<IndexPage<INDEX_TYPE>>(this->tree, points_to_leaf);
    KeyType new_key = keys[split_position];

    for (int i = split_position + 1; i < num_keys; ++i) {
        new_index_page->push_back(keys[i], children[i + 1]);
    }
    new_index_page->children[0] = children[split_position + 1];

    num_keys -= (new_index_page->num_keys + 1);
    return SplitResult<INDEX_TYPE> { new_index_page, new_key };
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::balance(std::streampos seek_parent, IndexPage<INDEX_TYPE>& parent, std::int32_t child_pos) -> void {
    // TODO
    throw LogicError();
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::deallocate_root() -> void {
    if (len() == 0) {
        this->tree->metadata_json[SEEK_ROOT] = children[0];

        if (points_to_leaf) {
            this->tree->metadata_json[ROOT_STATUS] = dataPage;
        }
    }
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::push_front(KeyType& key, std::int64_t child) -> void {
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


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::push_back(KeyType &key, std::int64_t child) -> void {
    if (num_keys == this->capacity) {
        throw FullPage();
    }

    keys[num_keys] = key;
    children[num_keys + 1] = child;
    ++num_keys;
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::reallocate_references_after_split(std::int32_t child_pos, KeyType& new_key, std::int64_t new_page_seek) -> void {
    for (int i = num_keys; i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::reallocate_references_after_merge(std::int32_t merged_child_pos) -> void {
    for (std::int32_t i = merged_child_pos; i < len() - 1; ++i) {
        keys[i] = keys[i + 1];
        children[i + 1] = children[i + 2];
    }
    --num_keys;
}


template <typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int32_t)  - sizeof(std::int64_t) - sizeof(bool))  /
            (sizeof(std::int64_t) + sizeof(KeyType))
    );
}
