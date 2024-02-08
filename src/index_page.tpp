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
auto IndexPage<INDEX_TYPE>::balance_page_insert(std::streampos seek_parent,
                                                IndexPage<KeyType, RecordType, Greater, Index> &parent,
                                                std::int32_t child_pos) -> void {
    std::streampos child_seek = parent.children[child_pos];

    SplitResult<INDEX_TYPE> split = this->split(this->tree->metadata_json[INDEX_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<IndexPage<INDEX_TYPE>>(split.new_page);

    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();
    new_page->write(this->tree->b_plus_index_file);

    this->save(child_seek);

    parent.reallocate_references_after_split(child_pos, split.split_key, new_page_seek);

    // Seek to the location of the parent index page in the index file and update it
    parent.save(seek_parent);
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::balance_page_remove(std::streampos seek_parent, IndexPage<INDEX_TYPE>& parent, std::int32_t child_pos) -> void {
    std::int32_t minimum = this->tree->metadata_json[MINIMUM_DATA_PAGE_RECORDS].asInt();
    if (len() >= minimum) {
        return;
    }

    IndexPage<INDEX_TYPE> left_sibling(this->tree);
    IndexPage<INDEX_TYPE> right_sibling(this->tree);

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        seek(this->tree->b_plus_index_file, seek_left_sibling);
        left_sibling.read(this->tree->b_plus_index_file);

        if (left_sibling.len() > minimum) {
            // left-borrow
            auto [last_key, last_child] = left_sibling.pop_back();
            this->push_front(parent.keys[child_pos - 1], last_child);
            parent.keys[child_pos - 1] = last_key;

            // save changes
            left_sibling.save(seek_left_sibling);
            this->save(parent.children[child_pos]);
            parent.save(seek_parent);
            return;
        }
    }
    else {
        std::streampos seek_right_sibling = parent.children[1];
        seek(this->tree->b_plus_index_file, seek_right_sibling);
        right_sibling.read(this->tree->b_plus_index_file);

        if (right_sibling.len() > minimum) {
            // right-borrow
            auto [first_key, first_child] = right_sibling.pop_front();
            this->push_back(parent.keys[0], first_child);
            parent.keys[0] = first_key;

            // save changes
            right_sibling.save(seek_right_sibling);
            this->save(parent.children[0]);
            parent.save(seek_parent);
            return;
        }
    }

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        // left-merge
        left_sibling.merge(*this, parent.keys[child_pos - 1]);
        parent.reallocate_references_after_merge(child_pos - 1);

        // save changes
        left_sibling.save(seek_left_sibling);
        parent.save(seek_parent);
    } else {
        // right-merge
        this->merge(right_sibling, parent.keys[0]);
        parent.reallocate_references_after_merge(0);

        // save changes
        this->save(parent.children[0]);
        parent.save(seek_parent);
    }
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::balance_root_insert(std::streampos old_root_seek) -> void {
    SplitResult<INDEX_TYPE> split = this->split(this->tree->metadata_json[INDEX_PAGE_CAPACITY].asInt() / 2);
    auto new_page = std::dynamic_pointer_cast<IndexPage<INDEX_TYPE>>(split.new_page);
    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();
    new_page->write(this->tree->b_plus_index_file);

    this->save(old_root_seek);

    IndexPage<INDEX_TYPE> new_root(this->tree, false);
    new_root.num_keys = 1;
    new_root.keys[0] = split.split_key;
    new_root.children[0] = old_root_seek;
    new_root.children[1] = new_page_seek;

    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_root_seek = this->tree->b_plus_index_file.tellp();
    new_root.write(this->tree->b_plus_index_file);

    this->tree->metadata_json[SEEK_ROOT] = static_cast<Json::Int64>(new_root_seek);
}


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::balance_root_remove() -> void {
    if (len() == 0) {
        this->tree->metadata_json[SEEK_ROOT] = children[0];

        if (points_to_leaf) {
            this->tree->metadata_json[ROOT_STATUS] = dataPage;
        }
    }
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::push_front(KeyType& key, std::streampos child) -> void {
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
auto IndexPage<INDEX_TYPE>::push_back(KeyType &key, std::streampos child) -> void {
    if (num_keys == this->capacity) {
        throw FullPage();
    }

    keys[num_keys] = key;
    children[num_keys + 1] = child;
    ++num_keys;
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::pop_front() -> std::pair<KeyType, std::streampos> {
    if (len() == 0) {
        throw EmptyPage();
    }

    KeyType key = keys[0];
    for (std::int32_t i = 0; i < len() - 1; ++i) {
        keys[i] = keys[i + 1];
    }

    std::streampos child = children[0];
    for (std::int32_t i = 0; i < len(); ++i) {
        children[i] = children[i + 1];
    }

    --num_keys;
    return std::make_pair(key, child);
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::pop_back() -> std::pair<KeyType, std::streampos> {
    if (len() == 0) {
        throw EmptyPage();
    }

    --num_keys;
    return std::make_pair(keys[num_keys], children[num_keys + 1]);
}


template <DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::reallocate_references_after_split(std::int32_t child_pos, KeyType& new_key, std::int64_t new_page_seek) -> void {
    for (int i = len(); i > child_pos; --i) {
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


template<DEFINE_INDEX_TYPE>
auto IndexPage<INDEX_TYPE>::merge(IndexPage<INDEX_TYPE> &right_sibling, KeyType& new_key) -> void {
    push_back(new_key, right_sibling.children[0]);
    for (std::int32_t i = 0; i < right_sibling.len(); ++i) {
        push_back(right_sibling.keys[i], right_sibling.children[i + 1]);
    }
}


template <typename KeyType>
auto get_expected_index_page_capacity() -> std::int32_t {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int32_t)  - sizeof(std::int64_t) - sizeof(bool))  /
            (sizeof(std::int64_t) + sizeof(KeyType))
    );
}
