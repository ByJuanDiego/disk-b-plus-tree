//
// Created by juan diego on 9/15/23.
//

#include "index_page.hpp"


template <TYPES(typename)>
IndexPage<TYPES()>::IndexPage(BPlusTree<TYPES()>* tree, bool points_to_leaf)
    : Page<TYPES()>(tree), num_keys(0), points_to_leaf(points_to_leaf){
    keys.resize(max_capacity(), FieldType());
    children.resize(max_capacity() + 1, emptyPage);
}


template <TYPES(typename)>
IndexPage<TYPES()>::~IndexPage() = default;


template <TYPES(typename)>
auto IndexPage<TYPES()>::bytes_len() -> int {
    return sizeof(std::int32_t) + max_capacity() * sizeof(FieldType) + (max_capacity() + 1) * sizeof(std::int64_t) + sizeof(bool);
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::len() -> std::size_t {
    return this->num_keys;
}


template<typename FieldType, typename RecordType, typename Greater, typename Index>
auto IndexPage<FieldType, RecordType, Greater, Index>::max_capacity() -> std::size_t {
    return this->tree->properties.MAX_INDEX_PAGE_CAPACITY;
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::write() -> void {
    char* buffer = new char[bytes_len()];

    int offset = 0;
    memcpy(buffer + offset, (char *) &num_keys, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    for (int i = 0; i < max_capacity(); ++i) {
        memcpy(buffer + offset, (char *)&keys[i], sizeof(FieldType));
        offset += sizeof(FieldType);
    }

    for (int i = 0; i <= max_capacity(); ++i) {
        memcpy(buffer + offset, (char *)&children[i], sizeof(std::int64_t));
        offset += sizeof(std::int64_t);
    }

    memcpy(buffer + offset, (char *)&points_to_leaf, sizeof(bool));
    this->tree->b_plus_index_file.write(buffer, bytes_len());

    delete [] buffer;
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::read() -> void {
    char* buffer = new char[bytes_len()];
    this->tree->b_plus_index_file.read(buffer, bytes_len());

    int offset = 0;
    memcpy((char *)& num_keys, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    for (int i = 0; i < max_capacity(); ++i) {
        memcpy((char *) &keys[i], buffer + offset, sizeof(FieldType));
        offset += sizeof(FieldType);
    }

    for (int i = 0; i <= max_capacity(); ++i) {
        memcpy((char *) &children[i], buffer + offset, sizeof(std::int64_t));
        offset += sizeof(std::int64_t);
    }

    memcpy((char *) &points_to_leaf, buffer + offset, sizeof(bool));
    delete [] buffer;
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::split(std::int32_t split_pos) -> SplitResult<TYPES()> {
    auto new_index_page = std::make_shared<IndexPage<TYPES()>>(this->tree, points_to_leaf);
    FieldType new_key = keys[split_pos];

    for (int i = split_pos + 1; i < num_keys; ++i) {
        new_index_page->push_back(keys[i], children[i + 1]);
    }
    new_index_page->children[0] = children[split_pos + 1];

    num_keys -= (new_index_page->num_keys + 1);
    return SplitResult<TYPES()> { new_index_page, new_key };
}


template<TYPES(typename)>
auto IndexPage<TYPES()>::balance_page_insert(std::streampos seek_parent,
                                                IndexPage<TYPES()> &parent,
                                                std::int32_t child_pos) -> void {
    std::streampos child_seek = parent.children[child_pos];

    SplitResult<TYPES()> split = this->split(this->tree->properties.SPLIT_POS_INDEX_PAGE);
    auto new_page = std::dynamic_pointer_cast<IndexPage<TYPES()>>(split.new_page);

    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();
    new_page->write();

    this->save(child_seek);

    parent.reallocate_references_after_split(child_pos, split.split_key, new_page_seek);

    // Seek to the location of the parent index page in the index file and update it
    parent.save(seek_parent);
}


template<TYPES(typename)>
auto IndexPage<TYPES()>::balance_page_remove(std::streampos seek_parent, IndexPage<TYPES()>& parent, std::int32_t child_pos) -> void {
    std::int32_t minimum = this->tree->properties.MIN_INDEX_PAGE_CAPACITY;
    if (len() >= minimum) {
        return;
    }

    IndexPage<TYPES()> left_sibling(this->tree);
    IndexPage<TYPES()> right_sibling(this->tree);

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        left_sibling.load(seek_left_sibling);

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
        right_sibling.load(seek_right_sibling);

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


template<TYPES(typename)>
auto IndexPage<TYPES()>::balance_root_insert(std::streampos old_root_seek) -> void {
    SplitResult<TYPES()> split = this->split(this->tree->properties.SPLIT_POS_INDEX_PAGE);
    auto new_page = std::dynamic_pointer_cast<IndexPage<TYPES()>>(split.new_page);
    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();
    new_page->write();

    this->save(old_root_seek);

    IndexPage<TYPES()> new_root(this->tree, false);
    new_root.num_keys = 1;
    new_root.keys[0] = split.split_key;
    new_root.children[0] = old_root_seek;
    new_root.children[1] = new_page_seek;

    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_root_seek = this->tree->b_plus_index_file.tellp();
    new_root.write();

    this->tree->properties.SEEK_ROOT = new_root_seek;
}


template<TYPES(typename)>
auto IndexPage<TYPES()>::balance_root_remove() -> void {
    if (this->is_empty()) {
        this->tree->properties.SEEK_ROOT = children[0];

        if (points_to_leaf) {
            this->tree->properties.ROOT_STATUS = dataPage;
        }
    }
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::push_front(FieldType& key, std::streampos child) -> void {
    if (this->is_full()) {
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


template <TYPES(typename)>
auto IndexPage<TYPES()>::push_back(FieldType &key, std::streampos child) -> void {
    if (this->is_full()) {
        throw FullPage();
    }

    keys[num_keys] = key;
    children[num_keys + 1] = child;
    ++num_keys;
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::pop_front() -> std::pair<FieldType, std::streampos> {
    if (this->is_empty()) {
        throw EmptyPage();
    }

    FieldType key = keys[0];
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


template <TYPES(typename)>
auto IndexPage<TYPES()>::pop_back() -> std::pair<FieldType, std::streampos> {
    if (this->is_empty()) {
        throw EmptyPage();
    }

    --num_keys;
    return std::make_pair(keys[num_keys], children[num_keys + 1]);
}


template <TYPES(typename)>
auto IndexPage<TYPES()>::reallocate_references_after_split(std::int32_t child_pos, FieldType& new_key, std::streampos new_page_seek) -> void {
    for (int i = len(); i > child_pos; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
    }

    keys[child_pos] = new_key;
    children[child_pos + 1] = new_page_seek;
    ++num_keys;
}


template<TYPES(typename)>
auto IndexPage<TYPES()>::reallocate_references_after_merge(std::int32_t merged_child_pos) -> void {
    for (std::int32_t i = merged_child_pos; i < len() - 1; ++i) {
        keys[i] = keys[i + 1];
        children[i + 1] = children[i + 2];
    }
    --num_keys;
}


template<TYPES(typename)>
auto IndexPage<TYPES()>::merge(IndexPage<TYPES()> &right_sibling, FieldType& new_key) -> void {
    push_back(new_key, right_sibling.children[0]);
    for (std::int32_t i = 0; i < right_sibling.len(); ++i) {
        push_back(right_sibling.keys[i], right_sibling.children[i + 1]);
    }
}


template <typename FieldType>
auto get_expected_index_page_capacity() -> std::int32_t {
    return std::floor(
            static_cast<double>(get_buffer_size() - sizeof(std::int32_t) - sizeof(std::int64_t) - sizeof(bool))  /
            (sizeof(std::int64_t) + sizeof(FieldType))
    );
}
