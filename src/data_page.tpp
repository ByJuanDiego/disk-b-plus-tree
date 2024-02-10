//
// Created by juan diego on 9/15/23.
//

#include "data_page.hpp"


template<DEFINE_INDEX_TYPE>
DataPage<INDEX_TYPE>::~DataPage() = default;


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::bytes_len() -> int {
    return sizeof(std::int32_t) + 2 * sizeof(std::int64_t) + max_capacity() * sizeof(RecordType);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::len() -> std::size_t {
    return this->num_records;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::max_capacity() -> std::size_t {
    return this->tree->properties.MAX_DATA_PAGE_CAPACITY;
}


template<DEFINE_INDEX_TYPE>
DataPage<INDEX_TYPE>::DataPage(BPlusTree<INDEX_TYPE>* tree)
        : Page<INDEX_TYPE>(tree), num_records(0), next_leaf(emptyPage), prev_leaf(emptyPage) {
    records.resize(max_capacity(), RecordType());
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::write() -> void {
    char* buffer = new char[bytes_len()];
    int offset = 0;

    memcpy(buffer + offset, (char *) &num_records, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy(buffer + offset, (char *) &next_leaf, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    memcpy(buffer + offset, (char *) &prev_leaf, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    for (int i = 0; i < len(); ++i) {
        memcpy(buffer + offset, (char *) &records[i], sizeof(RecordType));
        offset += sizeof(RecordType);
    }

    this->tree->b_plus_index_file.write(buffer, bytes_len());
    delete [] buffer;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::read() -> void {
    char* buffer = new char[bytes_len()];
    int offset = 0;
    this->tree->b_plus_index_file.read(buffer, bytes_len());

    memcpy((char *) &num_records, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy((char *) & next_leaf, buffer + offset, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    memcpy((char *) & prev_leaf, buffer + offset, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    for (int i = 0; i < len(); ++i) {
        memcpy((char *) & records[i], buffer + offset, sizeof(RecordType));
        offset += sizeof(RecordType);
    }

    delete [] buffer;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::split(std::int32_t split_pos) -> SplitResult<INDEX_TYPE> {
    auto new_data_page = std::make_shared<DataPage<INDEX_TYPE>>(this->tree);

    for (int i = split_pos; i < len(); ++i) {
        new_data_page->push_back(records[i]);
    }

    num_records -= new_data_page->len();
    return SplitResult<INDEX_TYPE> { new_data_page, this->tree->get_indexed_field(records[len() - 1]) };
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::balance_page_insert(std::streampos seek_parent,
                                               IndexPage<KeyType, RecordType, Greater, Index> &parent,
                                               std::int32_t child_pos) -> void {
    std::streampos child_seek = parent.children[child_pos];
    // Create a new data page to accommodate the split
    SplitResult<INDEX_TYPE> split = this->split(this->tree->properties.SPLIT_POS_DATA_PAGE);
    auto new_page = std::dynamic_pointer_cast<DataPage<INDEX_TYPE>>(split.new_page);

    // Seek to the end of the B+Tree index file to append the new page
    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();

    // Set the previous leaf pointer of the new page
    new_page->prev_leaf = child_seek;

    std::streampos next_leaf_seek = this->next_leaf;
    if (next_leaf_seek != emptyPage) {
        new_page->next_leaf = next_leaf_seek;

        DataPage<INDEX_TYPE> next_page(this->tree);
        next_page.load(next_leaf_seek);
        next_page.prev_leaf = new_page_seek;
        next_page.save(next_leaf_seek);
    }

    // Write the new data page to the B+Tree index file
    new_page->save(new_page_seek);

    // Update the next leaf pointer of the old page to point to the new page
    this->next_leaf = new_page_seek;

    // Seek to the location of the old page in the index file and update it
    this->save(child_seek);

    // Update references to data pages in the parent index page
    parent.reallocate_references_after_split(child_pos, split.split_key, new_page_seek);

    // Seek to the location of the parent index page in the index file and update it
    parent.save(seek_parent);
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::balance_page_remove(std::streampos seek_parent, IndexPage<INDEX_TYPE>& parent, std::int32_t child_pos) -> void {
    std::int32_t minimum = this->tree->properties.MIN_DATA_PAGE_CAPACITY;
    if (len() >= minimum) {
        return;
    }

    DataPage<INDEX_TYPE> left_sibling(this->tree);
    DataPage<INDEX_TYPE> right_sibling(this->tree);

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        left_sibling.load(seek_left_sibling);

        if (left_sibling.len() > minimum) {
            // left-borrow
            RecordType to_borrow = left_sibling.pop_back();
            this->push_front(to_borrow);
            RecordType left_max_record = left_sibling.max_record();
            KeyType new_key = this->tree->get_indexed_field(left_max_record);
            parent.keys[child_pos - 1] = new_key;

            // save changes
            left_sibling.save(seek_left_sibling);
            this->save(parent.children[child_pos]);
            parent.save(seek_parent);
            return;
        }
    } else {
        std::streampos seek_right_sibling = parent.children[1];
        right_sibling.load(seek_right_sibling);

        if (right_sibling.len() > minimum) {
            // right-borrow
            RecordType to_borrow = right_sibling.pop_front();
            this->push_back(to_borrow);
            KeyType new_key = this->tree->get_indexed_field(to_borrow);
            parent.keys[0] = new_key;

            // save changes
            right_sibling.save(seek_right_sibling);
            this->save(parent.children[0]);
            parent.save(seek_parent);
            return;
        }
    }

    if (child_pos > 0) {
        // left-merge
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        left_sibling.merge(*this);
        left_sibling.next_leaf = this->next_leaf;

        if (child_pos < parent.len()) {
            DataPage<INDEX_TYPE> other_sibling(this->tree);
            other_sibling.load(parent.children[child_pos + 1]);
            other_sibling.prev_leaf = seek_left_sibling;
            other_sibling.save(parent.children[child_pos + 1]);
        }

        parent.reallocate_references_after_merge(child_pos - 1);

        // save changes
        left_sibling.save(seek_left_sibling);
        parent.save(seek_parent);
    } else {
        // right-merge
        this->merge(right_sibling);
        this->next_leaf = right_sibling.next_leaf;

        if (parent.len() >= 2) {
            DataPage<INDEX_TYPE> other_sibling(this->tree);
            other_sibling.load(parent.children[2]);
            other_sibling.prev_leaf = parent.children[0];
            other_sibling.save(parent.children[2]);
        }

        parent.reallocate_references_after_merge(child_pos);

        // save changes
        this->save(parent.children[child_pos]);
        parent.save(seek_parent);
    }
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::balance_root_insert(std::streampos old_root_seek) -> void {
    SplitResult<INDEX_TYPE> split = this->split(this->tree->properties.SPLIT_POS_DATA_PAGE);
    auto new_page = std::dynamic_pointer_cast<DataPage<INDEX_TYPE>>(split.new_page);

    new_page->prev_leaf = old_root_seek;
    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_page_seek = this->tree->b_plus_index_file.tellp();
    new_page->write();

    this->next_leaf = new_page_seek;
    this->save(old_root_seek);

    IndexPage<INDEX_TYPE> new_root(this->tree);
    new_root.keys[0] = split.split_key;
    new_root.children[0] = old_root_seek;
    new_root.children[1] = new_page_seek;
    new_root.num_keys = 1;

    seek(this->tree->b_plus_index_file, 0, std::ios::end);
    std::streampos new_root_seek = this->tree->b_plus_index_file.tellp();
    new_root.write();

    this->tree->properties.SEEK_ROOT = new_root_seek;
    this->tree->properties.ROOT_STATUS = indexPage;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::balance_root_remove() -> void {
    if (this->is_empty()) {
        this->tree->properties.SEEK_ROOT = emptyPage;
        this->tree->properties.ROOT_STATUS = emptyPage;
    }
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::push_front(RecordType &record) -> void {
    if (this->is_full()) {
        throw FullPage();
    }

    for (int i = len() - 1; i >= 0; --i) {
        records[i + 1] = records[i];
    }

    records[0] = record;
    num_records++;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::push_back(RecordType &record) -> void {
    if (this->is_full()) {
        throw FullPage();
    }

    records[num_records++] = record;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::pop_back() -> RecordType {
    if (this->is_empty()) {
        throw EmptyPage();
    }

    return records[--num_records];
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::pop_front() -> RecordType {
    if (this->is_empty()) {
        throw EmptyPage();
    }

    RecordType record = records[0];

    for (int i = 0; i < len() - 1; ++i) {
        records[i] = records[i + 1];
    }

    --num_records;
    return record;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::max_record() -> RecordType  {
    if (this->is_empty()) {
        throw EmptyPage();
    }

    return records[num_records - 1];
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::sorted_insert(RecordType &record) -> void {
    if (this->is_full()) {
        throw FullPage();
    }

    KeyType key = this->tree->get_indexed_field(record);
    int record_pos = len();
    while (record_pos >= 1 && this->tree->gt(this->tree->get_indexed_field(records[record_pos - 1]), key)) {
        records[record_pos] = records[record_pos - 1];
        --record_pos;
    }

    records[record_pos] = record;
    ++num_records;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::remove(KeyType key) -> std::shared_ptr<KeyType> {
    std::int32_t i = 0;

    while (i < len() && this->tree->gt(key, this->tree->get_indexed_field(records[i]))) {
        ++i;
    }

    if (i == len() || this->tree->gt(this->tree->get_indexed_field(records[i]), key)) {
        throw KeyNotFound();
    }

    while (i < len() - 1) {
        records[i] = records[i + 1];
        i++;
    }

    num_records--;

    if (len() > 0) {
        return std::make_shared<KeyType>(this->tree->get_indexed_field(records[len() - 1]));
    }

    if (prev_leaf != emptyPage) {
        DataPage<INDEX_TYPE> prev_data_page(this->tree);
        prev_data_page.load(prev_leaf);
        return std::make_shared<KeyType>(this->tree->get_indexed_field(prev_data_page.records[prev_data_page.len() - 1]));
    }

    return nullptr;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::merge(DataPage<INDEX_TYPE> &right_sibling) -> void {
    for (std::int32_t i = 0; i < right_sibling.len(); ++i) {
        push_back(right_sibling.records[i]);
    }
}


template <typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t{
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int64_t) - sizeof(std::int32_t)) /
            (sizeof(RecordType))
    );
}
