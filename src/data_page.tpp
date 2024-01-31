//
// Created by juan diego on 9/15/23.
//

#include "data_page.hpp"


template<DEFINE_INDEX_TYPE>
DataPage<INDEX_TYPE>::~DataPage() = default;


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::size_of() -> int {
    return 2 * sizeof(std::int32_t) + 2 * sizeof(std::int64_t) + this->capacity * sizeof(RecordType);
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::len() -> std::size_t {
    return this->num_records;
}


template<DEFINE_INDEX_TYPE>
DataPage<INDEX_TYPE>::DataPage(BPlusTree<INDEX_TYPE>* b_plus)
        : Page<INDEX_TYPE>(b_plus->metadata_json[DATA_PAGE_CAPACITY].asInt(), b_plus), num_records(0), next_leaf(emptyPage), prev_leaf(emptyPage),
          records(this->capacity, RecordType()) {
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::write(std::fstream &file) -> void {
    char* buffer = new char[size_of()];
    int offset = 0;
    memcpy(buffer + offset, (char *) &this->capacity, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy(buffer + offset, (char *) &num_records, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy(buffer + offset, (char *) &next_leaf, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    memcpy(buffer + offset, (char *) &prev_leaf, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    for (int i = 0; i < num_records; ++i) {
        memcpy(buffer + offset, (char *) &records[i], sizeof(RecordType));
        offset += sizeof(RecordType);
    }

    file.write(buffer, size_of());
    delete [] buffer;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::read(std::fstream &file) -> void {
    char* buffer = new char[size_of()];
    int offset = 0;
    file.read(buffer, size_of());

    memcpy((char *) &this->capacity, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy((char *) &num_records, buffer + offset, sizeof(std::int32_t));
    offset += sizeof(std::int32_t);

    memcpy((char *) & next_leaf, buffer + offset, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    memcpy((char *) & prev_leaf, buffer + offset, sizeof(std::int64_t));
    offset += sizeof(std::int64_t);

    for (int i = 0; i < num_records; ++i) {
        memcpy((char *) & records[i], buffer + offset, sizeof(RecordType));
        offset += sizeof(RecordType);
    }

    delete [] buffer;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::split(std::int32_t split_pos) -> SplitResult<INDEX_TYPE> {
    auto new_data_page = std::make_shared<DataPage<INDEX_TYPE>>(this->tree);

    for (int i = split_pos; i < num_records; ++i) {
        new_data_page->push_back(records[i]);
    }

    num_records -= new_data_page->num_records;
    return SplitResult<INDEX_TYPE> { new_data_page, this->tree->get_indexed_field(records[num_records - 1]) };
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::balance(std::streampos seek_parent, IndexPage<INDEX_TYPE>& parent, std::int32_t child_pos) -> void {
    std::int32_t minimum = this->tree->metadata_json[MINIMUM_DATA_PAGE_RECORDS].asInt();
    if (num_records >= minimum) {
        return;
    }

    DataPage<INDEX_TYPE> left_sibling(this->tree);
    DataPage<INDEX_TYPE> right_sibling(this->tree);

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        seek(this->tree->b_plus_index_file, seek_left_sibling);
        left_sibling.read(this->tree->b_plus_index_file);

        if (left_sibling.num_records > minimum) {
            // left rotation
            RecordType to_borrow = left_sibling.pop_back();
            this->push_front(to_borrow);
            RecordType left_max_record = left_sibling.max_record();
            KeyType new_key = this->tree->get_indexed_field(left_max_record);
            parent.keys[child_pos - 1] = new_key;

            // save changes
            seek(this->tree->b_plus_index_file, seek_left_sibling);
            left_sibling.write(this->tree->b_plus_index_file);
            seek(this->tree->b_plus_index_file, parent.children[child_pos]);
            this->write(this->tree->b_plus_index_file);
            seek(this->tree->b_plus_index_file, seek_parent);
            parent.write(this->tree->b_plus_index_file);
            return;
        }
    } else {
        std::streampos seek_right_sibling = parent.children[child_pos + 1];
        seek(this->tree->b_plus_index_file, seek_right_sibling);
        right_sibling.read(this->tree->b_plus_index_file);

        if (right_sibling.num_records > minimum) {
            // right rotation
            RecordType to_borrow = right_sibling.pop_front();
            this->push_back(to_borrow);
            KeyType new_key = this->tree->get_indexed_field(to_borrow);
            parent.keys[child_pos] = new_key;

            // save changes
            seek(this->tree->b_plus_index_file, seek_right_sibling);
            right_sibling.write(this->tree->b_plus_index_file);
            seek(this->tree->b_plus_index_file, parent.children[child_pos]);
            this->write(this->tree->b_plus_index_file);
            seek(this->tree->b_plus_index_file, seek_parent);
            parent.write(this->tree->b_plus_index_file);
            return;
        }
    }

    if (child_pos > 0) {
        std::streampos seek_left_sibling = parent.children[child_pos - 1];
        std::shared_ptr<DataPage<INDEX_TYPE>> new_page = left_sibling.merge(*this);
        parent.reallocate_references_after_merge(child_pos);

        seek(this->tree->b_plus_index_file, seek_left_sibling);
        new_page->write(this->tree->b_plus_index_file);
        seek(this->tree->b_plus_index_file, seek_parent);
        parent.write(this->tree->b_plus_index_file);
    } else {
        std::shared_ptr<DataPage<INDEX_TYPE>> new_page = this->merge(right_sibling);
        parent.reallocate_references_after_merge(child_pos);

        seek(this->tree->b_plus_index_file, parent.children[child_pos]);
        new_page->write(this->tree->b_plus_index_file);
        seek(this->tree->b_plus_index_file, seek_parent);
        parent.write(this->tree->b_plus_index_file);
    }
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::deallocate_root() -> void {
    if (len() == 0) {
        this->tree->metadata_json[SEEK_ROOT] = emptyPage;
        this->tree->metadata_json[ROOT_STATUS] = emptyPage;
    }
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::push_front(RecordType &record) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    for (int i = num_records - 1; i >= 0; --i) {
        records[i + 1] = records[i];
    }

    records[0] = record;
    num_records++;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::push_back(RecordType &record) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    records[(num_records++)] = record;
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::pop_back() -> RecordType {
    if (num_records == 0) {
        throw EmptyPage();
    }

    return records[--num_records];
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::pop_front() -> RecordType {
    if (num_records == 0) {
        throw EmptyPage();
    }

    RecordType record = records[0];

    for (int i = 0; i < num_records - 1; ++i) {
        records[i] = records[i + 1];
    }

    --num_records;
    return record;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::max_record() -> RecordType  {
    if (num_records == 0) {
        throw EmptyPage();
    }

    return records[num_records - 1];
}


template<typename KeyType, typename RecordType, typename Greater, typename Index>
auto DataPage<KeyType, RecordType, Greater, Index>::min_record() -> RecordType {
    if (num_records == 0) {
        throw EmptyPage();
    }

    return records[0];
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::sorted_insert(RecordType &record) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    KeyType key = this->tree->get_indexed_field(record);
    int record_pos = num_records;
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

    while (i < num_records && this->tree->gt(key, this->tree->get_indexed_field(records[i]))) {
        ++i;
    }

    if (i == num_records || this->tree->gt(this->tree->get_indexed_field(records[i]), key)) {
        throw KeyNotFound();
    }

    while (i < num_records - 1) {
        records[i] = records[i + 1];
        i++;
    }

    num_records--;

    std::shared_ptr<KeyType> predecessor = nullptr;

    if (num_records > 0) {
        predecessor = std::make_shared<KeyType>(this->tree->get_indexed_field(records[len() - 1]));
    }

    if (prev_leaf != emptyPage) {
        DataPage<INDEX_TYPE> prev_data_page(this->tree);
        seek(this->tree->b_plus_index_file, prev_leaf);
        prev_data_page.read(this->tree->b_plus_index_file);
        predecessor = std::make_shared<KeyType>(this->tree->get_indexed_field(prev_data_page.records[prev_data_page.len() - 1]));
    }

    return predecessor;
}


template<DEFINE_INDEX_TYPE>
auto DataPage<INDEX_TYPE>::merge(DataPage<INDEX_TYPE> &right_sibling) -> std::shared_ptr<DataPage<INDEX_TYPE>> {
    DataPage<INDEX_TYPE> new_page(this->tree);

    for (std::int32_t i = 0; i < len(); ++i) {
        new_page.push_back(records[i]);
    }

    for (std::int32_t i = 0; i < right_sibling.len(); ++i) {
        new_page.push_back(right_sibling.records[i]);
    }

    return std::make_shared<DataPage<INDEX_TYPE>>(new_page);
}


template <typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t{
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int64_t) - 2 * sizeof(std::int32_t)) /
            (sizeof(RecordType))
    );
}
