//
// Created by juan diego on 9/15/23.
//

#include "data_page.hpp"


template<typename KeyType, typename RecordType, typename Index>
DataPage<KeyType, RecordType, Index>::~DataPage() = default;


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::size_of() -> int {
    return 2 * sizeof(std::int32_t) + 2 * sizeof(std::int64_t) + this->capacity * sizeof(RecordType);
}


template<typename KeyType, typename RecordType, typename Index>
DataPage<KeyType, RecordType, Index>::DataPage(DataPage &&other) noexcept
        : Page<KeyType>(std::move(other.capacity)),
          num_records(other.num_records),
          next_leaf(other.next_leaf),
          prev_leaf(other.prev_leaf),
          records(this->capacity, RecordType()){
    for (int i = 0; i < num_records; ++i) {
        records[i] = other.records[i];
    }
}


template<typename KeyType, typename RecordType, typename Index>
DataPage<KeyType, RecordType, Index>::DataPage(const DataPage &other)
        : Page<KeyType>(std::move(other.capacity)),
          num_records(std::move(other.num_records)),
          next_leaf(std::move(other.next_leaf)),
          prev_leaf(std::move((other.prev_leaf))),
          records(this->capacity, RecordType()) {
    for (int i = 0; i < num_records; ++i){
        records[i] = other.records[i];
    }
}


template<typename KeyType, typename RecordType, typename Index>
DataPage<KeyType, RecordType, Index>::DataPage(std::int32_t capacity, const Index& index)
        : Page<KeyType>(capacity), num_records(0), next_leaf(emptyPage), prev_leaf(emptyPage),
          records(this->capacity, RecordType()), get_indexed_field(index) {
}


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::write(std::fstream &file) -> void {
    char* buffer = new char[size_of()];
    int offset = 0;
    memcpy(buffer, (char *) &this->capacity, sizeof(std::int32_t));
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


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::read(std::fstream &file) -> void {
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


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::push_front(RecordType &record) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    for (int i = num_records - 1; i >= 0; --i) {
        records[i + 1] = records[i];
    }

    records[0] = record;
    num_records++;
}


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::push_back(RecordType &record) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    records[(num_records++)] = record;
}


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::max_record() -> RecordType  {
    if (num_records < 1) {
        throw EmptyPage();
    }

    return records[num_records - 1];
}


template<typename KeyType, typename RecordType, typename Index>
template<typename Greater>
auto DataPage<KeyType, RecordType, Index>::sorted_insert(RecordType &record, Greater greater_to) -> void {
    if (num_records == this->capacity) {
        throw FullPage();
    }

    KeyType key = get_indexed_field(record);
    int record_pos = num_records;
    while (record_pos >= 1 && greater_to(get_indexed_field(records[record_pos - 1]), key)) {
        records[record_pos] = records[record_pos - 1];
        --record_pos;
    }
    records[record_pos] = record;
    ++num_records;
}


template<typename KeyType, typename RecordType, typename Index>
auto DataPage<KeyType, RecordType, Index>::split(std::int32_t split_position) -> SplitResult<KeyType> {
    auto new_data_page = std::make_shared<DataPage<KeyType, RecordType, Index>>(this->capacity, get_indexed_field);

    for (int i = split_position; i < num_records; ++i) {
        new_data_page->push_back(records[i]);
    }

    num_records -= new_data_page->num_records;
    return SplitResult<KeyType> { new_data_page, get_indexed_field(records[num_records - 1]) };
}


template <typename RecordType>
auto get_expected_data_page_capacity() -> std::int32_t{
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(std::int64_t) - 2 * sizeof(std::int32_t)) /
            (sizeof(RecordType))
    );
}
