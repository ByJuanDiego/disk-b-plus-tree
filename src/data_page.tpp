//
// Created by juan diego on 9/15/23.
//

#include "data_page.hpp"


template<typename RecordType>
DataPage<RecordType>::~DataPage() = default;


template<typename RecordType>
auto DataPage<RecordType>::size_of() -> int {
    return 2 * sizeof(int32) + 2 * sizeof(int64) + capacity * sizeof(RecordType);
}


template<typename RecordType>
DataPage<RecordType>::DataPage(DataPage &&other) noexcept
        : capacity(std::move(other.capacity)),
          num_records(other.num_records),
          next_leaf(other.next_leaf),
          prev_leaf(other.prev_leaf),
          records(capacity, RecordType()){
    for (int i = 0; i < num_records; ++i) {
        records[i] = other.records[i];
    }
}


template<typename RecordType>
DataPage<RecordType>::DataPage(const DataPage &other)
        : capacity(std::move(other.capacity)),
          num_records(std::move(other.num_records)),
          next_leaf(std::move(other.next_leaf)),
          prev_leaf(std::move((other.prev_leaf))),
          records(capacity, RecordType()) {
    for (int i = 0; i < num_records; ++i){
        records[i] = other.records[i];
    }
}


template<typename RecordType>
DataPage<RecordType>::DataPage(int32 records_capacity)
        : capacity(records_capacity), num_records(0), next_leaf(emptyPage), prev_leaf(emptyPage), records(capacity, RecordType()) {
}


template<typename RecordType>
auto DataPage<RecordType>::get_expected_capacity() -> int32 {
    return std::floor(
            static_cast<double>(get_buffer_size() - 2 * sizeof(int64) - 2 * sizeof(int32)) /
            (sizeof(RecordType))
    );
}


template<typename RecordType>
void DataPage<RecordType>::write(std::fstream &file) {
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


template<typename RecordType>
void DataPage<RecordType>::read(std::fstream &file) {
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


template<typename RecordType>
void DataPage<RecordType>::push_front(RecordType &record) {
    if (num_records == capacity) {
        throw FullPage();
    }

    for (int i = num_records - 1; i >= 0; --i) {
        records[i + 1] = records[i];
    }

    records[0] = record;
    num_records++;
}


template<typename RecordType>
void DataPage<RecordType>::push_back(RecordType &record) {
    if (num_records == capacity) {
        throw FullPage();
    }

    records[(num_records++)] = record;
}


template<typename RecordType>
auto DataPage<RecordType>::max_record() -> RecordType  {
    if (num_records < 1) {
        throw EmptyPage();
    }

    return records[num_records - 1];
}


template<typename RecordType>
template<typename KeyType, typename Greater, typename Index>
void DataPage<RecordType>::sorted_insert(RecordType &record, Greater greater_to, Index get_indexed_field) {

    KeyType key = get_indexed_field(record);
    int record_pos = num_records;
    while (record_pos >= 1 && greater_to(get_indexed_field(records[record_pos]), key)) {
        records[record_pos] = records[record_pos - 1];
        --record_pos;
    }
    records[record_pos] = record;
    ++num_records;
}


template<typename RecordType>
auto DataPage<RecordType>::split(int32 min_data_page_records) {
    DataPage<RecordType> new_data_page(this->capacity);
    int32 new_data_page_num_records = std::floor(this->capacity / 2.0);

    for (int i = 0; i < new_data_page_num_records; ++i) {
        new_data_page.push_back(this->records[i + min_data_page_records + 1]);
    }

    this->num_records = min_data_page_records + 1;
    return new_data_page;
}
