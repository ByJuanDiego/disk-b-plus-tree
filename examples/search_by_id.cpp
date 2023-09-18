//
// Created by juan diego on 9/17/23.
//

#include <iostream>
#include "bplustree.hpp"
#include "record.hpp"

auto main() -> int {
    int32 const index_page_capacity = IndexPage<int32>::get_expected_capacity();
    int32 const data_page_capacity = DataPage<Record>::get_expected_capacity();
    bool const unique = true;

    Property const property(
            "./index/record/",
            "index_by_id_metadata.json",
            "index_by_id.dat",
            index_page_capacity,
            data_page_capacity,
            unique
    );

    std::function<int32(Record&)> const get_indexed_field = [](Record& record) {
        return record.id;
    };

    BPlusTree<int32, Record> tree(property, get_indexed_field);

    for (int i = 1; i <= 100'000; ++i) {
        const std::string name_ = "user " + std::to_string(i);
        const char * name = name_.c_str();
        const int age = i % 30;
        Record record(i, name, age);
        tree.insert(record);
    }

    const int lower_bound = 500;
    const int upper_bound = 800;

    std::vector<Record> const recovered = tree.between(lower_bound, upper_bound);
    for (const Record& record: recovered) {
        std::cout << record << '\n';
    }
}
