//
// Created by juan diego on 9/22/23.
//

#include <iostream>

#include "bplustree.hpp"
#include "record.hpp"
#include "time_utils.hpp"


auto main(int argc, char* argv[]) -> int {
    if (argc < 2) {
        return EXIT_FAILURE;
    }

    const std::string& dataset_file_name = argv[1];
    const std::string& directory_path = "./index/index_by_id/";
    const std::string& metadata_file_name = "metadata";
    const std::string& index_file_name = "btree";

    const int index_page_capacity = get_expected_index_page_capacity<std::int32_t>();
    const int data_page_capacity = get_expected_data_page_capacity<Record>();

    const bool unique_key = true;

    const Property props(
            directory_path,
            metadata_file_name,
            index_file_name,
            index_page_capacity,
            data_page_capacity,
            unique_key
    );

    const std::function<std::int32_t(Record&)> index_by_id = [](Record& record) -> std::int32_t {
        return record.id;
    };

    BPlusTree<std::int32_t, Record> btree(props, index_by_id);

    std::fstream file(dataset_file_name, std::ios::in);
    std::int32_t record_age {};
    std::int32_t record_id {};
    std::string record_name;

    const Clock clock;
    clock([&]() {
        while (file >> record_id >> record_name >> record_age) {
            Record record(record_id, record_name, record_age);
            btree.insert(record);
        }
    }, std::cout);

    file.close();
    return EXIT_SUCCESS;
}
