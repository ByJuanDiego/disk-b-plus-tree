//
// Created by juan diego on 9/21/23.
//


#include <iostream>
#include <algorithm>
#include <random>
#include "bplustree.hpp"
#include "record.hpp"


auto generate_random_vector(std::int32_t number_of_records) -> std::vector<std::int32_t> {
    std::vector<std::int32_t> arr(number_of_records);

    // Fill the array with numbers from 1 to N
    for (std::int32_t i = 0; i < number_of_records; ++i) {
        arr[i] = i + 1;
    }

    // Shuffle the array to make it unordered
    std::random_device random_device;
    std::mt19937 twister(random_device());
    std::shuffle(arr.begin(), arr.end(), twister);

    return arr;
}


void insert_records(BPlusTree<std::int32_t, Record>& tree, const std::vector<std::int32_t>& keys) {
    for (std::int32_t const key : keys) {
        Record record {key, "u", 0};
        tree.insert(record);
    }
}


auto main(int argc, char* argv[]) -> int {
    if (argc < 3) {
        return EXIT_FAILURE;
    }

    int const NUMBER_OF_TESTS = atoi(argv[1]);
    int const NUMBER_OF_RECORDS = atoi(argv[2]);

    int const index_page_capacity = get_expected_index_page_capacity<std::int32_t>();
    int const data_page_capacity = get_expected_data_page_capacity<Record>();
    bool const unique = true;
    std::string const path = "./index/record/";

    std::function<std::int32_t(Record&)> const get_indexed_field = [](Record& record) {
        return record.id;
    };

    for (int TEST = 1; TEST <= NUMBER_OF_TESTS; ++TEST) {
        std::string const metadata_file_name = "metadata_index_by_id_" + std::to_string(TEST) + ".json";
        std::string const index_file_name = "index_by_id_" + std::to_string(TEST) + ".dat";

        Property const property(
                path,
                metadata_file_name,
                index_file_name,
                index_page_capacity,
                data_page_capacity,
                unique
        );

        BPlusTree<std::int32_t, Record> btree(property, get_indexed_field);

        std::vector<std::int32_t> const records = generate_random_vector(NUMBER_OF_RECORDS);
        insert_records(btree, records);
        std::cout << "Index #" << TEST << " created successfully\n";
    }

    return EXIT_SUCCESS;
}
