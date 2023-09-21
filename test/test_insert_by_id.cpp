//
// Created by juan diego on 9/21/23.
//


#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>
#include "bplustree.hpp"
#include "record.hpp"


auto generate_random_vector(int32 number_of_records) -> std::vector<int32> {
    std::vector<int32> arr(number_of_records);

    // Fill the array with numbers from 1 to N
    for (int32 i = 0; i < number_of_records; ++i) {
        arr[i] = i + 1;
    }

    // Shuffle the array to make it unordered
    std::random_device random_device;
    std::mt19937 twister(random_device());
    std::shuffle(arr.begin(), arr.end(), twister);

    return arr;
}


void insert_records(BPlusTree<int32, Record>& tree, const std::vector<int32>& keys) {
    for (int32 const key : keys) {
        Record record {key, "u", 0};
        tree.insert(record);
    }
}


void search_test(BPlusTree<int32, Record>& tree, const int number_of_records) {
    int const min = std::ceil(number_of_records * 0.4);
    int const max = static_cast<int>(number_of_records - std::floor(number_of_records * 0.4));

    for (int32 j = min; j <= max; ++j) {
        std::cout << "j: " << j << "\n";
        for (int32 k = j; k <= number_of_records; ++k) {
            std::vector<Record> const recovered = tree.between(j, k);
            int const EXPECTED_SIZE = k - j + 1;
            std::size_t const SEARCH_SIZE = recovered.size();
            assert(EXPECTED_SIZE == SEARCH_SIZE);
        }
    }
}


auto main(int argc, char* argv[]) -> int {
    if (argc < 2) {
        return EXIT_SUCCESS;
    }

    int const NUMBER_OF_TESTS = atoi(argv[1]);
    int const NUMBER_OF_RECORDS = atoi(argv[2]);

    int32 const index_page_capacity = IndexPage<int32>::get_expected_capacity() / 3;
    int32 const data_page_capacity = DataPage<Record>::get_expected_capacity() / 3;
    bool const unique = true;
    std::string const path = "./index/record/";

    std::function<int32(Record&)> const get_indexed_field = [](Record& record) {
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

        BPlusTree<int32, Record> tree(property, get_indexed_field);

        std::vector<int32> const records = generate_random_vector(NUMBER_OF_RECORDS);
        insert_records(tree, records);
        std::cout << "\nIndex #" << TEST << " created successfully\n";
    }

    return EXIT_SUCCESS;
}
