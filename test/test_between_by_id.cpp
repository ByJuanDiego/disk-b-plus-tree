//
// Created by juan diego on 9/17/23.
//

#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>
#include "bplustree.hpp"
#include "record.hpp"

auto generate_vector(int32 N) -> std::vector<int32> {
    std::vector<int32> arr(N);

    // Fill the array with numbers from 1 to N
    for (int32 i = 0; i < N; ++i) {
        arr[i] = i + 1;
    }

    // Shuffle the array to make it unordered
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(arr.begin(), arr.end(), g);

    return arr;
}


auto main(int argc, char* argv[]) -> int {
    int const NUMBER_OF_TESTS = atoi(argv[1]);
    int const NUMBER_OF_RECORDS = atoi(argv[2]);

    for (int TEST = 1; TEST <= NUMBER_OF_TESTS; ++TEST) {
        int32 const index_page_capacity = IndexPage<int32>::get_expected_capacity();
        int32 const data_page_capacity = DataPage<Record>::get_expected_capacity();
        bool const unique = true;
        std::string const path = "./index/record/";
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

        std::function<int32(Record&)> const get_indexed_field = [](Record& record) {
            return record.id;
        };

        BPlusTree<int32, Record> tree(property, get_indexed_field);

        std::vector<int32> const keys = generate_vector(NUMBER_OF_RECORDS);
        for (int32 const key : keys) {
            Record record {key, "u", 0};
            tree.insert(record);
        }

        for (int32 j = 1; j <= NUMBER_OF_RECORDS; ++j) {
            for (int32 k = j; k <= NUMBER_OF_RECORDS; ++k) {
                std::vector<Record> const recovered = tree.between(j, k);
                int const EXPECTED_SIZE = k - j + 1;
                std::size_t const SEARCH_SIZE = recovered.size();
                assert(EXPECTED_SIZE == SEARCH_SIZE);
            }
        }

        std::cout << "passed test #" << TEST << "\n";
    }

    return EXIT_SUCCESS;
}
