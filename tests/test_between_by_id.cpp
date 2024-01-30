//
// Created by juan diego on 9/17/23.
//

#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>


#include "bplustree.hpp"
#include "record.hpp"


void search_test(BPlusTree<std::int32_t, Record>& tree, const int number_of_records, double const thresh_hold = 0.45) {
    int const min = std::ceil(number_of_records * thresh_hold);
    int const max = static_cast<int>(number_of_records - min);

    for (std::int32_t j = min; j <= max; ++j) {
        for (std::int32_t k = j; k <= max; ++k) {
            std::vector<Record> const recovered = tree.between(j, k);
            int const EXPECTED_SIZE = k - j + 1;
            std::size_t const SEARCH_SIZE = recovered.size();

            assert(EXPECTED_SIZE == SEARCH_SIZE);
        }
    }
}


auto main(int argc, char* argv[]) -> int {
    if (argc < 3) {
        return EXIT_FAILURE;
    }

    int const NUMBER_OF_TESTS = atoi(argv[1]);
    int const NUMBER_OF_RECORDS = atoi(argv[2]);

    std::int32_t const index_page_capacity = get_expected_index_page_capacity<std::int32_t>();
    std::int32_t const data_page_capacity = get_expected_data_page_capacity<Record>();
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
        search_test(btree, NUMBER_OF_RECORDS);
        std::cout << "Passed tests for index #" << TEST << "\n";
    }

    return EXIT_SUCCESS;
}
