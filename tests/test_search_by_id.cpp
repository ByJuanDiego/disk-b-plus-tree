//
// Created by juan diego on 9/17/23.
//

#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>


#include "bplustree.hpp"
#include "record.hpp"


void search_test(BPlusTree<std::int32_t, Record> &tree, const int number_of_records) {
    for (std::int32_t i = 1; i <= number_of_records; ++i) {
        std::vector<Record> const recovered = tree.search(i);
        int const EXPECTED_SIZE = 1;
        std::size_t const SEARCH_SIZE = recovered.size();
        assert(EXPECTED_SIZE == SEARCH_SIZE);
    }
}


void search_between_test(BPlusTree<std::int32_t, Record> &tree, const int number_of_records) {
    for (std::int32_t j = 1; j <= number_of_records; ++j) {
        for (std::int32_t k = j; k <= number_of_records; ++k) {
            std::vector<Record> const recovered = tree.between(j, k);
            int const EXPECTED_SIZE = k - j + 1;
            std::size_t const SEARCH_SIZE = recovered.size();

            assert(EXPECTED_SIZE == SEARCH_SIZE);
        }
    }
}


void search_above_test(BPlusTree<std::int32_t, Record> &tree, const int number_of_records) {
    for (std::int32_t i = 1; i < number_of_records; ++i) {
        std::vector<Record> recovered = tree.above(i);
        int const EXPECTED_SIZE = number_of_records - i + 1;
        std::size_t const SEARCH_SIZE = recovered.size();

        assert(EXPECTED_SIZE == SEARCH_SIZE);
    }
}


void search_below_test(BPlusTree<std::int32_t, Record> &tree, const int number_of_records) {
    for (std::int32_t i = number_of_records; i >= 1; --i) {
        std::vector<Record> recovered = tree.below(i);
        int const EXPECTED_SIZE = i;
        std::size_t const SEARCH_SIZE = recovered.size();

        assert(EXPECTED_SIZE == SEARCH_SIZE);
    }
}


auto main(int argc, char *argv[]) -> int {
    if (argc < 3) {
        return EXIT_FAILURE;
    }

    int const NUMBER_OF_TESTS = atoi(argv[1]);
    int const NUMBER_OF_RECORDS = atoi(argv[2]);

    std::int32_t const index_page_capacity = get_expected_index_page_capacity<std::int32_t>();
    std::int32_t const data_page_capacity = get_expected_data_page_capacity<Record>();
    bool const unique = true;
    std::string const path = "./index/record/";

    std::function<std::int32_t(Record &)> const get_indexed_field = [](Record &record) {
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
        search_above_test(btree, NUMBER_OF_RECORDS);
        search_below_test(btree, NUMBER_OF_RECORDS);
        search_between_test(btree, NUMBER_OF_RECORDS);
        std::cout << "Passed tests for index #" << TEST << "\n";
    }

    return EXIT_SUCCESS;
}
