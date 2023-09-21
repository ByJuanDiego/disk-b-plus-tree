//
// Created by juan diego on 9/17/23.
//

#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>
#include "bplustree.hpp"
#include "record.hpp"


void search_test(BPlusTree<int32, Record>& tree, const int number_of_records, double const thresh_hold = 0.4) {
    int const min = std::ceil(number_of_records * thresh_hold);
    int const max = static_cast<int>(number_of_records - std::floor(number_of_records * thresh_hold));

    for (int32 j = min; j <= max; ++j) {
        for (int32 k = j; k <= max; ++k) {
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
        search_test(tree, NUMBER_OF_RECORDS);
        std::cout << "\npassed test #" << TEST << "\n";
    }

    return EXIT_SUCCESS;
}
