//
// Created by juan diego on 9/23/23.
//

#include <iostream>
#include <random>

#include "bplustree.hpp"
#include "record.hpp"
#include "time_utils.hpp"


auto main(int argc, char* argv[]) -> int {
    // TODO
//    if (argc < 3) {
//        return EXIT_FAILURE;
//    }
//
//    char lower_bound [NAME_LENGTH];
//    char upper_bound [NAME_LENGTH];
//    std::memcpy(lower_bound, argv[1], NAME_LENGTH);
//    std::memcpy(lower_bound, argv[2], NAME_LENGTH);
//
//    std::cout << lower_bound << " " << upper_bound << "\n";
//
//    const std::string& directory_path = "./index/index_by_name/";
//    const std::string& metadata_file_name = "metadata.json";
//    const std::string& index_file_name = "btree.dat";
//
//    const int index_page_capacity = get_expected_index_page_capacity<char[NAME_LENGTH]>();
//    const int data_page_capacity = get_expected_data_page_capacity<Record>();
//
//    const bool unique_key = true;
//
//    const Property props(
//            directory_path,
//            metadata_file_name,
//            index_file_name,
//            index_page_capacity,
//            data_page_capacity,
//            unique_key
//    );
//
//    auto index_by_name = [](Record& record) -> const char * {
//        return static_cast<const char*>(record.name);
//    };
//
//    auto greater_than = [](const char s1[NAME_LENGTH], const char s2[NAME_LENGTH]) -> bool {
//        return strcmp(s1, s2) > 0;
//    };
//
//    BPlusTree<char[11], Record, decltype(greater_than), decltype(index_by_name)> btree(
//            props,
//            index_by_name,
//            greater_than
//    );
//    const Clock clock;
//
//    std::vector<Record> recovered;
//    clock([&]() {
//        recovered = btree.between(lower_bound, upper_bound);
//    }, std::cout);
//
//    std::cout << recovered.size() << " rows recovered" << "\n";
//
//    for (const Record& record: recovered) {
//        std::cout << record << "\n";
//    }

    return EXIT_SUCCESS;
}
