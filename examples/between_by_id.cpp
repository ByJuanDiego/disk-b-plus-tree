#include <iostream>
#include <chrono>

#include "bplustree.hpp"
#include "record.hpp"
#include "time_utils.hpp"

auto main(int argc, char* argv[]) -> int {
    int lower_bound;
    int upper_bound;

    if (argc < 3) {
        std::cout << "lower bound: ";
        std::cin >> lower_bound;

        std::cout << "upper bound: ";
        std::cin >> upper_bound;
    } else {
        lower_bound = atoi(argv[1]);
        upper_bound = atoi(argv[2]);
    }

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
    const Clock clock;

    std::vector<Record> recovered;
    clock([&]() {
        recovered = btree.between(lower_bound, upper_bound);
        }, std::cout);

    std::cout << recovered.size() << " rows recovered" << "\n";

    for (const Record& record: recovered) {
        std::cout << record << "\n";
    }

    return EXIT_SUCCESS;
}
