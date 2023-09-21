#include <iostream>
#include <chrono>

#include "bplustree.hpp"
#include "record.hpp"


auto main(int argc, char* argv[]) -> int {
    const std::string& directory_path = "./index/index_by_id/";
    const std::string& metadata_file_name = "metadata.json";
    const std::string& index_file_name = "btree.dat";

    const int32 index_page_capacity = IndexPage<int32>::get_expected_capacity();
    const int32 data_page_capacity = DataPage<Record>::get_expected_capacity();
    const bool unique_key = true;

    const Property props(
            directory_path,
            metadata_file_name,
            index_file_name,
            index_page_capacity,
            data_page_capacity,
            unique_key
    );

    const std::function<int32(Record&)> index_by_id = [](Record& record) -> int32 {
        return record.id;
    };

    BPlusTree<int32, Record> btree(props, index_by_id);

    std::function<void(const std::function<void()>&)> const measure_execution_time = [] (const std::function<void()>& procedure) {
        auto start_time = std::chrono::high_resolution_clock::now();
        procedure();
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> const duration = end_time - start_time;
        std::cout << "Execution time: " << duration.count() << " seconds\n";
    };

    std::vector<Record> recovered;

    int lower_bound = -1;
    int upper_bound = -1;

    std::cout << "Lower Bound: ";
    std::cin >> lower_bound;

    std::cout << "Upper Bound: ";
    std::cin >> upper_bound;

    measure_execution_time([&](){
        recovered = btree.between(lower_bound, upper_bound);
    });

    std::cout << recovered.size() << " rows recovered" << "\n";

//    for (const Record& record: recovered) {
//        std::cout << record << "\n";
//    }

    return 0;
}
