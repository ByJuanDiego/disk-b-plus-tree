#include <cassert>
#include <iostream>
#include <algorithm>
#include <random>


#include "bplustree.hpp"
#include "record.hpp"


auto generate_random_vector(const int number_of_records) -> std::vector<std::int32_t> {
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


void test_remove(BPlusTree<std::int32_t, Record> &tree, const int number_of_records, const int n_remove) {
    std::vector<std::int32_t> keys = generate_random_vector(number_of_records);

    for (int i = 0; i < n_remove; ++i) {
        std::cout << "i: " << i << "\n";
        std::int32_t key_to_remove = keys[i];
        tree.remove(key_to_remove);
        std::vector<Record> recovered = tree.between(1, 10000);
        int const EXPECTED_SIZE = number_of_records - i - 1;
        std::size_t const SEARCH_SIZE = recovered.size();

        assert(EXPECTED_SIZE == SEARCH_SIZE);
    }
}


int main(int argc, char *argv[]) {
    int const N_REMOVE = 9'800;
    int const NUMBER_OF_RECORDS = 10'000;

    std::int32_t const index_page_capacity = get_expected_index_page_capacity<std::int32_t>();
    std::int32_t const data_page_capacity = get_expected_data_page_capacity<Record>();
    bool const unique = true;
    std::string const path = "./index/record/";

    std::function<std::int32_t(Record &)> const get_indexed_field = [](Record &record) {
        return record.id;
    };

    std::string const metadata_file_name = "metadata_index_by_id_1.json";
    std::string const index_file_name = "index_by_id_1.dat";

    Property const property(
            path,
            metadata_file_name,
            index_file_name,
            index_page_capacity,
            data_page_capacity,
            unique
    );

    BPlusTree<std::int32_t, Record> btree(property, get_indexed_field);
    test_remove(btree, NUMBER_OF_RECORDS, N_REMOVE);
    std::cout << "Remove test passed" << std::endl;
    btree.display();

    return EXIT_SUCCESS;
}
