//
// Created by juan diego on 9/23/23.
//

#include <iostream>
#include <random>

#include "bplustree.hpp"
#include "record.hpp"
#include "time_utils.hpp"


auto generate_random_vector(int32_t number_of_records) -> std::vector<int32_t> {
    std::vector<int32_t> arr(number_of_records);

    // Fill the array with numbers from 1 to N
    for (int32_t i = 0; i < number_of_records; ++i) {
        arr[i] = i + 1;
    }

    // Shuffle the array to make it unordered
    std::random_device random_device;
    std::mt19937 twister(random_device());
    std::shuffle(arr.begin(), arr.end(), twister);

    return arr;
}

std::string generate_random_name() {
    std::string name = std::string(' ', 11);
    std::random_device rd;
    std::uniform_int_distribution dis(48, 122);

    for (int i = 0; i < 11; ++i) {
        name[i] = (char) dis(rd);
    }

    return name;
}

int32_t generate_random_age() {
    std::random_device rd;
    std::uniform_int_distribution dis(10, 90);
    return dis(rd);
}

void generate_file(std::ofstream& ofstream, int32_t integer) {
    for (int i = 1; i <= integer; ++i) {
        Record record {i, generate_random_name(), generate_random_age()};
        ofstream << record.id << " " << record.name << " " << record.age << "\n";
    }
}

auto main(int argc, char* argv[]) -> int {
    if (argc < 2) {
        return EXIT_FAILURE;
    }

    const std::string& dataset_file_name = argv[1];
    const std::string& directory_path = "./index/index_by_name/";
    const std::string& metadata_file_name = "metadata.json";
    const std::string& index_file_name = "btree.dat";

    const int index_page_capacity = get_expected_index_page_capacity<char[11]>();
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

    const std::function<const char *(Record&)> index_by_name = [](Record& record) -> const char * {
        return record.name;
    };

    const std::function<bool(const char *, const char*)> greater_than = [](const char* s1, const char * s2) {
        return strcmp(s1, s2) > 0;
    };

    BPlusTree<const char*, Record, std::function<bool(const char *, const char *)>> btree(
            props,
            index_by_name,
            greater_than
    );

    std::fstream file(dataset_file_name, std::ios::in);
    std::int32_t record_age {};
    std::int32_t record_id {};
    std::string record_name;

    const Clock clock;
    clock([&]() {
        while (file >> record_id >> record_name >> record_age) {
            Record record(record_id, record_name, record_age);
            btree.insert(record);
        }
    }, std::cout);

    file.close();
    return EXIT_SUCCESS;
}
