#include "bplustree.hpp"
#include <iostream>


constexpr int NAME_LENGTH = 11;
struct Record {
    int32 id;
    int32 age;
    char name [NAME_LENGTH];

    Record(): id(-1), age(-1), name('\0') {
    }

    explicit Record(int32 _id, const char * _name, int32 _age): id(_id), age(_age), name('\0') {
        strncpy(name, _name, NAME_LENGTH);
        name[NAME_LENGTH - 1] = '\0';
    }

    friend auto operator << (std::ostream& ostream, const Record& record) -> std::ostream& {
        ostream << "(" << record.id << ", " << record.age << ", " << record.name << ")";
        return ostream;
    }
};


auto main() -> int {

    int32 const index_page_capacity = IndexPage<int32>::get_expected_capacity();
    int32 const data_page_capacity = DataPage<Record>::get_expected_capacity();

    Property const property(
            "./b_plus/",
            "metadata.json",
            "index.dat",
            index_page_capacity,
            data_page_capacity,
            true
            );

    std::function<int32(Record&)> const get_indexed_field = [](Record& record) {
        return record.id;
    };

    BPlusTree<int32, Record> tree(property, get_indexed_field);

    for (int i = 1; i <= 100'000; ++i) {
        const std::string name_ = "user " + std::to_string(i);
        const char * name = name_.c_str();
        const int age = i % 30;
        Record record(i, name, age);
        tree.insert(record);
    }

    const int lower = 500;
    const int upper = 800;
    std::vector<Record> const recovered = tree.between(lower, upper);
    for (const Record& record: recovered) {
        std::cout << record.id << '\n';
    }

    return EXIT_SUCCESS;
}
