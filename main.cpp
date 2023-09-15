#include "bplustree.hpp"
#include <iostream>


struct Record {
    int32 id;
    int32 age;
    char name [11];

    friend auto operator << (std::ostream& ostream, const Record& record) -> std::ostream& {
        ostream << "(" << record.id << ", " << record.age << ", " << record.name << ")";
        return ostream;
    }
};

auto main() -> int {
    Property const property(
            "./b_plus/",
            "metadata.json",
            "index.dat",
            IndexPage<int32>::get_expected_capacity(),
            DataPage<Record>::get_expected_capacity(),
            true
            );

    std::function<int32(Record&)> const get_indexed_field = [](Record& record) {
        return record.id;
    };

    BPlusTree<int32, Record> tree(property, get_indexed_field);
    int lower = 0;
    int upper = 2;
    std::vector<Record> const recovered = tree.between(lower, upper);

    for (const Record& record: recovered) {
        std::cout << record << '\n';
    }

    return EXIT_SUCCESS;
}
