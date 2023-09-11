#include "bplustree.h"

struct Record {
    int32 id;
    int32 age;
    char name [11];
};

int main() {
    Property property(
            "./test/index_by_id/",
            "metadata.json",
            "index.dat",
            get_expected_index_page_capacity<int32>(),
            get_expected_data_page_capacity<Record>(),
            true
            );

    std::function<int32(Record&)> get_indexed_field = [](Record& record) {
        return record.id;
    };

    BPlusTree<int32, Record> tree(property, get_indexed_field);
    return EXIT_SUCCESS;
}
