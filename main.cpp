#include "bplustree.h"
#include <iostream>

struct Record {
    int32 id;
    int32 age;
    char name [11];
};

int main() {
    std::cout << get_expected_index_page_capacity<int>() << std::endl;
    std::cout << get_expected_data_page_capacity<Record>() << std::endl;

    std::cout << "Hello, World!" << std::endl;
    return 0;
}
