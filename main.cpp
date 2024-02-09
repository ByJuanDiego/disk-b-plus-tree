#include <iostream>
#include "bplustree.hpp"
#include "record.hpp"

void displayMenu() {
    std::cout << "\n\n*****************************************" << std::endl;
    std::cout << "             B+ TREE MENU                " << std::endl;
    std::cout << "*****************************************" << std::endl;
    std::cout << "[1] Insert" << std::endl;
    std::cout << "[2] Key Search" << std::endl;
    std::cout << "[3] Range Search" << std::endl;
    std::cout << "[4] Above-Key Search" << std::endl;
    std::cout << "[5] Below-Key Search" << std::endl;
    std::cout << "[6] Remove" << std::endl;
    std::cout << "[0] Exit" << std::endl;
    std::cout << "*****************************************" << std::endl;
    std::cout << "Enter your choice: ";
}


auto main() -> int {
    int choice;

    const std::string& directory_path = "./index/test_by_id/";
    const std::string& metadata_file_name = "metadata";
    const std::string& index_file_name = "btree";

    const int index_page_capacity = 4;
    const int data_page_capacity = 4;
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

    BPlusTree<std::int32_t, Record> bPlusTree(props, index_by_id);

    do {
        displayMenu();
        std::cin >> choice;

        switch (choice) {
            case 1: {
                Record record;
                std::cout << "Enter record to insert: ";
                std::cin >> record;
                std::cout << "Read record: " << record;
                bPlusTree.insert(record);
                break;
            }
            case 2: {
                int key;
                std::cout << "Enter key to search: ";
                std::cin >> key;
                std::vector<Record> recovered = bPlusTree.search(key);
                for (Record& record: recovered) {
                    std::cout << record << "\n";
                }
                break;
            }
            case 3: {
                int l, u;
                std::cout << "Enter lower bound: ";
                std::cin >> l;
                std::cout << "Enter upper bound: ";
                std::cin >> u;
                std::vector<Record> recovered = bPlusTree.between(l, u);
                std::cout << "Recovered records:\n";
                for (Record& record: recovered) {
                    std::cout << record << "\n";
                }
                break;
            }
            case 4: {
                int key;
                std::cout << "Enter lower bound: ";
                std::cin >> key;
                std::vector<Record> recovered = bPlusTree.above(key);
                for (Record& record: recovered) {
                    std::cout << record << "\n";
                }
                break;
            }
            case 5: {
                int key;
                std::cout << "Enter upper bound: ";
                std::cin >> key;
                std::vector<Record> recovered = bPlusTree.below(key);
                for (Record& record: recovered) {
                    std::cout << record << "\n";
                }
                break;
            }
            case 6: {
                int key;
                std::cout << "Enter key to remove: ";
                std::cin >> key;
                bPlusTree.remove(key);
                break;
            }
            case 0: {
                std::cout << "Exiting the program." << std::endl;
                break;
            }
            default: {
                std::cout << "Invalid choice. Please enter a valid option." << std::endl;
                break;
            }
        }
    } while (choice != 0);

    return 0;
}
