//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BPLUSTREE_HPP
#define B_PLUS_TREE_BPLUSTREE_HPP


#include <vector>
#include <functional>
#include <queue>

#include "data_page.hpp"
#include "index_page.hpp"


template <
    typename KeyType,
    typename RecordType,
    typename Greater = std::greater<KeyType>,
    typename Index = std::function<KeyType(RecordType&)>
> class BPlusTree {

    friend struct DataPage<INDEX_TYPE>;
    friend struct IndexPage<INDEX_TYPE>;

private:

    std::fstream metadata_file;
    std::fstream b_plus_index_file;

    Json::Value metadata_json;

    Greater gt;
    Index get_indexed_field;

    auto create_index()                                                         -> void;

    auto load_metadata()                                                        -> void;

    auto save_metadata()                                                        -> void;

    auto locate_data_page(const KeyType& key)                                   -> std::streampos;

    auto insert(std::streampos seek_page, PageType type, RecordType& record)    -> InsertResult;

    auto balance_data_page(IndexPage<INDEX_TYPE>& index_page, std::int32_t child_pos,
                           std::streampos seek_page, std::streampos child_seek) -> void;

    auto balance_index_page(IndexPage<INDEX_TYPE>& index_page, std::int32_t child_pos,
                            std::streampos seek_page, std::streampos child_seek) -> void;

    auto balance_root_data_page()                                                -> void;

    auto balance_root_index_page()                                               -> void;

    auto remove(std::streampos seek_page, PageType type, const KeyType& key)     -> RemoveResult<KeyType>;

public:

    explicit BPlusTree(const Property& property, Index index, Greater greater = Greater());

    auto insert(RecordType& record)                                               -> void;

    auto remove(const KeyType& key)                                               -> void;

    auto search(const KeyType& key)                                               -> std::vector<RecordType>;

    auto between(const KeyType& lower_bound, const KeyType& upper_bound)          -> std::vector<RecordType>;

    auto display()                                                                -> void;
};


template<DEFINE_INDEX_TYPE>
auto BPlusTree<INDEX_TYPE>::display() -> void {
    struct Trunk {
        int level;
        long seek;
        bool is_leaf;
    };

    if (metadata_json[ROOT_STATUS].asInt() == emptyPage) {
        std::cout << "{}" << "\n";
        return;
    }

    open(b_plus_index_file, metadata_json[INDEX_FULL_PATH].asString(), std::ios::in | std::ios::out);
    std::queue<Trunk> q;
    int current_level = -1;
    q.emplace(0, metadata_json[SEEK_ROOT].asInt64(), metadata_json[ROOT_STATUS].asInt());

    while (!q.empty()) {
        auto trunk = q.front();
        q.pop();

        seek(b_plus_index_file, trunk.seek);

        if (trunk.level > current_level) {
            std::cout << "\n";
            current_level++;
        }

        if (trunk.is_leaf) {
            auto page = std::make_shared<DataPage<INDEX_TYPE>>(metadata_json[DATA_PAGE_CAPACITY].asInt(), this);
            seek(b_plus_index_file, trunk.seek);
            page->read(b_plus_index_file);

            std::cout << "[";
            for (int i = 0; i < page->len(); ++i) {
                std::cout << page->records[i] << ",";
            }
            std::cout << "]";
        }

        else {
            auto page = std::make_shared<IndexPage<INDEX_TYPE>>(metadata_json[INDEX_PAGE_CAPACITY].asInt(), this);
            seek(b_plus_index_file, trunk.seek);
            page->read(b_plus_index_file);

            std::cout << "[";
            for (int i = 0; i < page->len(); ++i) {
                std::cout << page->keys[i] << ",";
            }
            std::cout << "]";

            for (int i = 0; i < page->num_keys + 1; ++i) {
                q.emplace(trunk.level + 1, page->children[i], page->points_to_leaf);
            }
        }
    }

    close(b_plus_index_file);
}


#include "bplustree.tpp"

#endif //B_PLUS_TREE_BPLUSTREE_HPP
