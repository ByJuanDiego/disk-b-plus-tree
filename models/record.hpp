//
// Created by juan diego on 9/17/23.
//

#ifndef B_PLUS_TREE_RECORD_HPP
#define B_PLUS_TREE_RECORD_HPP

#include <cstring>
#include <ostream>

constexpr int NAME_LENGTH = 11;
struct Record {
    std::int32_t id;
    std::int32_t age;
    char name [NAME_LENGTH];

    Record(): id(-1), age(-1), name("\0") {
    }

    explicit Record(std::int32_t _id, const std::string& _name, std::int32_t _age): id(_id), age(_age), name("\0") {
        strncpy(name, _name.c_str(), NAME_LENGTH);
        name[NAME_LENGTH - 1] = '\0';
    }

    friend auto operator << (std::ostream& ostream, const Record& record) -> std::ostream& {
        ostream << "(" << record.id << ", " << record.name << ")";
        return ostream;
    }

    friend auto operator >> (std::istream& istream, Record& record) -> std::istream& {
        istream >> record.id >> record.name;
        return istream;
    }
};


#endif //B_PLUS_TREE_RECORD_HPP
