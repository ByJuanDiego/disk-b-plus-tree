//
// Created by juan diego on 9/17/23.
//

#ifndef B_PLUS_TREE_RECORD_HPP
#define B_PLUS_TREE_RECORD_HPP

#include <cstring>
#include <ostream>

using int32 = int32_t;

constexpr int NAME_LENGTH = 11;
struct Record {
    int32 id;
    int32 age;
    char name [NAME_LENGTH];

    Record(): id(-1), age(-1), name("\0") {
    }

    explicit Record(int32 _id, const char * _name, int32 _age): id(_id), age(_age), name("\0") {
        strncpy(name, _name, NAME_LENGTH);
        name[NAME_LENGTH - 1] = '\0';
    }

    friend auto operator << (std::ostream& ostream, const Record& record) -> std::ostream& {
        ostream << "(" << record.id << ", " << record.name << ", " << record.age << ")";
        return ostream;
    }
};


#endif //B_PLUS_TREE_RECORD_HPP
