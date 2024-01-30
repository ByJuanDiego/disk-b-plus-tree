//
// Created by juan diego on 9/22/23.
//

#ifndef B_PLUS_TREE_TIME_UTILS_HPP
#define B_PLUS_TREE_TIME_UTILS_HPP

#include <chrono>
#include <functional>
#include <ostream>


class Clock {
public:
    void operator () (const std::function<void()>& procedure, std::ostream& ostream) const {
        auto start_time = std::chrono::high_resolution_clock::now();
        ostream << "====================[ Timer ]============================\n";
        procedure();
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> const duration = end_time - start_time;
        ostream << "[ Procedure finished in: " << duration.count() << "ms ] \n";
    };
};


#endif //B_PLUS_TREE_TIME_UTILS_HPP
