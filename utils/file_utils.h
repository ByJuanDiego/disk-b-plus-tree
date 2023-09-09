//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_FILE_UTILS_H
#define B_PLUS_TREE_FILE_UTILS_H

#include <string>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>

bool directory_exists(const std::string& path) {
    return std::filesystem::is_directory(path);
}

bool create_directory(const std::string& path) {
    int status = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    if (status == 0) {
        return true;
    }
    return false;
}

#endif //B_PLUS_TREE_FILE_UTILS_H
