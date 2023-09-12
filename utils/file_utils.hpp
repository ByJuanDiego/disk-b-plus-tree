//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_FILE_UTILS_HPP
#define B_PLUS_TREE_FILE_UTILS_HPP

#include <string>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>

bool directory_exists(const std::string& path) {
    return std::filesystem::is_directory(path);
}

bool create_directory(const std::string& path) {
    // Create the directory path
    std::string dir_path;
    std::string current_path;
    std::stringstream path_stream(path);

    while (getline(path_stream, dir_path, '/')) {
        if (dir_path == ".") {
            continue;
        }

        current_path += dir_path + "/";
        bool status = mkdir(current_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (status == 1) {
            return false;
        }
    }

    return true;
}

#endif //B_PLUS_TREE_FILE_UTILS_HPP
