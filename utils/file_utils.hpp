//
// Created by juan diego on 9/8/23.
//

#ifndef B_PLUS_TREE_FILE_UTILS_HPP
#define B_PLUS_TREE_FILE_UTILS_HPP

#include <string>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>


auto directory_exists(const std::string& path) -> bool {
    return std::filesystem::is_directory(path);
}


auto create_directory(const std::string& path) -> bool {
    // Create the directory path
    std::string dir_path;
    std::string current_path;
    std::stringstream path_stream(path);

    while (getline(path_stream, dir_path, '/')) {
        if (dir_path == ".") {
            continue;
        }

        current_path += dir_path + "/";
        int const status = mkdir(current_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (status == 1) {
            return false;
        }
    }

    return true;
}


auto open(std::fstream &file, const std::string &file_name, std::ios::openmode mode_flags) -> void {
    file.open(file_name, mode_flags);
}


auto close(std::fstream &file) -> void {
    file.close();
}

auto seek(std::fstream &file, std::int64_t pos, std::ios::seekdir offset = std::ios::beg) -> void {
    file.seekg(pos, offset);
    file.seekp(pos, offset);
}


#endif //B_PLUS_TREE_FILE_UTILS_HPP
