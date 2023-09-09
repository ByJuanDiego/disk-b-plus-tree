cmake_minimum_required(VERSION 3.26)
project(b_plus_tree)

set(CMAKE_CXX_STANDARD 17)

include_directories(utils include)

add_executable(b_plus_tree main.cpp
        include/bplustree.h
        include/pages.h
        utils/buffer_size.h
        include/property.h
        utils/file_utils.h)
