cmake_minimum_required(VERSION 3.26)
project(b_plus_tree)

set(CMAKE_CXX_STANDARD 17)

include_directories(utils include)

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP jsoncpp)

add_executable(b_plus_tree main.cpp
        include/bplustree.hpp
        include/pages.hpp
        utils/buffer_size.hpp
        include/property.hpp
        utils/file_utils.hpp
        utils/error_handler.hpp
    )


target_link_libraries(
        ${PROJECT_NAME}
        ${JSONCPP_LINK_LIBRARIES}
    )
