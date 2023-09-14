cmake_minimum_required(VERSION 3.26)
project(
        b_plus_tree
        VERSION 1.0
        DESCRIPTION "B+ Tree"
        LANGUAGES CXX)


# Generate compilation comands, usefull to lsp's
set(CMAKE_EXPORT_COMPILE_COMMANDS on)

add_executable(${PROJECT_NAME} main.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE utils include src)

target_sources(
        ${PROJECT_NAME}
        PRIVATE
        utils/buffer_size.hpp
        utils/error_handler.hpp
        utils/file_utils.hpp
        include/bplustree.hpp
        include/pages.hpp
        include/property.hpp
        src/bplustree.tpp
)

target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_20)


target_compile_options(
        ${PROJECT_NAME}
        INTERFACE # Prefered warnings
        $<$<CXX_COMPILER_ID:MSVC>:/W4
        /WX>
        $<$<CXX_COMPILER_ID:Clang>:-Weverything>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Werror
        -Wall
        -Wextra
        -Wpedantic
        -Wformat=2
        -Wno-unused-parameter
        -Wshadow
        -Wwrite-strings
        -Wcast-qual
        -Wcast-align
        -Wconversion>
        # Disable some warnings when using clang's Weverything
        $<$<CXX_COMPILER_ID:Clang>:-Wno-c++98-compat>
        $<$<CXX_COMPILER_ID:Clang>:-Wno-c++98-compat-pedantic>
        $<$<CXX_COMPILER_ID:Clang>:-Wno-c++20-compat>
        $<$<CXX_COMPILER_ID:Clang>:-Wno-zero-as-null-pointer-constant>
        $<$<CXX_COMPILER_ID:Clang>:-Wno-error=padded>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize-address-use-after-scope>
        $<$<CONFIG:RELEASE>:-Ofast>
        $<$<CONFIG:DEBUG>:-O0>
        $<$<CONFIG:DEBUG>:-ggdb3>
        "-fsanitize=address,undefined")

target_link_options(
        ${PROJECT_NAME} PRIVATE
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)


find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP jsoncpp)
link_libraries(${JSONCPP_LIBRARIES})

target_link_libraries(
        ${PROJECT_NAME}
        PRIVATE
        ${JSONCPP_LINK_LIBRARIES}
    )
