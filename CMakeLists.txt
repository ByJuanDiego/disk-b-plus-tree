cmake_minimum_required(VERSION 3.26)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(b_plus_tree VERSION 1.0 DESCRIPTION "B+ Tree" LANGUAGES CXX)

file (GLOB_RECURSE SRC_FILES src/*.cpp src/*.tpp)

add_executable(${PROJECT_NAME} main.cpp)
add_executable(between_by_id examples/between_by_id.cpp)
add_executable(insert_by_id examples/insert_by_id.cpp)
add_executable(test_between_by_id test/test_between_by_id.cpp)
add_executable(test_insert_by_id test/test_insert_by_id.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE utils include models src)
target_include_directories(between_by_id PRIVATE  utils include models src)
target_include_directories(insert_by_id PRIVATE  utils include models src)
target_include_directories(test_between_by_id PRIVATE utils include models src)
target_include_directories(test_insert_by_id PRIVATE utils include models src)

target_sources(${PROJECT_NAME} PRIVATE ${SRC_FILES})
target_sources(between_by_id PRIVATE ${SRC_FILES})
target_sources(insert_by_id PRIVATE ${SRC_FILES})
target_sources(test_between_by_id PRIVATE ${SRC_FILES})
target_sources(test_insert_by_id PRIVATE ${SRC_FILES})

target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_20)
target_compile_features(between_by_id PRIVATE cxx_std_20)
target_compile_features(insert_by_id PRIVATE cxx_std_20)
target_compile_features(test_between_by_id PRIVATE cxx_std_20)
target_compile_features(test_insert_by_id PRIVATE cxx_std_20)

target_compile_options(
        ${PROJECT_NAME}
        INTERFACE # Preferred warnings
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

target_link_options(${PROJECT_NAME} PRIVATE $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
target_link_options(between_by_id PRIVATE $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
target_link_options(insert_by_id PRIVATE $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
target_link_options(test_between_by_id PRIVATE $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)
target_link_options(test_insert_by_id PRIVATE $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=address,undefined>)

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP jsoncpp)
link_libraries(${JSONCPP_LIBRARIES})

target_link_libraries(${PROJECT_NAME} PRIVATE ${JSONCPP_LINK_LIBRARIES})
target_link_libraries(between_by_id PRIVATE ${JSONCPP_LINK_LIBRARIES})
target_link_libraries(insert_by_id PRIVATE ${JSONCPP_LINK_LIBRARIES})
target_link_libraries(test_between_by_id PRIVATE ${JSONCPP_LINK_LIBRARIES})
target_link_libraries(test_insert_by_id PRIVATE ${JSONCPP_LINK_LIBRARIES})
