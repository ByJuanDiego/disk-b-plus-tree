cmake_minimum_required(VERSION 3.26)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(b_plus_tree VERSION 1.0 DESCRIPTION "B+ Tree" LANGUAGES CXX)

# Set common source and header files
set(SRC_FILES
        src/bplustree.tpp
        src/data_page.tpp
        src/index_page.tpp
        src/page.tpp
        src/property.cpp
)

set(INCLUDE_DIRS
        utils
        include
        models
        src
)

# Set compile features globally
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add executables
add_executable(${PROJECT_NAME} main.cpp)
add_executable(between_by_id examples/between_by_id.cpp)
add_executable(insert_by_id examples/insert_by_id.cpp)
add_executable(test_search_by_id tests/test_search_by_id.cpp)
add_executable(test_insert_by_id tests/test_insert_by_id.cpp)
add_executable(test_remove_by_id tests/test_remove_by_id.cpp)

# Add common include directories
foreach(dir ${INCLUDE_DIRS})
    target_include_directories(${PROJECT_NAME} PRIVATE ${dir})
    target_include_directories(between_by_id PRIVATE ${dir})
    target_include_directories(insert_by_id PRIVATE ${dir})
    target_include_directories(test_search_by_id PRIVATE ${dir})
    target_include_directories(test_insert_by_id PRIVATE ${dir})
    target_include_directories(test_remove_by_id PRIVATE ${dir})
endforeach()

# Set compile features for all targets
foreach(target ${PROJECT_NAME} between_by_id insert_by_id test_search_by_id test_insert_by_id test_remove_by_id)
    target_compile_features(${target} PRIVATE cxx_std_20)
endforeach()

# Add source files to targets
foreach(target ${PROJECT_NAME} between_by_id insert_by_id test_search_by_id test_insert_by_id test_remove_by_id)
    target_sources(${target} PRIVATE ${SRC_FILES})
endforeach()

