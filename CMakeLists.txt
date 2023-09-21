cmake_minimum_required(VERSION 3.26)
project(
        b_plus_tree
        VERSION 1.0
        DESCRIPTION "B+ Tree"
        LANGUAGES CXX)


# Generate compilation comands, usefull to lsp's
set(CMAKE_EXPORT_COMPILE_COMMANDS on)

add_executable(${PROJECT_NAME} main.cpp)
add_executable(search_by_id examples/search_by_id.cpp)
add_executable(test_between_by_id test/test_between_by_id.cpp)
add_executable(test_insert_by_id test/test_insert_by_id.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE utils include src models)
target_include_directories(search_by_id PRIVATE utils include src models)
target_include_directories(test_between_by_id PRIVATE utils include src models)
target_include_directories(test_insert_by_id PRIVATE utils include src models)

target_sources(
        ${PROJECT_NAME}
        PRIVATE
        src/bplustree.tpp
        src/data_page.tpp
        src/index_page.tpp
        src/property.cpp
        utils/buffer_size.hpp
        utils/error_handler.hpp
        utils/file_utils.hpp
        utils/types.hpp
        include/bplustree.hpp
        include/data_page.hpp
        include/index_page.hpp
        include/pages.hpp
        include/property.hpp
        models/record.hpp
        main.cpp
)

target_sources(
        test_between_by_id
        PRIVATE
        src/bplustree.tpp
        src/data_page.tpp
        src/index_page.tpp
        src/property.cpp
        utils/buffer_size.hpp
        utils/error_handler.hpp
        utils/file_utils.hpp
        utils/types.hpp
        include/bplustree.hpp
        include/data_page.hpp
        include/index_page.hpp
        include/pages.hpp
        include/property.hpp
        models/record.hpp
        test/test_between_by_id.cpp
)

target_sources(
        search_by_id
        PRIVATE
        src/bplustree.tpp
        src/data_page.tpp
        src/index_page.tpp
        src/property.cpp
        utils/buffer_size.hpp
        utils/error_handler.hpp
        utils/file_utils.hpp
        utils/types.hpp
        include/bplustree.hpp
        include/data_page.hpp
        include/index_page.hpp
        include/pages.hpp
        include/property.hpp
        models/record.hpp
        examples/search_by_id.cpp
)


target_sources(
        test_insert_by_id
        PRIVATE
        src/bplustree.tpp
        src/data_page.tpp
        src/index_page.tpp
        src/property.cpp
        utils/buffer_size.hpp
        utils/error_handler.hpp
        utils/file_utils.hpp
        utils/types.hpp
        include/bplustree.hpp
        include/data_page.hpp
        include/index_page.hpp
        include/pages.hpp
        include/property.hpp
        models/record.hpp
        test/test_insert_by_id.cpp
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

target_link_libraries(
        search_by_id
        PRIVATE
        ${JSONCPP_LINK_LIBRARIES}
)

target_link_libraries(
        test_between_by_id
        PRIVATE
        ${JSONCPP_LINK_LIBRARIES}
)

target_link_libraries(
        test_insert_by_id
        PRIVATE
        ${JSONCPP_LINK_LIBRARIES}
)
