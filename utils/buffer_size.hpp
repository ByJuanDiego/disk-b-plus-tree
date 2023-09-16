//
// Created by juan diego on 9/7/23.
//

#ifndef B_PLUS_TREE_BUFFER_SIZE_HPP
#define B_PLUS_TREE_BUFFER_SIZE_HPP

#include <cstdint>
#if defined(_WIN64)
#include <windows.h>

#elif defined(__unix__)
#include <unistd.h>

#endif


auto get_buffer_size() -> uint64_t {
#if defined(_WIN64)
    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);
    DWORD page_size = systemInfo.dwPageSize;
    return page_size;
#elif defined(__unix__)
    return getpagesize();
#endif
}

#endif //B_PLUS_TREE_BUFFER_SIZE_HPP
