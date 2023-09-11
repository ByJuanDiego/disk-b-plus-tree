//
// Created by juan diego on 9/11/23.
//

#ifndef B_PLUS_TREE_ERROR_HANDLER_H
#define B_PLUS_TREE_ERROR_HANDLER_H

#include <stdexcept>

struct FullPage : public virtual std::runtime_error {
    FullPage(): std::runtime_error("The page is full") {}
};

struct KeyNotFound : public virtual std::runtime_error {
    KeyNotFound(): std::runtime_error("Key not found") {}
};

struct CreateDirectoryError : public virtual std::runtime_error {
    CreateDirectoryError(): std::runtime_error("Error creating directory") {}
};

struct CreateFileError : public virtual std::runtime_error {
    CreateFileError(): std::runtime_error("Error creating file") {}
};



#endif //B_PLUS_TREE_ERROR_HANDLER_H
