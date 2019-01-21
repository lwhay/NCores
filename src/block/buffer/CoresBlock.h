//
// Created by Michael on 2018/12/2.
//

#pragma once

#include <stdint.h>

template<typename type>
class CoresBlock {
    virtual type *loadFromFile() = 0;

    virtual type *readFromFile() = 0;

    virtual void appendToFile() = 0;

    virtual void appendToFile(type *buf) = 0;

    virtual void writeToFile() = 0;

    virtual void writeToFile(type *buf, uint64_t offset, int size) = 0;
};
