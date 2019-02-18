//
// Created by Michael on 2019/2/17.
//
#pragma once

#include "BlockCache.h"

template<typename type>
class Columnar {
    FILE *filep;
    PrimitiveBlock<type> *block;
    size_t begin;
    int bs;
    int ps;
public:
    Columnar(FILE *fp, size_t start, int block_size, int prefetch_size) : filep(fp), begin(start), bs(block_size),
                                                                          ps(prefetch_size) {
        block = new PrimitiveBlock<type>(filep, begin, 0, block_size);
    }

    ~Columnar() {
        delete block;
    }

    void write(type value) {
        block->append(value);
    }

    type next() {
        if (block.hashNext()) {
            return block->next();
        }
    }
};