#pragma once

#include <iostream>
#include <fstream>
#include "FileOperations.h"
#include "CoresIterator.h"
#include "CoresBlock.h"
#include "CoresRandomAccessor.h"

template<typename type>
class PrimitiveBlock : public CoresIterator<type>, public CoresBlock<type>, public CoresRandomAccesessor<type> {
    int *_cache;

    FILE *_fp;

    uint64_t _offset;

    int _count;

public:
    PrimitiveBlock(FILE *fp, long begin, int count) : _cache(new int[count]), _fp(fp), _offset(begin), _count(count) {}

    void set(int idx, type value) {
        _cache[idx] = value;
    }

    type get(int idx) {
        return _cache[idx];
    }

    ~PrimitiveBlock() { delete[] _cache; }

    type *readFromFile() {
        if (bigseek(_fp, _offset, SEEK_SET) == 0) {
            fread(_cache, sizeof(type), _count, _fp);
        } else {
            printf("Cannot seek block: %lld", _offset);
        }
        return _cache;
    }

    type *loadFromFile() {
        fread(_cache, sizeof(type), _count, _fp);
        return _cache;
    }

    void appendToFile() {
        appendToFile(_cache);
    }

    void appendToFile(type *buf) {
        fwrite(_cache, sizeof(type), _count, _fp);
    }

    void writeToFile() {
        if (bigseek(_fp, _offset, SEEK_SET) == 0) {
            appendToFile(_cache);
        } else {
            printf("Cannot write block: %lld", _offset);
        }
    }

    void writeToFile(type *buf, uint64_t offset, int size) {
        if (bigseek(_fp, offset, SEEK_SET) == 0) {
            fwrite(buf, sizeof(type), size, _fp);
        } else {
            printf("Cannot write block: %lld", offset);
        }
    }

    void open() {

    }

    bool hashNext() {
        bool has = false;
        return has;
    }

    int next() {
        return 0;
    }
};