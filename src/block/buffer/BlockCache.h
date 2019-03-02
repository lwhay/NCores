#pragma once

#include <iostream>
#include <fstream>
#include <limits>
#include "FileOperations.h"
#include "CoresIterator.h"
#include "CoresAppender.h"
#include "CoresBlock.h"
#include "CoresRandomAccessor.h"

using namespace std;

template<typename type>
class PrimitiveBlock
        : public CoresIterator<type>,
          public CoresAppender<type>,
          public CoresBlock<type>,
          public CoresRandomAccesessor<type> {
    type *_cache;
    FILE *_fp;
    size_t _offset;
    int _count;
    int _limit;
    size_t _cursor;
    size_t _total;

public:
    PrimitiveBlock(FILE *fp, size_t begin, int count = 0, int limit = 1024) : _cache(new type[count]), _fp(fp),
                                                                              _offset(begin), _count(count),
                                                                              _limit(limit), _cursor(0) {
        //bigseek(fp, 0, SEEK_END);
        _total = bigtell(fp);
        //bigseek(fp, _offset, SEEK_SET);
    }

    void set(int idx, type value) {
        _cache[idx] = value;
    }

    type get(int idx) {
        return _cache[idx];
    }

    void append(type value) {
        _cache[_count] = value;
        if (_count++ == _limit) {
            appendToFile();
            _offset += _count * sizeof(type);
            _count = 0;
        }
    }

    ~PrimitiveBlock() {
        //printf("destructor of PB\n");
        if (_cache != nullptr) {
            //delete[] _cache;
            //_cache = nullptr;
        }
    }

    type *readFromFile() {
        if (bigseek(_fp, _offset, SEEK_SET) == 0) {
            fread(_cache, sizeof(type), _limit, _fp);
        } else {
            printf("Cannot seek block: %lld", _offset);
        }
        return _cache;
    }

    type *loadFromFile() {
        size_t rs = fread(_cache, sizeof(type), _limit, _fp);
        printf("> %llu %llu, %d\n", bigtell(_fp) / sizeof(type), rs, _limit);
        return _cache;
    }

    void appendToFile() {
        appendToFile(_cache);
    }

    void appendToFile(type *buf) {
        printf(">\t%llu %llu\n", bigtell(_fp), _offset);
        for (int i = 0; i < _limit; i++) {
            //printf("\t\t%d\n", _cache[i]);
        }
        fwrite(_cache, sizeof(type), _limit, _fp);
        printf("<\t%llu %llu\n", bigtell(_fp), _offset);
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
        bigseek(_fp, _offset, SEEK_SET);
        _cursor = 0;
    }

    void wind(size_t offset) {
        _offset = offset;
        bigseek(_fp, _offset, SEEK_SET);
    }

    bool hashNext() {
        if (_cursor < _limit) {
            return true;
        }
        if (_offset >= _total) {
            return false;
        }
        size_t read = fread(_cache, sizeof(type), _limit, _fp);
        _offset += read;
        if (read > 0) {
            _cursor = 0;
            return true;
        }
        return false;
    }

    type next() {
        return _cache[_cursor++];
    }
};