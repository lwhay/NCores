#pragma once

#include <iostream>
#include <fstream>
#include <limits>
#include <cstring>
#include <vector>
#include <cmath>
#include "FileOperations.h"
#include "CoresIterator.h"
#include "CoresAppender.h"
#include "CoresBlock.h"
#include "CoresRandomAccessor.h"

using namespace std;

namespace codec {
    template<class T>
    void encode(T *_cache, int _idx, T _value) {
        _cache[_idx] = _value;
    }

    template<class T>
    T decode(T *_cache, int _idx) {
        return _cache[_idx];
    }

    template<>
    void encode(char **_cache, int _idx, char *_value) {
        short *length;
        length = (short *) ((char *) _cache + _idx);
        *length = strlen(_value);
        char *tmp = (char *) length + 2;
        std::strcpy(tmp, _value);
    }

    template<>
    char *decode(char **_cache, int _idx) {
        int index = 0;
        string str;
        for (int i = 0; i < _idx; ++i) {
            str = (char *) _cache + index;
            index += str.length() + 1;
        }
        return (char *) _cache + index;
    }

}

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
    PrimitiveBlock(FILE *fp, size_t begin, int count = 0, int limit = 1024) : _cache(
            (type *) calloc(limit, sizeof(char))), _fp(fp),
                                                                              _offset(begin), _count(count),
                                                                              _limit(limit / (sizeof(type))),
                                                                              _cursor(0) {
//        bigseek(fp, 0, SEEK_END);
//        _total = bigtell(fp);
//        bigseek(fp, _offset, SEEK_SET);
    }

    PrimitiveBlock(FILE *fp, char *cache, int limit = 1024) : _cache(cache), _fp(fp),
                                                              _offset(0), _count(0),
                                                              _limit(limit),
                                                              _cursor(0) {
//        bigseek(fp, 0, SEEK_END);
//        _total = bigtell(fp);
//        bigseek(fp, _offset, SEEK_SET);
    }

    void set(int idx, type value) {
        codec::encode(_cache, idx, value);
    }

    type get(int idx) {
        return codec::decode(_cache, idx);
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
        delete[] _cache;
        fflush(_fp);
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
        fread(_cache, sizeof(type), _limit, _fp);
        return _cache;
    }

    void appendToFile() {
        appendToFile(_cache);
    }

    void appendToFile(type *buf) {
        fwrite(buf, sizeof(type), _limit, _fp);
        memset(buf, 0, sizeof(type) * _limit);
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


template<typename T>
struct function_ {
    friend class Block;

    static T _get(void *_cache, int idx) {
        return ((T *) _cache)[idx];
    }

//    static int _append(void* _cache,int& _count,int& _cursor,int _limit,int _offset,T value){
//        ((T*)_cache)[_count] = value;
//        _count++;
//        _cursor+= sizeof(T);
//        if (_cursor>= _limit) {
////            Block::appendToFile();
//            _offset += _count * sizeof(T);
//            _count = 0;
//            _cursor=0;
//        }
//        return 0;
//    }

    static T _next(int &_count, void *_cache) {
        return ((T *) _cache)[_count++];
    }
};


template<>
struct function_<char *> {
    static char *_get(void *_cache, int idx) {
        int index = 0;
        string str;
        for (int i = 0; i < idx; ++i) {
            str = (char *) _cache + index;
            index += str.length() + 1;
        }
        return (char *) _cache + index;
    }

//    static int _append()(void* _cache,int& _count,int& _cursor,int _limit,int _offset,char* value) {
//        if (_cursor + strlen(value) >= _limit) {
////            appendToFile();
//            _offset += _limit;
//            _count = 0;
//            _cursor = 0;
//        }
//        short *length;
//        length = (short *) ((char *) _cache + _cursor);
//        *length = strlen(value);
//        char *tmp = (char *) length + 2;
//        std::strcpy(tmp, value);
//        return 0;
//    }


    static char *_next(int &_count, void *_cache) {
        char *tmp = (char *) _cache + _count;
        _count += strlen(tmp) + 1;
        return tmp;
    }
};

class Block {
    void *_cache;

    FILE *_fp;

    size_t _offset;

    int _count;

    int _limit;

    int _cursor;

    size_t _total;

public:
    Block() {}

    Block(FILE *fp, size_t begin, int count = 0, int limit = 1024) : _cache(
            (void *) calloc(limit, sizeof(char))), _fp(fp),
                                                                     _offset(begin), _count(count),
                                                                     _limit(limit),
                                                                     _cursor(0) {
//        fseek(fp, 0, SEEK_END);
//        _total = bigtell(fp);
//        fseek(fp, _offset, SEEK_SET);
    }

    Block(FILE *fp, char *cache, int limit = 1024) : _cache(cache), _fp(fp),
                                                     _offset(0), _count(0),
                                                     _limit(limit),
                                                     _cursor(0) {
//        bigseek(fp, 0, SEEK_END);
//        _total = bigtell(fp);
//        bigseek(fp, _offset, SEEK_SET);
    }

//    template <typename T>
//    void set(int idx, T value) {
//        codec::encode(_cache, idx, value);
//    }



    template<typename type>
    type get(int idx) {
        type tmp = function_<type>::_get((char *) _cache + _cursor, idx);
        return tmp;
    };

//    template <typename type>
//    void append(type value) {
//        function_<type>::_append(_cache,_count,_cursor, _limit,_offset,value);
//    }

    ~Block() {
        delete[] _cache;
        fflush(_fp);
    }

    template<typename type>
    type *readFromFile() {
        if (fseek(_fp, _offset, SEEK_SET) == 0) {
            fread(_cache, sizeof(type), _limit, _fp);
        } else {
            printf("Cannot seek block: %lld", _offset);
        }
        return _cache;
    }

    void loadFromFile() {
        _cursor = 0;
        _count = 0;
        fread(_cache, sizeof(char), _limit, _fp);
    }

    void loadFromFile(int rowcount) {
        _cursor = ceil((double) rowcount / 8);
        _count = 0;
        fread(_cache, sizeof(char), _limit, _fp);
    }

    void skipload(int offset, int rowcount) {
        _cursor = ceil((double) rowcount / 8);
        _count = 0;
        fseek(_fp, offset, SEEK_SET);
        fread(_cache, sizeof(char), _limit, _fp);
    }

    bool isvalid(int off) {
        return (((char *) _cache)[off / 8]) & (1l << off % 8) != 0;
    }

    int getValidOff(int off) {
        int count = 0;
        for (int i = 0; i < off / 8; ++i) {
            for (int j = 0; j < 8; ++j) {
                count += (((char *) _cache)[i] >> j) & 1l;
            }
        }
        for (int k = 0; k < off % 8; ++k) {
            count += (((char *) _cache)[off / 8] >> k) & 1l;
        }
        return count;
    }

    void appendToFile() {
        appendToFile(_cache);
    }

    void appendToFile(void *buf) {
        fwrite(buf, sizeof(char), _limit, _fp);
        memset(buf, 0, sizeof(char) * _limit);
    }

    void writeToFile() {
        if (fseek(_fp, _offset, SEEK_SET) == 0) {
            appendToFile(_cache);
        } else {
            printf("Cannot write block: %lld", _offset);
        }
    }

    void writeToFile(void *buf, uint64_t offset, int size) {
        if (fseek(_fp, offset, SEEK_SET) == 0) {
            fwrite(buf, sizeof(char), size, _fp);
        } else {
            printf("Cannot write block: %lld", offset);
        }
    }

    void open() {
        fseek(_fp, _offset, SEEK_SET);
        _cursor = 0;
    }

    void wind(size_t offset) {
        _offset = offset;
        fseek(_fp, _offset, SEEK_SET);
    }

    bool hashNext() {
        if (_cursor < _limit) {
            return true;
        }
        if (_offset >= _total) {
            return false;
        }
        size_t read = fread(_cache, sizeof(char), _limit, _fp);
        _offset += read;
        if (read > 0) {
            _cursor = 0;
            return true;
        }
        return false;
    }

    template<typename type>
    type next() {
        int &tmp = _count;
        type tmpc = function_<type>::_next(_count, (char *) _cache + _cursor);
        return tmpc;
    }

    vector<int> *initString(int offsize) {
        char *tmpbuf = new char[4]();
        char *tmp = (char *) _cache + _cursor;

        vector<int> *offarr = new vector<int>();
        memcpy(tmpbuf, tmp, offsize);
        int *tmpi = (int *) tmpbuf;
        offarr->push_back(*tmpi);
        int num = (*tmpi) / offsize;
        for (int j = 1; j < num; ++j) {
            tmp += offsize;
            memcpy(tmpbuf, tmp, offsize);
            offarr->push_back(*tmpi);
        }
        return offarr;
    }

    char *getoffstring(int offset) {
        return (char *) _cache + _cursor + offset;
    }

};
