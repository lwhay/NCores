//
// Created by Michael on 2018/12/4.
//

#pragma once

#include <lz4hc.h>
#include <lz4.h>

class LZ4Codec {
    const bool SMALL_END = false;

    size_t _size;
    size_t _rsize;
    size_t _csize;

    char *_buf;
    char *_cbuf;

    int _level;

    LZ4_streamHC_t body = {{0}};
    LZ4_streamHC_t *pb = &body;
    LZ4_stream_t lowbody = {{0}};
    LZ4_stream_t *plbody = &lowbody;

public:

    LZ4Codec(char *buf, size_t size = 16384, size_t level = 5) : _size(size), _rsize(1024 * 8 + size), _buf(buf),
                                                                 _csize(0), _cbuf(new char[_rsize]), _level(level) {}

    ~LZ4Codec() {
        delete[] _buf;
        delete[] _cbuf;
    }

    unsigned long long rawsize() {
        return _size;
    }

    unsigned long long codecsize() {
        return _csize;
    }

    char *code() {
        int lz4compbound = LZ4_COMPRESSBOUND(_size);
        if (_level > 3) {
            if (SMALL_END) {
                //Revise nest.
            } else {
                _csize = LZ4_compress_HC(_buf, _cbuf, _size, _rsize, 12);
            }
        } else {
            _csize = LZ4_compress_fast(_buf, _cbuf, _size, _rsize, 2);
        }
        return _cbuf;
    }

    char *decode() {
        int lz4compbound = LZ4_COMPRESSBOUND(_size);
        _csize = LZ4_decompress_fast(_buf, _cbuf, _size);
        return _cbuf;
    }
};