//
// Created by Michael on 2018/12/2.
//

#pragma once

#include <string.h>
#include <assert.h>
#include <iostream>

class Bitset {
    unsigned long long _capacity;

    unsigned long long _setbits;

    unsigned long long _validity;

    unsigned char *bitvector;

public:
    Bitset(unsigned long long capacity, unsigned char iv = 0) : _capacity(capacity), _setbits(0), _validity(0),
                                                                bitvector(new unsigned char[capacity / 8 + 1]) {
        memset(bitvector, iv, capacity / 8 + 1);
        for (int i = capacity % 8; i < 8; i++) {
            bitvector[capacity / 8] &= (0xff xor (1 << i));
        }

    }

    ~Bitset() { delete[] bitvector; }

    unsigned long long size() {
        return _capacity;
    }

    unsigned long long capacity() {
        return _setbits;
    }

    unsigned long long validity() {
        return _validity;
    }

    unsigned char getByte(unsigned long long idx) {
        return bitvector[idx];
    }

    inline unsigned long long nextSetBit(unsigned long long idx, int gran = 8) {
        idx++;
        if (gran == 64) {
            while (idx < _capacity) {
                /*if (idx % 64 == 0 && idx + 64 < _capacity) {
                    if (((unsigned long long *) bitvector)[idx / 64] == 0) {
                        idx += 64;
                        continue;
                    }
                }
                if (idx % 32 == 0 && idx + 32 < _capacity) {
                    if (((unsigned int *) bitvector)[idx / 32] == 0) {
                        idx += 32;
                        continue;
                    }
                }
                if (idx % 16 == 0 && idx + 16 < _capacity) {
                    if (((unsigned short *) bitvector)[idx / 16] == 0) {
                        idx += 16;
                        continue;
                    }
                }
                if (idx % 8 == 0 && idx + 8 < _capacity) {
                    if (bitvector[idx / 8] == 0) {
                        idx += 8;
                        continue;
                    }
                }
                if (get(idx)) {
                    break;
                }
                idx++;*/
                /*if (get(idx)) {
                    return idx;
                }
                int width = idx % 64;
                //std::cout << ">" << idx << "\t" << width << std::endl;
                switch (width) {
                    case 0:
                        if (((unsigned long long *) bitvector)[idx / 64] == 0) {
                            idx += 64;
                        } else {
                            width = 64;
                        }
                        break;
                    case 32:
                        if (((unsigned int *) bitvector)[idx / 32] == 0) {
                            idx += 32;
                            width = 0;
                        }
                        break;
                    case 16:
                        if (((unsigned short *) bitvector)[idx / 16] == 0) {
                            idx += 16;
                            width = 0;
                        }
                        break;
                    case 8:
                        if (bitvector[idx / 8] == 0) {
                            idx += 8;
                            width = 0;
                        }
                        break;
                    default:
                        break;
                }
                //std::cout << "<" << idx << "\t" << width << std::endl;
                for (int i = 0; i < width % 8; i++) {
                    if (get(idx)) {
                        //std::cout << "\t" << idx << "\t" << i << std::endl;
                        return idx;
                    }
                    idx++;
                }*/
                /*char begin = idx % 64;
                unsigned long long dw = ((unsigned long long *) bitvector)[idx / 64] >> begin;
                char limit = 64 - begin;
                if (dw != 0L) {
                    do {
                        if (get(idx)) {
                            return idx;
                        }
                        idx++;
                    } while (--limit >= 0);
                } else {
                    idx += limit;
                }*/
                /*char begin = idx % 8;
                unsigned char byte = bitvector[idx / 8] >> begin;
                char limit = 8 - begin;
                if (byte != 0) {
                    do {
                        if (get(idx)) {
                            return idx;
                        }
                        idx++;
                    } while (--limit >= 0);
                } else {
                    idx += limit;
                }*/
                /*char begin = idx % 16;
                unsigned short dw = ((unsigned short *) bitvector)[idx / 16] >> begin;
                char limit = 16 - begin;
                if (dw != 0) {
                    do {
                        if (get(idx)) {
                            return idx;
                        }
                        idx++;
                    } while (--limit >= 0);
                } else {
                    idx += limit;
                }*/
                char begin = idx % 64;
                unsigned long long dw = ((unsigned long long *) bitvector)[idx / 64] >> begin;
                char limit = 64 - begin;
                if (dw != 0L) {
                    /*for (char pos = 0; pos < limit; pos++) {
                        if (((unsigned char *) &dw)[pos / 8] & (1 << (pos % 8))) {
                            return idx + pos;
                        }
                    }*/
                    for (char pos = 0; pos < limit; pos++) {
                        if (dw & (1L << pos)) {
                            if ((idx + pos) < _capacity)
                                return (idx + pos);
                            else return -1;
                        }
                    }
                } else {
                    idx += limit;
                }
            };
        } else if (gran == 16) {
            do {
                char begin = idx % 16;
                unsigned short dw = ((unsigned short *) bitvector)[idx / 16] >> begin;
                char limit = 16 - begin;
                if (dw != 0) {
                    for (char pos = 0; pos < limit; pos++) {
                        if (dw & (1 << pos)) {
                            return (idx + pos);
                        }
                    }
                } else {
                    idx += limit;
                }
            } while (idx < _capacity);
        } else {
            do {
                char begin = idx % 8;
                unsigned char dw = bitvector[idx / 8] >> begin;
                char limit = 8 - begin;
                if (dw != 0) {
                    for (char pos = 0; pos < limit; pos++) {
                        if (dw & (1 << pos)) {
                            return (idx + pos);
                        }
                    }
                } else {
                    idx += limit;
                }
            } while (idx < _capacity);
        }
        return -1;
    }

    unsigned long long nextClearBit(unsigned long long idx) {
        while (get(idx)) {
            idx++;
        }
        return idx;
    }

    unsigned long long getLong(unsigned long long idx) {
        return *(unsigned long long *) &(bitvector[idx * 8]);
    }

    bool get(unsigned long long idx) {
        assert(idx < _capacity);
        return (bitvector[idx / 8] & 1 << (idx % 8)) != 0;
    }

    void set(unsigned long long idx) {
        assert(idx < _capacity);

        if (!get(idx)) {
            _setbits++;
        }
        if (idx > _validity) {
            _validity = idx;
        }
        bitvector[idx / 8] |= (1 << (idx % 8));
    }

    void clear(unsigned long long idx) {
        assert(idx < _capacity);
        if (get(idx)) {
            _setbits--;
        }
        if (idx > _validity) {
            _validity = idx;
        }
        bitvector[idx / 8] &= (0xff xor 1 << (idx % 8));
    }

    void setRange(unsigned long long bidx, unsigned long long eidx) {
        assert(bidx < eidx && bidx >= 0 && eidx < _capacity);
        if (eidx % 8 <= bidx % 8) {
            for (unsigned long long idx = bidx; idx < eidx; idx++) {
                set(idx);
            }
            return;
        }
        unsigned long long lrest = 8 - bidx % 8;
        unsigned long long bbyte = bidx / 8 + 1;
        unsigned long long bytes = (eidx / 8 > bidx / 8) ? (eidx / 8 - bidx / 8 - 1) : 0;
        unsigned long long rbidx = (eidx / 8) * 8;
        unsigned long long rrest = (eidx - bidx - lrest) % 8;
        for (unsigned long long i = 0; i < lrest; i++) {
            set(bidx + i);
        }
        for (unsigned long long i = 0; i < bytes; i++) {
            bitvector[bbyte + i] = 0xff;
        }
        for (unsigned long long i = 0; i < rrest; i++) {
            set(rbidx + i);
        }
    }

    void set(Bitset bs, unsigned long long size, unsigned long long begin = 0, unsigned long long rbeg = 0) {
        assert(begin + size < _capacity && rbeg + size < bs.size());
        if (size < 8) {
            unsigned long long finish = begin + size;
            for (unsigned long long i = 0; i < size; i++) {
                bs.get(rbeg + i) ? set(begin + i) : clear(begin + i);
            }
            return;
        } else if (size < 64) {

        } else {

        }
    }

    void AND(Bitset bs, unsigned long long size, unsigned long long begin = 0, unsigned long long rbeg = 0) {

    }

    void OR(Bitset bs, unsigned long long size, unsigned long long begin = 0, unsigned long long rbeg = 0) {

    }

    void XOR(Bitset bs, unsigned long long size, unsigned long long begin = 0, unsigned long long rbeg = 0) {

    }
};