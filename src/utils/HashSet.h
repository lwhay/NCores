//
// Created by Michael on 2018/12/5.
//

#pragma once

#include <iostream>
#include <unordered_set>

#define DEFAULT_BUCKET_COUNT 65536

#define BIT_WIDE 16

template<typename type>
class HashSet {
    const unsigned short width = sizeof(type) * 8;

    const unsigned short gran = width / BIT_WIDE;

    std::unordered_set<type> buckets[DEFAULT_BUCKET_COUNT];

    size_t _size;

    unsigned short bitmap[BIT_WIDE] = {9, 3, 14, 4, 1, 12, 6, 2, 0, 11, 7, 13, 8, 15, 10, 5};
public:
    HashSet() : _size(0) {}

    ~HashSet() {
        for (int i = 0; i < DEFAULT_BUCKET_COUNT; i++) {
            if (buckets[i].size() > 0) {
                std::cout << i << ":" << buckets[i].size() << " ";
            }
            buckets[i].clear();
        }
        std::cout << _size << std::endl;
    }

    unsigned short hashkey(type value) {
        unsigned short v = 0;
        for (int i = 0; i < BIT_WIDE; i++) {
            unsigned long long bits = value & (1LLU << (bitmap[i] * gran));
            if (bits != 0LLU) {
                v |= (1 << bitmap[i]);
            }
            //v |= ((value & (1LLU << (bitmap[i] * gran)) == 0LLU) ? 0 : (1 << bitmap[i]));
        }
        return v;
    }

    void add(type v) {
        unsigned short k = hashkey(v);
        if (buckets[k].find(v) == buckets[k].end()) {
            buckets[k].insert(v);
            _size++;
        }
    }

    size_t size() {
        return size;
    }
};
