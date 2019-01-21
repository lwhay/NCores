#pragma once

#include <stdio.h>
#include "FileOperations.h"

class Cache {
public:
    enum uses {
        free, used, fixed
    };    // for fuf_cont
    int ptr;                //current position in cache
    int cachesize;        //the number of blocks kept in memory
    int blocklength;
    int page_faults;
    bigint *cache_cont;        // array of the indices of blocks that are in cache
    FILE **cache_tree;      // array of ptrs to the correspondent Cacheables where the blocks belong to
    uses *fuf_cont;         //indicator array that shows whether one cache block is free, used or fixed
    bool *dirty_indicator;  //indicator that shows if a cache page has been written
    int num_io;

    int *LRU_indicator; //indicator that shows how old (unused) is a page in the cache

    char **cache;        // Cache

    Cache(int csize, int blength);

    ~Cache();

    int next();

    int in_cache(bigint index, FILE *rt);

    bool read_block(bigint b, FILE *rt, void *);

    bool write_block(bigint b, FILE *rt, void *);

    bigint write_new_block(FILE *rt, void *);

    void flush();

    void clear(FILE *rt);
};
