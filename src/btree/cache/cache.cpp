#include <stdlib.h>
#include <string.h>
#include "LSMCache.h"

Cache::Cache(int csize, int blength) {
    ptr = 0;
    blocklength = blength;
    cachesize = csize;

    if (csize > 0) {
        cache_cont = new bigint[cachesize];
        cache_tree = new FILE *[cachesize];
        fuf_cont = new uses[cachesize];
        LRU_indicator = new int[cachesize];
        dirty_indicator = new bool[cachesize];

        for (int i = 0; i < cachesize; i++) {
            cache_cont[i] = -1;
            cache_tree[i] = NULL;
            fuf_cont[i] = free;
            LRU_indicator[i] = 0;
            dirty_indicator[i] = false;
        }

        cache = new char *[cachesize];
        for (int i = 0; i < cachesize; i++)
            cache[i] = new char[blocklength];
    }
    page_faults = 0;
    num_io = 0;
}

Cache::~Cache() {
    if (cachesize > 0) {
        delete[] cache_cont;
        delete[] fuf_cont;
        delete[] LRU_indicator;
        delete[] cache_tree;
        delete[] dirty_indicator;

        for (int i = 0; i < cachesize; i++)
            delete[] cache[i];
        delete[] cache;
    }
}

int Cache::next() {
    int ret_val, tmp;

    if (cachesize == 0) return -1;
    else {
        if (fuf_cont[ptr] == free) {
            ret_val = ptr++;
            ptr = ptr % cachesize;
            return ret_val;
        } else {
            tmp = (ptr + 1) % cachesize;
            //find the first free block
            while (tmp != ptr && fuf_cont[tmp] != free)
                tmp = (tmp + 1) % cachesize;

            if (ptr == tmp)    //failed to find a free block
            {
                // select a victim page to be written back to disk
                int lru_index = 0; // the index of the victim page
                for (int i = 0; i < cachesize; i++)
                    if (LRU_indicator[i] > LRU_indicator[lru_index])
                        lru_index = i;

                ptr = lru_index;

                if (dirty_indicator[ptr]) {
                    //write block
                    if (bigseek(cache_tree[ptr], cache_cont[ptr], SEEK_SET) == 0) {
                        if (!fwrite(cache[ptr], blocklength, 1, cache_tree[ptr])) {
                            printf("------------------------File I/O Error-------------------------------\n");
                            printf("Can't read block.\n");
                            printf("---------------------------------------------------------------------\n");
                            exit(-1);
                        }
                        num_io++;
                    } else {
                        printf("------------------------File I/O Error-------------------------------\n");
                        printf("Can't seek block.\n");
                        printf("---------------------------------------------------------------------\n");
                        exit(-1);
                    }
                }

                fuf_cont[ptr] = free;
                dirty_indicator[ptr] = false;

                ret_val = ptr++;
                ptr = ptr % cachesize;

                return ret_val;
            } else  //a cache page has been found
                return tmp;
        }
    }
}

int Cache::in_cache(bigint block, FILE *rt) {
    int i;
    int ret_val = -1;
    for (i = 0; i < cachesize; i++) {
        if ((cache_cont[i] == block) && (cache_tree[i] == rt) && (fuf_cont[i] != free)) {
            LRU_indicator[i] = 0;
            ret_val = i;
        } else if (fuf_cont[i] != free)
            LRU_indicator[i]++;    // increase indicator for this block
    }
    return ret_val;
}

bool Cache::read_block(bigint block, FILE *rt, void *node) {
    int c_ind;

    bigint filesize = -1;
    if (bigseek(rt, 0L, SEEK_END) == 0)
        filesize = bigtell(rt);

    if (block <= filesize && block >= 0) {
        if ((c_ind = in_cache(block, rt)) >= 0)
            memcpy(node, cache[c_ind], blocklength);
        else // not in Cache
        {
            page_faults++;
            c_ind = next();
            if (c_ind >= 0) // a block has been freed in cache
            {
                if (bigseek(rt, block, SEEK_SET) == 0) {
                    if (!fread(cache[c_ind], blocklength, 1, rt)) {
                        printf("------------------------File I/O Error-------------------------------\n");
                        printf("Can't read block.\n");
                        printf("---------------------------------------------------------------------\n");
                        exit(-1);
                    }
                } else {
                    printf("------------------------File I/O Error-------------------------------\n");
                    printf("Can't seek block.\n");
                    printf("---------------------------------------------------------------------\n");
                    exit(-1);
                }
                num_io++;
                cache_cont[c_ind] = block;
                cache_tree[c_ind] = rt;
                fuf_cont[c_ind] = used;
                LRU_indicator[c_ind] = 0;
                memcpy(node, cache[c_ind], blocklength);
            } else {
                if (bigseek(rt, block, SEEK_SET) == 0) {
                    if (!fread(node, blocklength, 1, rt)) {
                        printf("------------------------File I/O Error-------------------------------\n");
                        printf("Can't read block.\n");
                        printf("---------------------------------------------------------------------\n");
                        exit(-1);
                    }
                } else {
                    printf("------------------------File I/O Error-------------------------------\n");
                    printf("Can't seek block.\n");
                    printf("---------------------------------------------------------------------\n");
                    exit(-1);
                }
                num_io++;
            }
        }
        return true;
    } else {
        printf("------------------------File I/O Error-------------------------------\n");
        printf("Reader requesting block error %lld %lld.\n", block, filesize);
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
}

bool Cache::write_block(bigint block, FILE *rt, void *node) {
    int c_ind;

    bigint filesize = -1;
    if (bigseek(rt, 0L, SEEK_END) == 0)
        filesize = bigtell(rt);

    if (block <= filesize && block >= 0) {
        c_ind = in_cache(block, rt);
        if (c_ind >= 0) {
            memcpy(cache[c_ind], node, blocklength);
            dirty_indicator[c_ind] = true;
        } else        // not in Cache
        {
            c_ind = next();
            if (c_ind >= 0) {
                memcpy(cache[c_ind], node, blocklength);
                cache_cont[c_ind] = block;
                cache_tree[c_ind] = rt;
                fuf_cont[c_ind] = used;
                LRU_indicator[c_ind] = 0;
                dirty_indicator[c_ind] = true;
            } else {
                if (bigseek(rt, block, SEEK_SET) == 0) {
                    if (!fwrite(node, blocklength, 1, rt)) {
                        printf("------------------------File I/O Error-------------------------------\n");
                        printf("Can't read block.\n");
                        printf("---------------------------------------------------------------------\n");
                        exit(-1);
                    }
                } else {
                    printf("------------------------File I/O Error-------------------------------\n");
                    printf("Can't seek block.\n");
                    printf("---------------------------------------------------------------------\n");
                    exit(-1);
                }
                num_io++;
            }
        }
        return true;
    } else {
        printf("------------------------File I/O Error-------------------------------\n");
        printf("Writer requesting block error %lld %lld.\n", block, filesize);
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
}

bigint Cache::write_new_block(FILE *rt, void *node) {
    bigint block = -1;
    if (bigseek(rt, 0L, SEEK_END) == 0) {
        block = bigtell(rt);
        if (!fwrite(node, blocklength, 1, rt)) {
            printf("------------------------File I/O Error-------------------------------\n");
            printf("Can't write block.\n");
            printf("---------------------------------------------------------------------\n");
            exit(-1);
        }
        write_block(block, rt, node);
    } else {
        printf("------------------------File I/O Error-------------------------------\n");
        printf("Can't seek block.\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
    return block;
}

void Cache::flush() {
    for (int i = 0; i < cachesize; i++) {
        if (fuf_cont[i] != free && dirty_indicator[i]) {
            if (bigseek(cache_tree[i], cache_cont[i], SEEK_SET) == 0) {
                if (!fwrite(cache[i], blocklength, 1, cache_tree[i])) {
                    printf("------------------------File I/O Error-------------------------------\n");
                    printf("Can't write block.\n");
                    printf("---------------------------------------------------------------------\n");
                    exit(-1);
                }
            } else {
                printf("------------------------File I/O Error-------------------------------\n");
                printf("Can't seek block.\n");
                printf("---------------------------------------------------------------------\n");
                exit(-1);
            }
            num_io++;
        }
        fuf_cont[i] = free;
        dirty_indicator[i] = false;
        cache_tree[i] = NULL;
        cache_cont[i] = -1;
        LRU_indicator[i] = 0;
    }
    ptr = 0;
}

void Cache::clear(FILE *rt) {
    for (int i = 0; i < cachesize; i++) {
        if (fuf_cont[i] != free && cache_tree[i] == rt) {
            fuf_cont[i] = free;
            dirty_indicator[i] = false;
            cache_tree[i] = NULL;
            cache_cont[i] = -1;
            LRU_indicator[i] = 0;
        }
    }
}