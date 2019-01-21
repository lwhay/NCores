#pragma once

#define _CRT_SECURE_NO_DEPRECATE

#include <string.h>
#include "cache/LSMCache.h"

#ifndef __DEBUG
//#define __DEBUG
#endif

typedef long KEY_TYPE;

enum RET_TYPE {
    NORMAL, SPLIT, REINSERT, DELETED, NOTFOUND, UNDERFILL
};

class RECORD_TYPE {
public:
    float *coord;
    int dim;

    RECORD_TYPE();

    ~RECORD_TYPE();

    void init(int _dim);

    int get_size();

    void get_mbr(float *low, float *high);

    void read_from_buffer(char *buffer);

    void write_to_buffer(char *buffer);

    RECORD_TYPE &operator=(RECORD_TYPE &d);
};

class Tree {
public:
    Cache *cache;
    char fname[100];    // the index file name
    FILE *fptr;  // the index file
    int num_of_data;
    int num_of_dnode;
    int num_of_inode;
    int num_of_onode;
    int root_lvl;
    bigint root;

    virtual void traverse() {};
};


class Tree_CC {
public:
    Cache *cache;
    char fname[100];    // the index file name
    FILE *fptr;  // the index file
    int num_of_data;
    int num_of_dnode;
    int num_of_inode;
    int num_of_onode;
    int root_lvl;
    bigint root;

    virtual void traverse() {};
};

class Node {
public:
    int num_entries;
    int capacity;
    bigint block;
    bool dirty;
    Tree *tree;
};