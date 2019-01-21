//
// Created by Michael on 2018/12/8.
//

#include <iostream>
#include "fdtree.h"
#include "btree.h"
#include "buffer.h"
#include "error.h"
#include "utils.h"

using namespace std;

#define NUM (1 << 8)

#define BSIZE 1024 * 1024 * 32

#undef INFILE
#ifdef INFILE
#define DATAPATH "e:/kv.data"
#define PAGE_SIZE (1 << 12)
#endif

#define SHUFFLE false

#define REVERSE false

int ELOG_LEVEL = INFO;

int main(int argc, char **argv) {
    initBufPool(BSIZE, true);
    cout << "Buffered" << endl;
    BTreeState *state;
    BTree *tree = new BTree;
    cout << "Created" << endl;
    state = btree_initWrite(tree, true);
    cout << "Inited" << endl;
    Entry e;
    Tracer tracer;
#ifndef INFILE
    int *input = new int[NUM];
    //for (int i = NUM; i >= 0; i--) {
    for (int i = 0; i < NUM; i++) {
        input[i] = i;
    }
#if SHUFFLE
    for (int i = 0; i < NUM; i++) {
        unsigned long long idx = (rand() * rand() + rand()) % NUM;
        int tmp = input[i];
        input[i] = input[idx];
        input[idx] = tmp;
    }
    int *verify = new int[NUM];
    memset(verify, 0, NUM);
    for (int i = 0; i < NUM; i++) {
        verify[input[i]] = input[i];
    }
    for (int i = 0; i < NUM; i++) {
        if (i > 0 && verify[i] != verify[i - 1] + 1) {
            exit(-1);
        }
    }
#endif
    tracer.startTime();
#if REVERSE
    for (int i = NUM - 1; i >= 0; i--) {
#else
        for (int i = 0; i < NUM; i++) {
#endif
        e.key = input[i];
        e.ptr = input[i];
        btree_insert(tree, e);
        //btree_writeNextEntry(state, &e, 0.7);
    }
#else
    FILE *dp = fopen(DATAPATH, "rb+");
    char buffer[PAGE_SIZE];
    int count = PAGE_SIZE / (2 * sizeof(int));
    int round = NUM / count;
    for (int i = 0; i < round; i++) {
        fread(buffer, PAGE_SIZE, 1, dp);
        for (int j = 0; j < count; j++) {
            e.key = ((int *) buffer)[2 * j];
            e.ptr = ((int *) buffer)[2 * j + 1];
            btree_insert(tree, e);
        }
    }
    fclose(dp);
#endif
    cout << "Build\t" << tracer.getRunTime() << endl;
    tracer.startTime();
    cout << "Flush\t" << tracer.getRunTime() << endl;

    /*e.key = MAX_KEY_VALUE;
    e.ptr = INVALID_PAGE;
    ENTRY_SET_FENCE(&e);
    btree_insert(tree, e);*/
    //btree_writeNextEntry(state, &e);

    btree_flush(state);
    bool suc = true;
    delete state;
    for (int i = 100; i >= 0; i -= 30) {
        e.key = i;
        e.ptr = -1;
        BlockNum num = btree_point_query(tree, i, &e);
        if (e.ptr != i) {
            cout << i << "<->" << e.key << "\t" << e.ptr << "\t" << num << endl;
            suc = false;
        }
    }
    cout << "Success: " << suc << endl;

    delete tree;
    return 0;
}