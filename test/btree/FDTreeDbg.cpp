//
// Created by Michael on 2018/12/13.
//

//
// Created by Michael on 2018/12/8.
//

#include <iostream>
#include <cassert>
#include "fdtree.h"
#include "buffer.h"
#include "error.h"
#include "utils.h"

using namespace std;

#define WARMUP (1 << 9)

#define NUM (1 << 16)

#define BSIZE 1024 * /*1024 * */128

#undef INFILE
#ifdef INFILE
#define DATAPATH "e:/kv.data"
#define PAGE_SIZE (1 << 12)
#endif

#define SHUFFLE true

#define REVERSE false

int ELOG_LEVEL = INFO;

int main(int argc, char **argv) {
    int k = 4;
    initBufPool(BSIZE, true);
    FDLevelState *state;
    cout << "Buffered" << endl;
    FDTree *tree = new FDTree;
    cout << "Created" << endl;
    fdtree_init(tree);
    {

        int nHeadTreeEntry = (HEADTREE_PAGES_BOUND - 1) * PAGE_MAX_NUM(Entry);
        tree->n = NUM;
        tree->k = k;
        tree->nLevel = (int) ceil(log(double(NUM) / nHeadTreeEntry) / log(double(k))) + 1;
        //tree->deamortizedMode = true;

        if (tree->nLevel < 2)
            tree->nLevel = 2;
        assert(tree->nLevel < MAX_LEVELS);
        state = fdtree_initWriteUpperLevels(tree, tree->nLevel - 1);
    }
    cout << "Inited level: " << tree->nLevel << endl;
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
        unsigned long long idx = ((unsigned long long) rand() * rand() + rand()) % NUM;
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
            cout << "Shuffle error" << endl;
            exit(-1);
        }
    }
#endif
    cout << "Insert" << endl;
    tracer.startTime();
#if REVERSE
    for (int i = NUM - 1; i >= 0; i--) {
#else
    for (int i = 0; i < WARMUP; i++) {
#endif
        e.key = input[i];
        e.ptr = input[i];
        ENTRY_SET_NON_FENCE(&e);
        ENTRY_SET_NON_FILTER(&e);
        fdtree_insert(tree, e);
        //fdtree_writeNextEntry(state, &e);
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

    /*e.key = input[NUM - 1];
    e.ptr = input[NUM - 1];
    fdtree_writeNextEntry(state, &e);*/

    fdtree_flush(state);
    fdtree_replaceLevels(tree, state, tree->nLevel - 1);

    //for (int i = NUM - 2; i >= WARMUP; i--) {
    for (int i = WARMUP; i < NUM - 2; i++) {
        e.key = input[i];
        e.ptr = input[i];
        ENTRY_SET_NON_FENCE(&e);
        ENTRY_SET_NON_FILTER(&e);
        fdtree_writeNextEntry(state, &e);
    }

    bool suc = true;
    for (int i = 100; i >= 0; i -= 30) {
        e.key = i;
        e.ptr = -1;
        BlockNum num = fdtree_point_search(tree, i, &e);
        if (e.ptr != i) {
            cout << i << "<->" << e.key << "\t" << e.ptr << "\t" << num << endl;
            suc = false;
        }
    }
    cout << "Success: " << suc << endl;

    delete tree;
    return 0;
}