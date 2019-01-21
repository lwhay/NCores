//
// Created by Michael on 2018/12/5.
//

#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_set>
#include "buffer.h"
#include "btree.h"
#include "error.h"
#include "fdtree.h"
#include "utils.h"
#include "FileOperations.h"

using namespace std;

#define NUM 1000000000

#define BSZIE 1000

#define VERIFY true

#define SHUFFLE true

int b = 1024 * 1024 * 64;

int ELOG_LEVEL = INFO;

int main(int argc, char **argv) {
    cout << "Init" << endl;
    initBufPool(b);
    Entry entry;
    //file_clearDataDir();
    BTreeState *state;
    BTree *tree = new BTree;
    state = btree_initWrite(tree, true);
    cout << "Built" << endl;
    Tracer tracer;
    int *input = new int[NUM];
    for (int i = 0; i < NUM; i++) {
        input[i] = i;
    }
    if (SHUFFLE) {
        int tmp;
        for (int i = 0; i < NUM; i++) {
            int left = i;//(rand() * 3100 + rand()) % NUM;
            int right = (rand() * 3100 + rand()) % NUM;
            tmp = input[left];
            input[left] = input[right];
            input[right] = tmp;
            //swap(input[left], input[right]);
        }
        int *verify = new int[NUM];
        memset(verify, 0, NUM);
        cout << "Input verify" << endl;
        for (int i = 0; i < NUM; i++) {
            verify[input[i]] = input[i];
        }
        for (int i = 0; i < NUM; i++) {
            if (verify[i] != i) {
                cout << i << "<->" << verify[i] << endl;
            }
        }
        /*unordered_set<int> verify;
        for (int i = 0; i < NUM; i++) {
            verify.insert(input[i]);
        }
        cout << verify.size() << "\t" << input[0] << "\t" << input[1000] << endl;*/
    }
    cout << "Ready" << endl;
    tracer.startTime();
    //for (int i = NUM; i >= 0; i--) {
    for (int i = 0; i < NUM; i++) {
        entry.key = input[i];
        entry.ptr = input[i];//rand() % 100;
        btree_writeNextEntry(state, &entry, 0.7);
    }
    delete[] input;
    cout << tracer.getRunTime() << endl;

    entry.key = MAX_KEY_VALUE;
    entry.ptr = INVALID_PAGE;
    ENTRY_SET_FENCE(&entry);
    btree_writeNextEntry(state, &entry);

    btree_flush(state);

    tree->nEntry = NUM;

    delete state;

    cout << "begin search" << endl;
    bigint qcount = 0;
    int pqueries[NUM / 100000000];
    int idx = 0;
    for (int i = 0; i < NUM; i++) {
        if (VERIFY && (i % 100000000 == 0)) {
            pqueries[idx++] = i;
            BlockNum num = btree_point_query(tree, i, &entry);
            if (entry.ptr != i) {
                cout << i << "<->" << entry.key << "-" << entry.ptr << endl;
            }
            qcount++;
        }
    }
    cout << qcount << "\t" << tracer.getRunTime() << endl;
    cout << "range search" << endl;
    int rqueries[NUM / 100000000];
    idx = 1;
    qcount = 0;
    tracer.startTime();
    for (int i = 1; i < NUM; i++) {
        if (VERIFY && (i % 100000000 == 0)) {
            rqueries[idx++] = i;
            int num = btree_range_query(tree, i - 100, i);
            if (num != 101) {
                cout << i << "<->" << num << endl;
            }
            qcount++;
        }
    }
    /*int tmp;
    for (int i = 0; i < idx; i++) {
        int left = i;
        int right = (rand() * 3100 + rand()) % idx;
        tmp = queries[left];
        queries[left] = queries[right];
        queries[right] = tmp;
    }
    tracer.startTime();
    for (bigint n : queries) {
        BlockNum num = btree_point_query(tree, n, &entry);
    }*/
    cout << qcount << "\t" << tracer.getRunTime() << endl;
    delete tree;
    return 0;
}