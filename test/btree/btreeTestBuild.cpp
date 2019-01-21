//
// Created by Michael on 2018/11/30.
//
#include <iostream>
#include <cmath>
#include <unordered_set>
#include "SerialBTree.h"
#include "cache/LSMCache.h"
#include "utils.h"

#define DEFAULT_TEST_COUNT 10000000//0

using namespace std;

extern int DIM;

int main(int argc, char **argv) {
    DIM = 4;
    cout << "begin" << endl;
    Tracer tracer;
    tracer.startTime();
    SerialBTree *btree = new SerialBTree("./test.idx", new Cache(10000, 16384)/*, false*/);
    KEY_TYPE *keys = new KEY_TYPE[DEFAULT_TEST_COUNT];
    for (int i = 0; i < DEFAULT_TEST_COUNT; i++) {
        if (i % 1000000 == 0) {
            cout << i << "\t" << tracer.getRunTime() << endl;
        }
        BTreeLEntry *e = new BTreeLEntry();
        keys[i] = e->key = i;
        e->rid = i;
        e->record.coord[0] = rand();
        e->record.coord[1] = rand();
        e->record.coord[2] = rand();
        e->record.coord[3] = rand();
        btree->insert(e->rid, e->key, &e->record);
        //cout << e->rid << "\t" << e->key << endl;
        delete e;
    }
    btree->cache->flush();
    btree->traverse();
    delete[] keys;
    delete btree;
    cout << tracer.getRunTime() << endl;
    return 0;
}