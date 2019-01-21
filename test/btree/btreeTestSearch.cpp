//
// Created by Michael on 2018/11/30.
//
#include <iostream>
#include <cmath>
#include <unordered_set>
#include "SerialBTree.h"
#include "cache/LSMCache.h"
#include "utils.h"

#define DEFAULT_TEST_COUNT 10000//000
#define SKIP_GRAN 10//000

using namespace std;

extern int DIM;

int main(int argc, char **argv) {
    DIM = 4;
    cout << "begin" << endl;
    Tracer tracer;
    tracer.startTime();
    BTreeLEntry *rs = new BTreeLEntry[DEFAULT_TEST_COUNT];
    SerialBTree *btree = new SerialBTree("./test.idx", new Cache(0, 16384)/*, true*/);
    for (int i = 0; i < DEFAULT_TEST_COUNT; i++) {
        KEY_TYPE lowkey = i;
        KEY_TYPE highkey = i;
        rs[i].rid = -1;
        btree->search(lowkey, highkey, &rs[i]);
        cout << i << "\t" << rs[i].rid << endl;
    }
    cout << tracer.getRunTime() << endl;
    delete[] rs;
    delete btree;
}