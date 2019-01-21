//
// Created by Michael on 2018/12/7.
//

#include <fstream>
#include <iostream>
#include "buffer.h"
#include "btree.h"
#include "error.h"
#include "fdtree.h"
#include "utils.h"

using namespace std;

#define NUM 100000000

#define BSZIE 1024 * 1024 * 32

int ELOG_LEVEL = INFO;

int main(int argc, char **argv) {
    Entry entry;
    BTreeState *state;
    BTree *tree = new BTree;
    initBufPool(BSZIE);
    state = btree_reuseRead(tree, true);
    Tracer tracer;
    tree->nEntry = NUM;
    cout << "begin search" << endl;
    tracer.startTime();
    for (int i = 0; i < 2; i++) {
        cout << i << endl;
        BlockNum num = btree_point_query(tree, i);
        if (num > 0) {
            cout << i << "<->" << num << endl;
        }
    }
    cout << tracer.getRunTime() << endl;

    delete state;
    delete tree;

    return 0;
}