//
// Created by Michael on 2019/2/17.
//
#include <iostream>
#include "BlockManager.h"

using namespace std;

int main() {
    BlockManager bm;
    cout << "block_size=" << bm.block_size << endl;
    cout << "prefetch_size=" << bm.prefetch_size << endl;
}