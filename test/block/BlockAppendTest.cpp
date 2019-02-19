//
// Created by Michael on 2019-02-18.
//

#include <iostream>
#include <utils.h>
#include "BlockManager.h"
#include <ColumnarFile.h>

#define TOTAL_NUMBER (1L << 10)

int main() {
    BlockManager *bm = new BlockManager();
    ColumnarFile *bc = new ColumnarFile("../test.dat", bm);
    return 0;
}