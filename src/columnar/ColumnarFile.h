//
// Created by Michael on 2019/2/17.
//
#pragma once

#include <cassert>
#include <vector>
#include <string>
#include <unordered_map>
#include <stdio.h>
#include "CoresBlock.h"
#include "BlockManager.h"
#include "Columnar.h"
#include "ColumnarHeader.h"

using namespace std;

class ColumnarFile {
    string filepath;
    FILE *fp;
    BlockManager *bm;
    ColumnarHeader header;
    // Temporary metamap will be replaced by Metadata manager.
    unordered_map<string, int> indexMap;
    unordered_map<int, string> nameMap;
    vector<Columnar<size_t>> columns;
public:
    ColumnarFile(string path, BlockManager *blockManager) : filepath(path), bm(blockManager) {}

    void create() {
        assert(filepath != null);
        /*if (path) {
            filepath = path;
        }*/
        fp = fopen(filepath.c_str(), "+wb");
    }

    void open() {
        assert(filepath != null);
        /*if (path) {
            filepath = path;
        }*/
        fp = fopen(filepath.c_str(), "+rb");
    }
};