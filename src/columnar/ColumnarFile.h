//
// Created by Michael on 2019/2/17.
//
#pragma once

#include <cassert>
#include <vector>
#include <string>
#include <unordered_map>
#include "CoresBlock.h"
#include "BlockManager.h"
#include "Columnar.h"
#include "ColumnarHeader.h"

using namespace std;

class ColumnarFile {
    string filepath;
    File *fp;
    BlockManager *bm;
    ColumnarHeader header;
    // Temporary metamap will be replaced by Metadata manager.
    unordered_map<string, int> indexMap;
    unordered_map<int, string> nameMap;
    vector<Columnar<type>> columns;
public:
    ColumnarFile(string path, const BlockManager *blockManager) : filepath(path), bm(blockManager) {}

    void create() {
        assert(filepath != null);
        if (path) {
            filepath = path;
        }
        fp = fopen(filepath, "+wb");
    }

    void open() {
        assert(filepath != null);
        if (path) {
            filepath = path;
        }
        fp = fopen(filepath, "+rb");
    }
};