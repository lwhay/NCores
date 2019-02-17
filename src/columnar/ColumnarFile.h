//
// Created by Michael on 2019/2/17.
//
#pragma once

#include <cassert>
#include <vector>
#include <string>
#include "Columnar.h"
#include "ColumnarHeader.h"

using namespace std;

class ColumnarFile {
    string filepath;
    File *fp;
    ColumnarHeader header;
    vector<Columnar<type>> columns;
public:
    ColumnarFile(string path) : filepath(path) {}

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