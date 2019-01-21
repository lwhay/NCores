//
// Created by Michael on 2018/12/4.
//

#include <iostream>
#include "lz4Codec.h"

#define DEFAULT_BLOCK_SIZE 16384

using namespace std;

int main(int garc, char **argv) {
    char *buf = new char[DEFAULT_BLOCK_SIZE];
    LZ4Codec lz4c(buf);
    char *cbuf = lz4c.code();
    cout << lz4c.rawsize() << "<->" << lz4c.codecsize() << endl;
    return 0;
}