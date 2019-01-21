//
// Created by Michael on 2018/12/5.
//

#include <algorithm>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <random>
#include "utils.h"
#include "FileOperations.h"

const int blocksize = 4096;

const int blocknumber = 10485760;

const int pseudoticks = 50000;

const char path[255] = "f://io.dat";

using namespace std;

void writreFile() {
    FILE *fp = fopen(path, "wb+");
    char buf[blocksize];
    memset(buf, 0, blocksize);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < blocknumber; i++) {
        memset(buf, i % 255, blocksize);
        fwrite(buf, blocksize, 1, fp);
    }
    cout << tracer.getRunTime() << endl;
    fclose(fp);
}

void sequentialRead() {
    FILE *fp = fopen(path, "rb+");
    char buf[blocksize];
    memset(buf, 0, blocksize);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < blocknumber; i++) {
        fread(buf, blocksize, 1, fp);
        if (i % 1048576 == 0 || i % 1048571 == 0) {
            cout << "\t" << (int) (buf[0]) << ":" << (int) (buf[1]) << endl;
        }
    }
    cout << tracer.getRunTime() << endl;
    fclose(fp);
}

void randomRead(bool sorted) {
    vector<unsigned long long> ticks;
    for (int i = 0; i < pseudoticks; i++) {
        //Posed issues in case of 40GB.
        ticks.push_back(rand() % 1000 * 10000 + rand());
    }
    char buf[blocksize];
    FILE *fp = fopen(path, "rb+");
    if (sorted) {
        sort(ticks.begin(), ticks.end(), less<int>());
    }
    Tracer tracer;
    tracer.startTime();
    for (unsigned long long tick : ticks) {
        if (bigseek(fp, tick * blocksize, SEEK_SET) == 0) {
            //cout << tick * blocksize << endl;
            fread(buf, blocksize, 1, fp);
        }
    }
    cout << tracer.getRunTime() << endl;
    fclose(fp);
}

int main(int argc, char **argv) {
    //writreFile();     // 40GB             4GB
    //sequentialRead(); // 600s-hdd (*)     60s
    randomRead(true); // 40s-hdd          50s
    //sequentialRead(); // 400s-hdd (*)
    //randomRead(false);// 500s-hdd         500s
    return 0;
}