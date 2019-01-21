//
// Created by Michael on 2018/12/13.
//

#include <cmath>
#include <iostream>
#include <stdlib.h>
#include <random>
#include "error.h"

#define DATAPATH "D://kv.data"

#define WORKLOADS_UNIFORM   0
#define WORKLOADS_RANDOM    1
#define WORKLOADS_ZIPFAN    2
#define WORKLOADS_VERIFY    3

#define TOTAL_POINTS        (1 << 30)

#define PAGE_SIZE           (1 << 12)

#define MAX_VERIFY_SIZE     (1 << 10)

using namespace std;

const int workloads_type = WORKLOADS_RANDOM;

void genUniformWorkloads() {

}

int ELOG_LEVEL = INFO;

void genRandomWorkloads() {
    int *loads = new int[TOTAL_POINTS];
    cout << PAGE_SIZE << "\t" << TOTAL_POINTS << endl;
    for (int i = 0; i < TOTAL_POINTS; i++) {
        loads[i] = i;
    }
    cout << "Gen:" << endl;
    for (int i = 0; i < TOTAL_POINTS; i++) {
        unsigned long long idx = abs(rand() * rand() * rand() + rand() * rand() + rand()) % TOTAL_POINTS;
        if (idx < 0 || idx >= TOTAL_POINTS) {
            cout << "\t" << idx << endl;
            exit;
        }
        int tmp = loads[i];
        loads[i] = loads[idx];
        loads[idx] = tmp;
    }
    int count = PAGE_SIZE / (2 * sizeof(int));
    int round = TOTAL_POINTS / count;
    char buffer[PAGE_SIZE];
    cout << count << "\t" << round << endl;
    FILE *fp = fopen(DATAPATH, "wb+");
    for (int i = 0; i < round; i++) {
        for (int j = 0; j < count; j++) {
            ((int *) buffer)[2 * j] = loads[i * count + j];
            ((int *) buffer)[2 * j + 1] = i * count + j;
        }
        fwrite(buffer, PAGE_SIZE, 1, fp);
    }
    fclose(fp);
}

void genZipfanWorkloads() {

}

void workloadsVerfify() {
    int count = PAGE_SIZE / (2 * sizeof(int));
    int round = TOTAL_POINTS / count;
    char buffer[PAGE_SIZE];
    int vc = 0;
    FILE *fp = fopen(DATAPATH, "rb+");
    for (int i = 0; i < round; i++) {
        fread(buffer, PAGE_SIZE, 1, fp);
        for (int j = 0; j < count; j++) {
            if (j == 0 || j == (count - 1)) {
                cout << ((int *) buffer)[2 * j + 1] << "\t";
                if (vc++ > MAX_VERIFY_SIZE) {
                    goto jumpout;
                }
            }
        }
    }
    jumpout:
    fclose(fp);
}

int main() {
    switch (workloads_type) {
        case WORKLOADS_UNIFORM:
            genUniformWorkloads();
            break;
        case WORKLOADS_RANDOM:
            genRandomWorkloads();
            break;
        case WORKLOADS_ZIPFAN:
            genZipfanWorkloads();
            break;
        case WORKLOADS_VERIFY:
            workloadsVerfify();
            break;
        default:
            break;
    }
}