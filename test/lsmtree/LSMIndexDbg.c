//
// Created by Michael on 2018/12/13.
//

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "../../src/lsmtree/index.h"
#include "../../src/lsmtree/debug.h"
#include "../../src/lsmtree/util.h"

#define KSIZE (16)
#define VSIZE (20)
#define DATAPATH "./kv.data"
#define NUM (1 << 30)
#define PAGE_SIZE (1 << 16)
#define MAX_MTBL (7)
#define MAX_MTBL_SIZE (3000000)

int main() {
    int i, ret;
    char key[KSIZE];
    char val[VSIZE];
    int line = 0;
    struct slice sk, sv;
    FILE *dp = fopen(DATAPATH, "rb+");
    char buffer[PAGE_SIZE];
    int count = PAGE_SIZE / (2 * sizeof(int));
    int round = NUM / count;
    struct timeval tpstart, tpend;
    struct index *idx = index_new("lsm.idx", MAX_MTBL, MAX_MTBL_SIZE);
    gettimeofday(&tpstart, NULL);
    for (int i = 0; i < round; i++) {
        fread(buffer, PAGE_SIZE, 1, dp);
        for (int j = 0; j < count; j++) {
            memset(key, 0, 256);
            sprintf(key, "%d", ((int *) buffer)[2 * j]);
            snprintf(val, VSIZE, "val:%d", line++);

            sk.len = KSIZE;
            sk.data = key;
            sv.len = VSIZE;
            sv.data = val;

            ret = index_add(idx, &sk, &sv);
            if (!ret)
                    __DEBUG("%s", "Write failed....");
        }
    }
    gettimeofday(&tpend, NULL);
    double timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("\nTime: %lf\n", timeused);
    fclose(dp);
    return 0;
}