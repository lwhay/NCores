//
// Created by Michael on 2018/12/14.
//
#include <sys/time.h>
#include <string.h>
#include "../../src/lsmtree/blossom.h"

#define NUM (1 << 30)

#define STRLEN 16

int main(int argc, char **agrv) {
    int *input = (int *) malloc(sizeof(int) * NUM);
    for (int i = 0; i < NUM; i++) {
        input[i] = i;
    }
    for (int i = 0; i < NUM; i++) {
        unsigned long long idx = ((unsigned long long) rand() * rand() + rand()) % NUM;
        int tmp = input[i];
        input[i] = input[idx];
        input[idx] = tmp;
    }
    struct timeval tpstart, tpend;
    char buf[STRLEN];
    printf("Begin\n");
    bloom_t *bloom = bloom_create(NUM, 0.01);
    gettimeofday(&tpstart, NULL);
    for (int i = 0; i < NUM; i++) {
        memset(buf, 0, STRLEN);
        sprintf(buf, "$d", input[i]);
        bloom_add(bloom, buf, sizeof(buf));
    }
    gettimeofday(&tpend, NULL);
    double timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("%d %llu\n", timeused, bloom->size);
    unsigned long long right = 0;
    unsigned long long wrong = 0;
    gettimeofday(&tpstart, NULL);
    for (int i = 0; i < NUM; i++) {
        memset(buf, 0, STRLEN);
        sprintf(buf, "$d", input[i]);
        if (bloom_get(bloom, buf, sizeof(buf)) > 0) {
            right++;
        } else {
            wrong++;
        }
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("%d %llu\n", timeused, bloom->size);
    printf("Right positives: %d, Wrong negatives: %d\n", right, wrong);
    right = 0;
    wrong = 0;
    gettimeofday(&tpstart, NULL);
    for (int i = NUM; i < (unsigned long long) NUM * 2; i++) {
        memset(buf, 0, STRLEN);
        sprintf(buf, "$d", i);
        if (bloom_get(bloom, buf, sizeof(buf)) > 0) {
            right++;
        } else {
            wrong++;
        }
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("%d %llu\n", timeused, bloom->size);
    printf("Wrong positives: %d, Right negatives: %d\n", right, wrong);
    return 0;
}