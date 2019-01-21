//
// Created by Michael on 2018/12/14.
//
#include <string.h>
#include <sys/time.h>
#include <iostream>
#include <random>
#include <time.h>
#include "../../src/lsmtree/bloom_filter.h"

#define NUM (1 << 30)

#define VERIFY false

using namespace std;

int main(int argc, char **argv) {
    bloom_parameters *bp = new bloom_parameters();
    bp->false_positive_probability = 0.01;
    bp->projected_element_count = NUM;
    bp->compute_optimal_parameters();
    bloom_filter *bf = new bloom_filter(*bp);
    bf->insert("ab");
    int *input = new int[NUM];
    for (int i = 0; i < NUM; i++) {
        input[i] = i;
    }
    for (int i = 0; i < NUM; i++) {
        unsigned long long idx = ((unsigned long long) rand() * rand() + rand()) % NUM;
        int tmp = input[i];
        input[i] = input[idx];
        input[idx] = tmp;
    }
#if VERIFY
    int *verify = new int[NUM];
    memset(verify, 0, NUM);
    for (int i = 0; i < NUM; i++) {
        verify[input[i]] = input[i];
    }
    for (int i = 0; i < NUM; i++) {
        if (i > 0 && verify[i] != verify[i - 1] + 1) {
            exit(-1);
        }
    }
#endif
    struct timeval tpstart, tpend;
    cout << "Begin" << endl;
    gettimeofday(&tpstart, NULL);
    for (int i = 0; i < NUM; i++) {
        //for (int i = NUM - 1; i >= 0; i--) {
        bf->insert(input[i]);
        //cout <<"\t\t"<< input[i]<<endl;
        if (i % (NUM / 100) == 0) {
            cout << ".";
        }
    }
    gettimeofday(&tpend, NULL);
    double timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    cout << endl << timeused << "\t" << bf->size() << endl;

    unsigned long long right = 0;
    unsigned long long wrong = 0;
    gettimeofday(&tpstart, NULL);
    for (int i = 0; i < NUM; i++) {
        if (bf->contains(input[i]) > 0) {
            right++;
        } else {
            cout << "\t" << i << endl;
            wrong++;
        }
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    cout << timeused << endl;
    cout << "False negatives: " << wrong << endl;
    cout << "True positive: " << right << endl;

    right = 0;
    wrong = 0;
    gettimeofday(&tpstart, NULL);
    for (int i = 0/*NUM*/; i < NUM/*(unsigned long long) NUM * 2*/; i++) {
        if (bf->contains(input[i] + NUM) > 0) {
            wrong++;
        } else {
            right++;
        }
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    cout << timeused << endl;
    if (bf->contains("cd") > 0) {
        wrong++;
    } else {
        right++;
    }
    cout << "False positives: " << wrong << endl;
    cout << "True negatives: " << right << endl;
    delete bf;
    delete bp;
    return 0;
}