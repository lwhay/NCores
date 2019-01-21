//
// Created by Michael on 2018/12/5.
//

#include <iostream>
#include <emmintrin.h>
#include <nmmintrin.h>
#include <wmmintrin.h>
#include <random>
#include <vector>
#include <unordered_set>
#include "utils.h"
#include <HashSet.h>

#define DEFAULT_ARRAY_SIZE 1073741824

using namespace std;

inline void sandbox() {
    //volatile uint32_t x = _mm_crc32_u32(0, 0);
    const auto a = _mm_set_epi64x(0, 0);
    const auto b = _mm_set_epi64x(0, 0);
    const auto c = _mm_clmulepi64_si128(a, b, 0x00);
    auto d = _mm_cvtsi128_si64(c);
}

inline void seed(bool switcher) {
    if (!switcher) {
        return;
    }
    vector<int> distinct;
    while (true) {
        int v = rand() % 16;
        bool found = false;
        for (int d:distinct) {
            if (d == v) {
                found = true;
            }
        }
        if (!found) {
            distinct.push_back(v);
        }
        if (distinct.size() == 16) {
            break;
        }
    }
    for (int d: distinct) {
        cout << d << endl;
    }
}

void hashEfficiency() {
    HashSet<unsigned long long> *hashSet = new HashSet<unsigned long long>();
    for (int i = 0; i < 256; i++) {
        int r = rand();
        hashSet->add(r);
    }
    delete hashSet;
    Tracer tracer;
    tracer.startTime();
    vector<unsigned long long> array;
    for (int i = 0; i < DEFAULT_ARRAY_SIZE / 4; i++) {
        unsigned long long r =
                ((rand() * rand() + rand()) * rand() + rand()) * rand() + (rand() * rand() + rand()) * rand() +
                rand() * rand() + rand();
        array.push_back(r);
    }
    cout << tracer.getRunTime() << "\t" << sizeof(void **) << endl;
    hashSet = new HashSet<unsigned long long>();
    tracer.startTime();
    for (unsigned long long v:array) {
        hashSet->add(v);
    }
    cout << tracer.getRunTime() << endl;
    delete hashSet;
    unordered_set<unsigned long long> hash;
    tracer.startTime();
    for (unsigned long long v:array) {
        hash.insert(v);
    }
    cout << tracer.getRunTime() << endl;
    cout << "***************************************************************" << endl << endl;
}

int main(int argc, char **argv) {
    seed(false);
    sandbox();
    hashEfficiency();
    Tracer tracer;
    tracer.startTime();
    double *input = new double[DEFAULT_ARRAY_SIZE];
    cout << "New: " << tracer.getRunTime() << endl;
    tracer.startTime();
    memset(input, 0, DEFAULT_ARRAY_SIZE * sizeof(double));
    cout << "Init: " << tracer.getRunTime() << endl;
    tracer.startTime();
    double max = .0;
    for (int i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        input[i] = (double) rand() / 32768;
        if (input[i] > max) {
            max = input[i];
        }
    }
    cout << "Generate: " << tracer.getRunTime() << "\t" << sizeof(double) << "\t" << max << endl;
    unsigned int satisfied[10];
    memset(satisfied, 0, sizeof(satisfied));
    for (int i = 0; i < 10; i++) {
        cout << "\t" << i << "\t" << satisfied[i] << "\t" << sizeof(satisfied) << endl;
    }
    unsigned int filtered = 0;
    double filter = 0.2;
    tracer.startTime();
    for (int i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        if (input[i] < filter) {
            filtered++;
        }
    }
    cout << "Filter: " << tracer.getRunTime() << "\t" << filtered << endl;
    tracer.startTime();
    for (int i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        int idx = (int) (input[i] / 0.1);
        satisfied[idx]++;
    }
    cout << "Group: " << tracer.getRunTime() << endl;
    unsigned total = 0;
    for (int i = 0; i < 10; i++) {
        cout << "\t" << satisfied[i] << endl;
        total += satisfied[i];
    }
    cout << total << endl;
    unordered_set<double> diffset;
    tracer.startTime();
    for (int i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        diffset.insert(input[i]);
    }
    cout << "Set: " << tracer.getRunTime() << "\t" << diffset.size() << endl;

    input[0];
}