//
// Created by Michael on 2018/12/6.
//

#include <iostream>
#include <string.h>
#include "utils.h"
#include <assert.h>

using namespace std;

constexpr unsigned long long memsize = 1048576;

constexpr int pagesize = 2048;

int main(int argc, char **argv) {
    Tracer tracer;
    tracer.startTime();
    unsigned long long *src = new unsigned long long[memsize * pagesize];
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    unsigned long long *des = new unsigned long long[memsize * pagesize];
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    memset(src, 192, memsize * pagesize * sizeof(unsigned long long));
    cout << tracer.getRunTime() << endl;
    unsigned long long key = 0;
    for (int i = 0; i < sizeof(unsigned long long); i++) {
        key |= 192 << (8 * i);
    }
    for (unsigned long long idx = 0; idx < memsize * pagesize; idx++) {
        assert(src[idx] == key);
        if (idx == 999) {
            cout << "\t" << src[idx] << "<->" << key << endl;
        }
    }
    tracer.startTime();
    memcpy(src, des, memsize * pagesize * sizeof(unsigned long long));
    cout << tracer.getRunTime() << endl;
    for (unsigned long long idx = 0; idx < memsize * pagesize; idx++) {
        assert(des[idx] == key);
        if (idx == 999999) {
            cout << "\t" << des[idx] << "<->" << key << endl;
        }
    }
}
