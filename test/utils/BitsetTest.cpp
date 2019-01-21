//
// Created by Michael on 2018/12/2.
//

#include <iostream>
#include <bitset>
#include <boost/dynamic_bitset.hpp>
#include "Bitset.h"
#include "utils.h"

using namespace std;

//#define CAPACITY ((1L << 30) * (1L << 10))

#define SKIPSTEP 256

void stdBitSet() {
    const unsigned long long CAPACITY = (1L << 31) - 1;
    bitset<CAPACITY> bs;
    Tracer tracer;
    tracer.startTime();
    //bs.reset();
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (i % SKIPSTEP == 0) {
            bs.set(i);
        } else {
            bs.set(i, false);
        }
    }
    cout << "std bitset: " << tracer.getRunTime() << "\t" << bs.size() /*<< "\t" << bs.count()*/ << endl;
}

void boostBitSet() {
    const unsigned long long CAPACITY = (1L << 31) - 1;
    boost::dynamic_bitset<> bdbs(CAPACITY);
    Tracer tracer;
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (i % SKIPSTEP == 0) {
            bdbs.set(i);
        }
    }
    cout << "boost bitset: " << tracer.getRunTime() << "\t" << bdbs.size() /*<< "\t" << bs.count()*/ << endl;
}

void coresBitSet() {
    const unsigned long long CAPACITY = (1L << 31) - 1;
    Bitset cbs(CAPACITY);
    Tracer tracer;
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (i % SKIPSTEP == 0) {
            cbs.set(i);
        }
    }
    cout << "cores bitset: " << tracer.getRunTime() << "\t" << cbs.size() /*<< "\t" << bs.count()*/ << endl;
}

int main(int argc, char **argb) {
    const unsigned long long CAPACITY = (1L << 31) - 1;
    //const unsigned long long CAPACITY = ((unsigned long long) (1L << 30) * (1L << 6));
    cout << (1 << 2) << "\t" << (256 >> 2) << endl;
    //stdBitSet();
    //boostBitSet();
    //coresBitSet();
    Bitset *bsp = new Bitset(CAPACITY);
    Bitset *vsp = new Bitset(CAPACITY);
    Tracer tracer;
    tracer.startTime();
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (i % SKIPSTEP == 0) {
            bsp->set(i);
        } else {
            vsp->set(i);
        }
    }
    cout << bsp->size() << "\t" << tracer.getRunTime() << endl;
    unsigned long long next = 0;
    unsigned long long found = 0;
    tracer.startTime();
    while (next <= vsp->validity()) {
        next = vsp->nextSetBit(next);
        next++;
        found++;
    }
    cout << found << "\t" << tracer.getRunTime() << "\t" << (vsp->capacity() * SKIPSTEP) << endl;
    unsigned long long setbit = 0;
    tracer.startTime();
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (vsp->get(i)) {
            setbit++;
        }
    }
    cout << setbit << "\t" << setbit * SKIPSTEP << "\t" << tracer.getRunTime() << "\t" << (vsp->capacity() * SKIPSTEP)
         << endl;
    delete vsp;

    next = 0;
    found = 0;
    tracer.startTime();
    while (next <= bsp->validity()) {
        next = bsp->nextSetBit(next);
        next++;
        found++;
    }
    cout << found << "\t" << tracer.getRunTime() << "\t" << (bsp->capacity() * SKIPSTEP) << endl;
    setbit = 0;
    tracer.startTime();
    for (unsigned long long i = 0; i < CAPACITY; i++) {
        if (bsp->get(i)) {
            setbit++;
        }
    }
    cout << setbit << "\t" << setbit * SKIPSTEP << "\t" << tracer.getRunTime() << "\t" << (bsp->capacity() * SKIPSTEP)
         << endl;
    delete bsp;

    Bitset rbs(CAPACITY);

    tracer.startTime();
    for (unsigned long long begin = 0; begin < rbs.size(); begin += SKIPSTEP) {
        rbs.setRange(begin, begin + SKIPSTEP / 2);
    }
    cout << tracer.getRunTime() << "\t" << rbs.size() << "\t" << rbs.capacity() << endl;

    const unsigned long long TINY = 64;
    Bitset *tbsp = new Bitset(TINY);
    tbsp->setRange(2, 14);
    for (unsigned long long i = 0; i < TINY; i++) {
        cout << i << "\t" << tbsp->get(i) << endl;
    }
    cout << tbsp->getLong(0) << endl;
    delete tbsp;
}