#pragma once

#include <iostream>
#include <ctime>
#include <sys/time.h>
#include <string.h>
#include <math.h>

#define PMAXSIZE_EPS 1e-10

using namespace std;

class Tracer {
    timeval bt;

    timeval et;

    long dt;

public:
    void startTime() {
        gettimeofday(&bt, nullptr);
    }

    long getRunTime() {
        gettimeofday(&et, nullptr);
        dt = (et.tv_sec - bt.tv_sec) * 1000000 + et.tv_usec - bt.tv_usec;
        bt = et;
        return dt;
    }
};

void startTime();

long getRunTime();

long fetchTime();

int intcmp(int &a, int &b);