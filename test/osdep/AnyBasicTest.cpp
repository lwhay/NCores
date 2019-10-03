//
// Created by iclab on 9/25/19.
//

#include <iostream>
#include <any>
#include <vector>
#include <array>
#include "utils.h"

using namespace std;

constexpr long total_count(1LLU << 30);

void vectorUnits() {
    vector<any> units;
    vector<long> grounds;

    long counter = 0;

    Tracer tracer;

    tracer.startTime();
    any a;
    for (long l = 0; l < total_count; l++) {
        a = l;
        if (any_cast<long>(a) >= 0) counter++;
    }
    cout << "Any dummy:" << tracer.getRunTime() << ":" << counter << endl;

    tracer.startTime();
    for (long l = 0; l < total_count; l++) {
        units.push_back(int(l));
    }
    cout << "Any insert:" << tracer.getRunTime() << ":" << total_count << endl;

    tracer.startTime();
    units.clear();
    cout << "Any clear:" << tracer.getRunTime() << ":" << grounds.size() << endl;

    long ll;
    counter = 0;
    tracer.startTime();
    for (long l = 0; l < total_count; l++) {
        ll = l;
        if (ll >= 0) counter++;
    }
    cout << "Int dummy:" << tracer.getRunTime() << ":" << counter << endl;

    tracer.startTime();
    for (long l = 0; l < total_count; l++) {
        grounds.push_back(static_cast<int>(l));
    }
    cout << "Int insert:" << tracer.getRunTime() << ":" << total_count << endl;

    tracer.startTime();
    grounds.clear();
    cout << "Int clear:" << tracer.getRunTime() << ":" << grounds.size() << endl;
}


void arrayUnits() {
    array<any, total_count> units;

    Tracer tracer;

    tracer.startTime();
    for (long l = 0; l < total_count; l++) {
        units.at(l) = int(l);
    }
    array<any, total_count>::iterator iter = units.end();
    iter--;
    any a = units[total_count - 1];
    cout << "Any insert:" << tracer.getRunTime() << ":" << any_cast<int>(units[0]) << ":"
         << any_cast<int>(units[total_count - 1]) << endl;

    tracer.startTime();
    cout << "Any clear:" << tracer.getRunTime() << ":" << units.size() << endl;

    array<int, total_count> grounds;

    tracer.startTime();
    for (long l = 0; l < total_count; l++) {
        grounds.at(l) = (static_cast<int>(l));
    }
    cout << "Int insert:" << tracer.getRunTime() << ":" << grounds[total_count - 1] << endl;

    tracer.startTime();
    cout << "Int clear:" << tracer.getRunTime() << ":" << grounds.size() << endl;
}

int main(int argc, char **argv) {
    vectorUnits();
    //arrayUnits();
}
