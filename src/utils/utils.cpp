#include "utils.h"

timeval begTime;

timeval endTime;

long duration;

long counter;

void startTime() {
    gettimeofday(&begTime, nullptr);
}

long getRunTime() {
    gettimeofday(&endTime, nullptr);
    //cout << endTime.tv_sec << "<->" << endTime.tv_usec << "\t";
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    begTime = endTime;
    return duration;
}

long fetchTime() {
    cout << endTime.tv_sec << " " << endTime.tv_usec << endl;
    return duration;
}

int intcmp(int &a, int &b) {
    return a < b;
}