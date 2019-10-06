//
// Created by iclab on 10/6/19.
//
#include <cassert>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include "utils.h"

using namespace std;

struct DummyHeap {
public:
    uint64_t size;
    uint8_t *buf;

public:
    DummyHeap(uint64_t size) : size(size), buf(new uint8_t[size]) {}

    DummyHeap(uint64_t size, bool copyOnWrite) : DummyHeap(size) {
        std::memset(buf, 0x80, size);
    }

    void Initialize(int size) {
        this->size = size;
        this->buf = new uint8_t[size];
    }

    ~DummyHeap() {
        delete[] buf;
    }
};

long bufferSize = (1LLU << 10);
long totalRounds = (1LLU << 20);
int threadNum = (1 << 2);
int needInit = 0;

DummyHeap **heap;

void DummyHeapCreate(int threadId) {
    heap[threadId] = static_cast<DummyHeap *>(malloc(sizeof(DummyHeap) * totalRounds / threadNum));
    for (long i = 0; i < totalRounds / threadNum; i++) {
        heap[threadId][i].Initialize(bufferSize);
        if (needInit > 0) std::memset(heap[threadId][i].buf, 0x80, bufferSize);
    }
}

void DummyHeapAccess(int threadId) {
    for (long i = 0; i < totalRounds / threadNum; i++) {
        for (int j = 0; j < bufferSize; j++) {
            heap[threadId][i].buf[j] = (uint8_t) i;
        }
    }
}

void DummyHeapDestroy(int threadId) {
    free(heap[threadId]);
}

int main(int argc, char **argv) {
    if (argc > 4) {
        bufferSize = std::atol(argv[0]);
        totalRounds = std::atol(argv[1]);
        threadNum = std::atoi(argv[2]);
        needInit = std::atoi(argv[3]);
    }
    heap = new DummyHeap *[threadNum];
    Tracer tracer;
    tracer.startTime();
    thread threads[threadNum];
    for (int i = 0; i < threadNum; i++) {
        threads[i] = thread(DummyHeapCreate, i);
    }
    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }
    cout << "Create time: " << tracer.getRunTime() << endl;

    for (int i = 0; i < threadNum; i++) {
        threads[i] = thread(DummyHeapAccess, i);
    }
    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }
    cout << "Access time: " << tracer.getRunTime() << endl;

    for (int i = 0; i < threadNum; i++) {
        threads[i] = thread(DummyHeapDestroy, i);
    }
    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }
    cout << "Destroy time: " << tracer.getRunTime() << endl;

    delete[] heap;
}