//
// Created by iclab on 10/6/19.
//
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include "utils.h"

using namespace std;

struct DummyHeap {
public:
    uint32_t size;
    bool onSet;
    uint8_t *buf;

public:
    /*DummyHeap() {}*/

    DummyHeap(uint64_t size) : size(size) {}

    DummyHeap(uint64_t size, bool onSet) : DummyHeap(size) { this->onSet = onSet; }

    void Initialize(int size) {
        this->size = size;
        this->buf = new uint8_t[size];
        if (onSet) std::memset(buf, 0x80, size);
    }

    ~DummyHeap() {
        delete[] buf;
    }
};

constexpr long maxAllocation = (1 << 20);

long bufferSize = (1LLU << 10);
long totalRounds = (1LLU << 20);
int threadNum = (1 << 2);
int needInit = 0;

DummyHeap ***heap;

void DummyHeapCreate(int threadId) {
    long total = totalRounds / threadNum;
    long round = total / maxAllocation;
    heap[threadId] = new DummyHeap *[round];
    for (long r = 0; r < round; r++) {
        heap[threadId][r] = static_cast<DummyHeap *>(malloc(sizeof(DummyHeap) * maxAllocation));
        /*heap[threadId][r] = new DummyHeap[maxAllocation];*/
        for (long i = 0; i < maxAllocation; i++) {
            heap[threadId][r][i].Initialize(bufferSize);
            if (needInit > 0) std::memset(heap[threadId][r][i].buf, 0x80, bufferSize);
        }
    }
}

void DummyHeapAccess(int threadId) {
    long total = totalRounds / threadNum;
    long round = total / maxAllocation;
    for (long r = 0; r < round; r++) {
        for (long i = 0; i < maxAllocation; i++) {
            for (int j = 0; j < bufferSize; j++) {
                heap[threadId][r][i].buf[j] = (uint8_t) i;
            }
        }
    }
}

void DummyHeapDestroy(int threadId) {
    long total = totalRounds / threadNum;
    long round = total / maxAllocation;
    for (long r = 0; r < round; r++) {
        for (long i = 0; i < maxAllocation; i++) heap[threadId][r][i].~DummyHeap();
        free(heap[threadId][r]);
        /*delete[] heap[threadId][r];*/
    }
    delete[] heap[threadId];
}

int main(int argc, char **argv) {
    if (argc > 4) {
        bufferSize = std::atol(argv[1]);
        totalRounds = std::atol(argv[2]);
        threadNum = std::atoi(argv[3]);
        needInit = std::atoi(argv[4]);
    }
    cout << "DotNetComp " << bufferSize << " " << totalRounds << " " << threadNum << " " << needInit << endl;
    heap = new DummyHeap **[threadNum];

    Tracer tracer;
    for (int r = 0; r < 6; r++) {
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
    }

    delete[] heap;
}