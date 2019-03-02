//
// Created by Michael on 2018/12/2.
//
#include <iostream>
#include "BlockCache.h"
#include "utils.h"

using namespace std;

#define BLOCK_COUNT 1024

#define BLOCK_LIMIT 1024

#define FILTERING false

class Dummy {
    int _id;
    int *_cache;
public:
    Dummy(int id) : _id(id) {
        std::cout << "constructor " << _id << "\n";
        _cache = new int[BLOCK_LIMIT];
    }

    ~Dummy() {
        std::cout << "destructor " << _id << "\n";
        delete[] _cache;
    }
};

int classMallocTest() {
    Dummy a(1);
    Dummy *pa = new Dummy(2);
    delete pa;
    return 0;
}

int fileRWTest() {
    int *cache = new int[BLOCK_LIMIT];
    FILE *fp = fopen("./test.dat", "wb+");
    for (int k = 0; k < BLOCK_COUNT; k++) {
        for (int i = 0; i < BLOCK_LIMIT; i++) {
            cache[i] = k * BLOCK_LIMIT + i;
        }
        fwrite(cache, sizeof(int), BLOCK_LIMIT, fp);
    }
    delete[] cache;
    fflush(fp);
    fclose(fp);

    cache = new int[BLOCK_LIMIT];
    fp = fopen("./test.dat", "rb+");
    int k = 0;
    int i = 0;
    for (k = 0; k < BLOCK_COUNT; k++) {
        fread(cache, sizeof(int), BLOCK_LIMIT, fp);
        for (i = 0; i < BLOCK_LIMIT; i++) {
            if (cache[i] != k * BLOCK_LIMIT + i) {
                printf("Unexpected: %d\n", k * BLOCK_LIMIT + i);
            }
        }
    }
    delete[] cache;
    fclose(fp);
    printf("FileRWTest succeeds by: k = %d i = %d\n", k, i);
}

int blockCacheTest() {
    FILE *fp = fopen("./test.dat", "w+");
    PrimitiveBlock<int> *intBlock = new PrimitiveBlock<int>(fp, 0L, 0, BLOCK_LIMIT);
    unsigned long long total = 0;
    Tracer tracer;
    tracer.startTime();
    for (int k = 0; k < BLOCK_COUNT; k++) {
        for (int i = 0; i < BLOCK_LIMIT; i++) {
            intBlock->set(i, k * BLOCK_LIMIT + i);
            total += k * BLOCK_LIMIT + i;
        }
        intBlock->appendToFile();
    }
    cout << "Ingestion: " << tracer.getRunTime() << endl;
    fflush(fp);
    fclose(fp);
    delete intBlock;

    fp = fopen("./text.dat", "rb+");
    unsigned long long verify = 0;
    intBlock = new PrimitiveBlock<int>(fp, 0L, 0, BLOCK_LIMIT);
    tracer.startTime();
    unsigned long long count = 0;
    for (int k = 0; k < BLOCK_COUNT; k++) {
        unsigned long long filter = k * BLOCK_LIMIT + BLOCK_LIMIT / 2;
        intBlock->loadFromFile();
        int i = 0;
        for (i = 0; i < BLOCK_LIMIT; i++) {
            if (FILTERING) {
                if (intBlock->get(i) > filter) {
                    count++;
                }
            } else {
                //printf("\t\t%d\n", intBlock->get(i));
                verify += intBlock->get(i);
            }
        }
        //printf("< v = %d k = %d i = %d\n", intBlock->get(0), k, i);
    }
    intBlock->loadFromFile();
    cout << "Load: " << tracer.getRunTime() << "\t" << total << "<->" << count << "<->" << verify << endl;
    delete intBlock;
    fclose(fp);
}

int main(int argc, char **argv) {
    classMallocTest();
    fileRWTest();
    blockCacheTest();
    return 0;
}
