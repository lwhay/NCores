//
// Created by Michael on 2018/12/14.
//

#include <algorithm>
#include <iostream>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include "utils.h"
#include "FileOperations.h"
#include <pthread.h>

#define HDDPATH "i:/io.dat"
#define HDDSECD "j:/io.dat"
#define HDDTHRD "k:/io.dat"
#define HDDFORD "l:/io.dat"
#define MULTIPLE 0

#define SSDPATH "f:/io.dat"

#define INITFILE false

#define USINGSSD 0

#define THREADNUM 8

#define MAX_OFFSET (1LL << 33)

#define TOTAL_PAGES (1 << 20)

#define PAYLOAD_CNT (TOTAL_PAGES / THREADNUM)

#define PAGE_SIZE (1 << 12)

#define TOTAL_CNT (MAX_OFFSET / sizeof(bigint))

#define INNER_CNT (PAGE_SIZE / sizeof(bigint))

#define PARTITION true //Parallel writing both single file and multiple files; Otherwise parallel writing single files.

#define SEPARATE true //Parallel writing multiple files

#define PSEUDOWRITE 10

#define SORTOPS false

#define CHECKDUP true

using namespace std;

string path;

atomic_ullong counter;

atomic_ullong duplication;

atomic_ullong totalclicks;

unsigned long long at = 1;

struct payload {
    char data[PAGE_SIZE];
    bigint offset;
};

struct threadload {
    bool type;
    int id;
    struct payload *load;
};

bool payloadCmp(const struct payload &a, const struct payload &b) {
    return a.offset < b.offset;
}

void initFile(bool usingssd) {
#if SEPARATE
    for (int i = 0; i < THREADNUM; i++) {
        char path[245];
        if (usingssd) {
            sprintf(path, "%s%d", SSDPATH, i);
        } else {
            sprintf(path, "%s%d", HDDPATH, i);
        }
        bigint buf[INNER_CNT];
        bigint count = TOTAL_CNT / INNER_CNT / THREADNUM;
        FILE *fp = fopen(path, "wb+");
        bigint val = 0;
        for (bigint i = 0; i < count; i++) {
            for (int j = 0; j < INNER_CNT; j++) {
                buf[j] = val++;
            }
            fwrite(buf, PAGE_SIZE, 1, fp);
        }
        fflush(fp);
        fclose(fp);
    }
#else
    if (usingssd) {
        path = SSDPATH;
    } else {
        path = HDDPATH;
    }
    bigint buf[INNER_CNT];
    bigint count = TOTAL_CNT / INNER_CNT;
    FILE *fp = fopen(path.c_str(), "wb+");
    bigint val = 0;
    for (bigint i = 0; i < count; i++) {
        for (int j = 0; j < INNER_CNT; j++) {
            buf[j] = val++;
        }
        fwrite(buf, PAGE_SIZE, 1, fp);
    }
    fflush(fp);
    fclose(fp);
#endif
}

struct payload *generate(bool sorted, bigint offset = 0) {
    struct payload *loads = new struct payload[PAYLOAD_CNT];
    for (int i = 0; i < PAYLOAD_CNT; i++) {
        //Align with PAGE_SIZE
        bigint r;
#if PARTITION
#if SEPARATE
        bigint partrange = MAX_OFFSET / THREADNUM;
        r = ((bigint) ((bigint) rand() * rand() * rand() + rand() * rand() + rand()) % partrange) / PAGE_SIZE *
            PAGE_SIZE;
#else
        bigint partrange = MAX_OFFSET / THREADNUM;
        bigint partstart = offset * partrange;
        r = ((bigint) ((bigint) rand() * rand() * rand() + rand() * rand() + rand() + offset * PAGE_SIZE) %
             partrange) / PAGE_SIZE * PAGE_SIZE + partstart;
#endif
#else
        r = ((bigint) ((bigint) rand() * rand() * rand() + rand() * rand() + rand() + offset * PAGE_SIZE) %
                    MAX_OFFSET) / PAGE_SIZE * PAGE_SIZE;
#endif
        loads[i].offset = r;
        for (int j = 0; j < INNER_CNT; j++) {
            ((bigint *) loads[i].data)[j] = r + j;
        }
    }
    if (sorted) {
        sort(loads, loads + PAYLOAD_CNT, payloadCmp);
#if CHECKDUP
        bigint prevPos = 0;
        for (int i = 0; i < PAYLOAD_CNT; i++) {
            if (prevPos == loads[i].offset) {
                atomic_fetch_add(&duplication, at);
            }
            prevPos = loads[i].offset;
            atomic_fetch_add(&totalclicks, at);
        }
#endif
    }
    return loads;
}

void freeloads(struct payload *loads) {
    delete[] loads;
}

void readloads(FILE *fp, struct payload *loads) {
    for (int i = 0; i < PAYLOAD_CNT; i++) {
        //cout << loads[i].offset << "\t";offset = {bigint} 7629385728
        if (bigseek(fp, loads[i].offset, SEEK_SET) == 0) {
            fread(loads[i].data, PAGE_SIZE, 1, fp);
            //cout << ((bigint*)(loads[i].data))[0] << endl;
        } else {
            cout << "Error" << endl;
            exit(-1);
        }
    }
}

void writeLoad(FILE *fp, struct payload *loads) {
    for (int r = 0; r < PSEUDOWRITE; r++) {
        for (int i = 0; i < PAYLOAD_CNT; i++) {
            //cout << loads[i].offset << "\t";
            if (bigseek(fp, loads[i].offset, SEEK_SET) == 0) {
                atomic_fetch_add(&counter, at);
                fwrite(loads[i].data, PAGE_SIZE, 1, fp);
                //fflush(fp);
                //cout << ((bigint*)(loads[i].data))[0] << endl;
            } else {
                cout << "Error" << endl;
                exit(-1);
            }
        }
    }
}

void singlethreadread() {
    //initFile(true);
    Tracer tracer;
    tracer.startTime();
    struct payload *loads = generate(SORTOPS);
#if 0
    FILE *fp = fopen(SSDPATH, "rb+");
    bigseek(fp, (1LL << 34), SEEK_SET);
    char buf[PAGE_SIZE];
    fread(buf, PAGE_SIZE, 1, fp);
    cout << ((bigint *) buf)[0] << endl;
    cout << ((bigint *) buf)[1] << endl;
    cout << ((bigint *) buf)[2] << endl;
    cout << ((bigint *) buf)[3] << endl;
    fclose(fp);
#endif

    FILE *fp;
#if USINGSSD
    fp = fopen(SSDPATH, "rb+");
#else
    fp = fopen(HDDPATH, "rb+");
#endif
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    readloads(fp, loads);
    cout << PAYLOAD_CNT << "\t" << tracer.getRunTime() << endl;
    fclose(fp);
    freeloads(loads);
}

void singthreadwrite() {
    //initFile(true);
    Tracer tracer;
    tracer.startTime();
    struct payload *loads = generate(SORTOPS);
#if 0
    FILE *fp = fopen(SSDPATH, "rb+");
    bigseek(fp, (1LL << 34), SEEK_SET);
    char buf[PAGE_SIZE];
    fread(buf, PAGE_SIZE, 1, fp);
    cout << ((bigint *) buf)[0] << endl;
    cout << ((bigint *) buf)[1] << endl;
    cout << ((bigint *) buf)[2] << endl;
    cout << ((bigint *) buf)[3] << endl;
    fclose(fp);
#endif
    FILE *fp;
#if USINGSSD
    fp = fopen(SSDPATH, "rb+");
#else
    fp = fopen(HDDPATH, "rb+");
#endif
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    writeLoad(fp, loads);
    cout << PAYLOAD_CNT << "\t" << tracer.getRunTime() << endl;
    fflush(fp);
    fclose(fp);
    freeloads(loads);
}

void *threadFunc(void *parms) {
    FILE *fp;
#if USINGSSD
    char path[256];
#if SEPARATE
    sprintf(path, "%s%d", SSDPATH, ((struct threadload *) parms)->id);
    //cout << "\t" << ((struct threadload *) parms)->id << " Open: " << path << endl;
    fp = fopen(path, "rb+");
#else
    fp = fopen(SSDPATH, "rb+");
#endif
#else
#if SEPARATE
#if MULTIPLE
    char path[256];
    if (((struct threadload *) parms)->id < THREADNUM / 4) {
        sprintf(path, "%s%d", HDDPATH, ((struct threadload *) parms)->id);
    } else if (((struct threadload *) parms)->id < THREADNUM / 2) {
        sprintf(path, "%s%d", HDDSECD, ((struct threadload *) parms)->id);
    } else if (((struct threadload *) parms)->id < THREADNUM * 3 / 4) {
        sprintf(path, "%s%d", HDDTHRD, ((struct threadload *) parms)->id);
    } else {
        sprintf(path, "%s%d", HDDFORD, ((struct threadload *) parms)->id);
    }
    fp = fopen(path, "rb+");
#else
    char path[256];
    memset(path, '\n', 256);
    sprintf(path, "%s%d", HDDPATH, ((struct threadload *) parms)->id);
    cout << ((struct threadload *) parms)->id << " " << path << endl;
    fp = fopen(path, "rb+");
#endif
#else
    fp = fopen(HDDPATH, "rb+");
#endif
#endif
    if (((struct threadload *) parms)->type) {
        readloads(fp, ((struct threadload *) parms)->load);
    } else {
        writeLoad(fp, ((struct threadload *) parms)->load);
    }
    fclose(fp);
}

void multithreadworkers(int tnum, bool type) {
    struct threadload *payloads = new struct threadload[tnum];
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < tnum; i++) {
        payloads[i].id = i;
        payloads[i].type = type;
#if PARTITION
        payloads[i].load = generate(SORTOPS, i);
#else
        payloads[i].load = generate(SORTOPS, i * rand());
#endif
    }
    cout << "Generate: " << tracer.getRunTime() << " duplications: " << duplication << " out of: " << totalclicks
         << endl;
    tracer.startTime();
    pthread_t pids[tnum];
    for (int i = 0; i < tnum; i++) {
        pthread_create(&pids[i], NULL, threadFunc, &payloads[i]);
    }
    for (int i = 0; i < tnum; i++) {
        pthread_join(pids[i], NULL);
    }
    cout << "Total: " << tracer.getRunTime() << " counter: " << counter << endl;
    for (int i = 0; i < tnum; i++) {
        delete[] payloads[i].load;
    }
    delete[] payloads;
}

int main(int argc, char **argv) {
    counter = 0;
#if INITFILE
    initFile(USINGSSD);
#else
#if 0
    singlethreadread();
    singthreadwrite();
#endif
    multithreadworkers(THREADNUM, false);
#endif
}