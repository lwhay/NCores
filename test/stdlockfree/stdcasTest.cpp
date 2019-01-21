//
// Created by Michael on 2018/12/18.
//
#include <atomic>
#include <iostream>
#include <sys/time.h>

#define DEFAULT_ARRAY_SIZE (1LL << 30)

using namespace std;

struct dw {
    long long x;
    long long y;
};

int main(int argc, char **argv) {
    if (1) {
        long long v1(1);
        long long v2(2);
        long long v3(3);
        atomic<long long> d1(v1);
        bool ret = -1;
        cout << ret << "\t" << d1 << "\t" << v2 << "\t" << v3 << endl;
        ret = d1.compare_exchange_weak(v2, v3);
        cout << ret << "\t" << d1 << "\t" << v2 << "\t" << v3 << endl;
        ret = d1.compare_exchange_weak(v1, v3);
        cout << ret << "\t" << d1 << "\t" << v2 << "\t" << v3 << endl;
    }
    if (1) {
        struct dw v1{1, 2};
        struct dw v2{3, 4};
        struct dw v3{5, 6};
        atomic<struct dw> d1;
        d1 = v1;
        bool ret = -1;
        cout << ret << "\t" << "<" << v1.x << "," << v1.y << ">" << "\t" << "<" << v2.x << "," << v2.y << ">" << "\t"
             << "<" << v3.x << "," << v3.y << ">" << endl;
        ret = d1.compare_exchange_strong(v2, v3);
        cout << ret << "\t" << "<" << v1.x << "," << v1.y << ">" << "\t" << "<" << v2.x << "," << v2.y << ">" << "\t"
             << "<" << v3.x << "," << v3.y << ">" << endl;
        ret = d1.compare_exchange_strong(v1, v3);
        cout << ret << "\t" << "<" << v1.x << "," << v1.y << ">" << "\t" << "<" << v2.x << "," << v2.y << ">" << "\t"
             << "<" << v3.x << "," << v3.y << ">" << endl;
    }
    long long *swaps = (long long *) malloc(sizeof(long long) * DEFAULT_ARRAY_SIZE);
    struct timeval tpstart, tpend;
    unsigned long long int count = 0;
    gettimeofday(&tpstart, NULL);
    for (long long i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        swaps[i] = i;
        count++;
    }
    gettimeofday(&tpend, NULL);
    double timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Init: %d %llu %lf\n", sizeof(count), count, timeused);

    atomic<long long> mapper;
    gettimeofday(&tpstart, NULL);
    for (long long i = 0; i < DEFAULT_ARRAY_SIZE - 1; i += 2) {
        mapper = swaps[i];
        mapper.compare_exchange_weak(swaps[i], swaps[i + 1]);
        //swaps[i + 1] = abstraction_cas((volatile atom_t *) &swaps[i], swaps[i + 1], swaps[i]);
        count++;
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Swap: %d %llu %lf\n", sizeof(count), count, timeused);

    gettimeofday(&tpstart, NULL);
    delete[] swaps;
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Free: %d %llu %lf\n", sizeof(count), count, timeused);

    struct dw *swwds = (struct dw *) malloc(sizeof(struct dw) * DEFAULT_ARRAY_SIZE);
    count = 0;
    gettimeofday(&tpstart, NULL);
    for (long long i = 0; i < DEFAULT_ARRAY_SIZE; i++) {
        swwds[i].x = i;
        swwds[i].y = i + 1;
        count++;
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Init: %d %llu %lf\n", sizeof(count), count, timeused);

    atomic<struct dw> dwmapper;
    gettimeofday(&tpstart, NULL);
    for (long long i = 0; i < DEFAULT_ARRAY_SIZE - 1; i += 2) {
        dwmapper = swwds[i];
        dwmapper.compare_exchange_strong(swwds[i], swwds[i + 1]);
        //swaps[i + 1] = abstraction_cas((volatile atom_t *) &swaps[i], swaps[i + 1], swaps[i]);
        count++;
    }
    gettimeofday(&tpend, NULL);
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Swap: %d %llu %lf\n", sizeof(count), count, timeused);

    gettimeofday(&tpstart, NULL);
    delete[] swwds;
    timeused = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + tpend.tv_usec - tpstart.tv_usec;
    printf("Free: %d %llu %lf\n", sizeof(count), count, timeused);

    return 0;
}
