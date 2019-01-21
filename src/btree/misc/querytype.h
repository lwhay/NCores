#pragma once

#include <float.h>

class rangeQuery {
public:
    float *low;
    float *high;
    float tq;
    int dim;

    rangeQuery(int _dim) {
        dim = _dim;
        low = new float[dim];
        high = new float[dim];
    }

    ~rangeQuery() {
        delete[] low;
        delete[] high;
    }
};

class knnQuery {
public:
    float *loc;
    float tq;
    int k;
    int dim;

    knnQuery(int _dim) {
        dim = _dim;
        loc = new float[dim];
    }

    ~knnQuery() {
        delete[] loc;
    }
};

class knnRS {
public:
    int *rid;
    float *dist;
    int num_nn;
    int knn;
    float knndist;

    knnRS(int k) {
        rid = new int[k];
        dist = new float[k];
        num_nn = 0;
        knndist = 0;
        knn = -1;
    }

    ~knnRS() {
        delete[] rid;
        delete[] dist;
    }
};