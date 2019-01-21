#include "global.h"
#include <string.h>

int DIM;

void RECORD_TYPE::init(int _dim) {
    dim = _dim;
    if (coord == NULL)
        coord = new float[dim];
}

RECORD_TYPE::RECORD_TYPE() {
    if (DIM > 0) {
        dim = DIM;
        coord = new float[DIM];
    } else {
        dim = 0;
        coord = NULL;
    }
}

RECORD_TYPE::~RECORD_TYPE() {
    if (coord != NULL)
        delete[] coord;
}

int RECORD_TYPE::get_size() {
    return dim * sizeof(float);
}

void RECORD_TYPE::get_mbr(float *low, float *high) {
    for (int i = 0; i < dim; i++) {
        low[i] = coord[i];
        high[i] = coord[i];
    }
}

void RECORD_TYPE::read_from_buffer(char *buffer) {
    memcpy(coord, buffer, sizeof(float) * dim);
}

void RECORD_TYPE::write_to_buffer(char *buffer) {
    memcpy(buffer, coord, sizeof(float) * dim);
}

RECORD_TYPE &RECORD_TYPE::operator=(RECORD_TYPE &d) {
    memcpy(coord, d.coord, sizeof(float) * dim);
    return *this;
}