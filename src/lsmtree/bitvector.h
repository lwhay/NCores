#pragma once

#include <stdatomic.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <stdio.h>

/*
 * BitVector definitions and helper functions
 *
 */

#define INT_BIT (sizeof(_Atomic(int)) * CHAR_BIT)

#ifndef NDEBUG
#define BV_BOUND_CHECK(size, idx) assert(idx < size)
#else
#define BV_BOUND_CHECK(size, idx)
#endif

typedef struct BitVector {
    _Atomic int *field; //bits themselves
    size_t size; //number of bits
} bitvector_t;


/**
 * Initialize a bitvector
 */
static int bv_init(bitvector_t *bv, size_t k) {
    bv->size = k;
    /* ints is the number of integers required for space k */
    size_t ints = (k + (INT_BIT - 1)) / INT_BIT;
    bv->field = calloc(ints, sizeof(_Atomic (int)));
    if (bv->field == NULL) {
        return -1;
    }
    return 0;
}

/**
 * Create a bitvector (struct and content)
 */
static bitvector_t *bv_create(size_t k) {
    bitvector_t *v = malloc(sizeof(bitvector_t));
    if (v == NULL || bv_init(v, k)) {
        free(v);
        return NULL;
    }
    return v;
}

/**
 * Free content of bitvector
 */
static void bv_free(bitvector_t *bv) {
    free(bv->field);
    bv->size = 0;
}

/**
 * Free bitvector and content
 */
static void bv_destroy(bitvector_t *bv) {
    bv_free(bv);
    free(bv);
}


/**
 * \brief set_bit toggles a bit on in a bit vector
 */
static void bv_set(bitvector_t *bv, size_t k) {
    BV_BOUND_CHECK(bv->size, k);
    /*bv->field[k / INT_BIT] |= 1 << (k % INT_BIT);*/
    atomic_fetch_or(&(bv->field[k / INT_BIT]), 1 << (k % INT_BIT));
}

static void bv_unset(bitvector_t *bv, size_t k) {
    BV_BOUND_CHECK(bv->size, k);
    /*bv->field[k / INT_BIT] &= ~(1 << (k % INT_BIT));*/
    atomic_fetch_and(&(bv->field[k / INT_BIT]), ~(1 << (k % INT_BIT)));
}

static void bv_toggle(bitvector_t *bv, size_t k) {
    BV_BOUND_CHECK(bv->size, k);
    /*bv->field[k / INT_BIT] ^= 1 << (k % INT_BIT);*/
    atomic_fetch_xor(&(bv->field[k / INT_BIT]), 1 << (k % INT_BIT));
}

static int bv_get(bitvector_t *bv, size_t k) {
    BV_BOUND_CHECK(bv->size, k);
    /*return ((bv->field[k / INT_BIT] & (1 << (k % INT_BIT))) != 0);*/
    return ((atomic_load(&(bv->field[k / INT_BIT])) & (1 << (k % INT_BIT))) != 0);
}

#undef BV_BOUND_CHECK
#undef INT_BIT
/*
 * vim: ft=c
 */
