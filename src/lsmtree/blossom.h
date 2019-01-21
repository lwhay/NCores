#ifndef BLOOM_H
#define BLOOM_H

#include "bitvector.h"
#include <stdlib.h>

/**
 * bloom_t structure
 */
typedef struct {
    size_t capacity; /** Capacity of the bloom filter */
    size_t nhashes; /** Number of hashses required for this filter */
    double error_rate; /** Maximum acceptable error-rate */
    _Atomic size_t nitems; /** Number of items inserted into filter. */
    size_t size; /** Maximum size of bloom filter. */
    bitvector_t *bitvector; /** Pointer to bitvector for bloom filter */
} bloom_t;

/**
 * Initialize bloom filter
 * @param[out] bloom      Pointer to memory to store bloom filter
 * @param[in]  capacity   Maximum capacity of bloom filter
 * @param[in]  error_rate Maximum acceptable error rate
 * @return 0: Initialization successful, -1: an error occured.
 */
int bloom_init(bloom_t *bloom, size_t capacity, double error_rate);

/**
 * Create a bloom filter
 * @param[in] capacity   Maximum capcity of Bloom Filter.
 * @param[in] error_rate Maximul acceptable error rate.
 * @return Pointer to bloom filter.
 */
bloom_t *bloom_create(size_t capacity, double error_rate);

/**
 * Add entry to Bloom Filter
 * @param[in] bloom   A pointer to the Bloom Filter.
 * @param[in] key     Key to insert into the Bloom Filter.
 * @param[in] key_len Length of the key.
 * @return 0: The key inserted was unqiue, >0: The key was not unique.
*/
int bloom_add(bloom_t *bloom, const char *key, size_t key_len);

/**
 * Get status of key in Bloom Filter
 * @param[in] bloom   A pointer to the Bloom Filter.
 * @param[in] key     Key to insert into the Bloom Filter.
 * @param[in] key_len Length of the key.
 * @return 0: Key in not in filter, 1: key in filter
 */
int bloom_get(bloom_t *bloom, const char *key, size_t key_len);

/**
 * Deallocate the memory used for a bloom filter
 * @param[in] bloom Pointer to bloom filter.
 */
void bloom_destroy(bloom_t *bloom);

#endif /* ifndef BLOOM_H */
/*
 * vim: ft=c
 */
