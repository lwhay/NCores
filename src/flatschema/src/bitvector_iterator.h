// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BITVECTOR_ITERATOR_H_
#define BITWEAVING_SRC_BITVECTOR_ITERATOR_H_

#include <assert.h>

#include "bitweaving/bitvector.h"
#include "bitweaving/status.h"
#include "bitweaving/types.h"
#include "src/bitvector_block.h"

namespace bitweaving {

/**
 * @brief Class for accessing 1-bit in a bit-vector.
 */
class BitVectorIterator {
public:
    /**
     * @brief Constructs a new iterator for the input bit vector.
     * @ param bitvector The bit-vector that this iterator is created on.
     */
    BitVectorIterator(const BitVector &bitvector);

    /**
     * @brief Destructor.
     */
    ~BitVectorIterator();

    /**
     * @brief Seeks to the position of the first bit in the bit-vector.
     */
    void Begin();

    /**
     * @brief Moves to the position of the next 1-bit in the bit-vector.
     */
    bool Next();

    /**
     * @brief Gets the current position in the bit-vector.
     */
    TupleId GetPosition() const {
        return pos_;
    }

private:
    /**
     * @brief The bit-vector.
     */
    const BitVector &bitvector_;

    /**
     * @brief The block ID of the current block of the bit-vector.
     */
    size_t block_id_;
    /**
     * @brief The object of the current block of the bit-vector.
     */
    BitVectorBlock *cur_block_;
    /**
     * @brief The starting position (bit ID) of the current block of the bit-vector.
     */
    size_t block_offset_;
    /**
     * @brief The position (bit ID) of current 1-bit in the bit-vector.
     */
    TupleId pos_;
    /**
     * @brief The number of word units in the current block of the bit-vector.
     */
    size_t word_unit_pos_;
    /**
     * @brief The ID of current word unit in the current block of the bit-vector.
     */
    size_t num_word_units_;
    /**
     * @brief A stack of next 1-bit positions.
     */
    size_t stack_[kNumBitsPerWord];
    /**
     * @brief The number of positions in the stack.
     */
    size_t stack_size_;
};

}  // namespace bitweaving

#endif // BITWEAVING_SRC_BITVECTOR_ITERATOR_H_
