// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BITVECTOR_BLOCK_H_
#define BITWEAVING_SRC_BITVECTOR_BLOCK_H_

#include <cassert>

#include "bitweaving/status.h"
#include "bitweaving/types.h"
#include "bitweaving/iterator.h"
#include "src/file.h"

namespace bitweaving {

class Iterator;

/**
 * @brief Class of a block of a fixed-length bit-vector to
 * indicate the results of a scan.
 */
class BitVectorBlock {
public:
    BitVectorBlock();

    BitVectorBlock(size_t num);

    ~BitVectorBlock();

    /**
     * @brief Copy the bit-vector block.
     * @param block The bit-vector block to copy.
     */
    void Clone(const BitVectorBlock &block);

    /**
     * @brief Comparator
     * @param block The bit-vector block to be compared with.
     * @return True iff the two bit-vector blocks are identical.
     */
    bool Equal(const BitVectorBlock &block) const;

    /**
     * @brief Get the number of bits in this bit-vector block.
     * @return The number of bits in this bit-vector block.
     */
    size_t GetNum() {
        return num_;
    }

    /**
     * @brief Set all bits in this bit-vector block to be 0-bit.
     */
    void SetZeros();

    /**
     * @brief Set all bits in this bit-vector block to be 1-bit.
     */
    void SetOnes();

    /**
     * @brief Count the number of 1-bit in this bit-vector block.
     * @return The number of 1-bit.
     */
    size_t Count();

    /**
     * @brief Perform logical complement on this bit-vector block.
     * Change all 1-bit to 0-bit, and 0-bit to 1-bit.
     */
    void Complement();

    /**
     * @brief Perform logical AND with the input bit-vector block.
     * @param block The input bit-vector block for logical AND operation.
     * @return A Status object to indicate success or failure.
     */
    Status And(const BitVectorBlock &block);

    /**
     * @brief Perform logical OR with the input bit-vector block.
     * @param block The input bit-vector block for logical OR operation.
     * @return A Status object to indicate success or failure.
     */
    Status Or(const BitVectorBlock &block);

    /**
     * @brief Clear up the unused bits at the end of this bit-vector block.
     */
    void Finalize();

    /**
     * @brief Get the number of word units this block occupies.
     * @return The number of word units.
     */
    size_t GetNumWordUnits() {
        return num_word_units_;
    }

    /**
     * @brief Get the word unit at the given position.
     * @param pos The position to search for.
     * @return The word unit at the given position.
     */
    WordUnit GetWordUnit(size_t pos);

    /**
     * @brief Set the word unit at the given position.
     * @param pos The position to search for.
     * @param word The word unit to set for.
     */
    void SetWordUnit(size_t pos, WordUnit word);

    /**
     * @brief Get the bit at the given position.
     * @param pos The position to search for.
     * @return The bit at the given position.
     */
    bool GetBit(size_t pos);

    /**
     * @brief Set the bit at the given position.
     * @param pos The position to search for.
     * @param word The bit to set for.
     */
    void SetBit(size_t pos, bool bit);

    /**
     * @brief Conver this bit-vector block to a string.
     * @warning This function is only used for debugging.
     * @return A string that represents this bit-vector block.
     */
    std::string ToString();

    /**
     * @brief Load this bit-vector block from the file system.
     * @param filename The name of data file.
     * @return A Status object to indicate success or failure.
     */
    Status Load(SequentialReadFile &file);

    /**
     * @brief Save this bit-vector block to the file system.
     * @param filename The name of data file.
     * @return A Status object to indicate success or failure.
     */
    Status Save(SequentialWriteFile &file);

    //private:
    WordUnit *data_;
    size_t num_;
    size_t num_word_units_;
};

inline WordUnit BitVectorBlock::GetWordUnit(size_t pos) {
    assert(pos < num_word_units_);
    return data_[pos];
}

inline void BitVectorBlock::SetWordUnit(size_t pos, WordUnit word) {
    assert(pos < num_word_units_);
    data_[pos] = word;
}

inline bool BitVectorBlock::GetBit(size_t pos) {
    assert(pos < num_);
    size_t word_id = pos / kNumBitsPerWord;
    size_t bit_offset = pos % kNumBitsPerWord;
    return (data_[word_id] >> (kNumBitsPerWord - 1 - bit_offset)) & 0x1;
}

inline void BitVectorBlock::SetBit(size_t pos, bool bit) {
    assert(pos < num_);
    size_t word_id = pos / kNumBitsPerWord;
    size_t bit_offset = pos % kNumBitsPerWord;
    data_[word_id] &= ~((WordUnit) 1 << (kNumBitsPerWord - 1 - bit_offset));
    data_[word_id] |= (WordUnit) bit << (kNumBitsPerWord - 1 - bit_offset);
}

}  // namespace bitweaving

#endif // BITWEAVING_BITVECTOR_BLOCK_H_
