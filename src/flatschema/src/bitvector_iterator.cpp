// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>

#include "src/bitvector_iterator.h"

#include "src/bitwise.h"

namespace bitweaving {

BitVectorIterator::BitVectorIterator(const BitVector &bitvector)
        : bitvector_(bitvector) {
    Begin();
}

BitVectorIterator::~BitVectorIterator() {

}

void BitVectorIterator::Begin() {
    num_word_units_ = 0;
    block_id_ = 0;
    block_offset_ = 0;
    word_unit_pos_ = 0;
    pos_ = 0;
    stack_size_ = 0;
    cur_block_ = NULL;
}

bool BitVectorIterator::Next() {
    if (stack_size_ == 0) {
        // Stack is empty, load next word unit.
        WordUnit word = 0;
        // Scan to next non-zero word
        while (word == 0) {
            if (word_unit_pos_ >= num_word_units_) {
                if (block_id_ >= bitvector_.GetNumBitVectorBlock())
                    return false;
                if (cur_block_ != NULL)
                    block_offset_ += cur_block_->GetNum();
                // cur_block_ = bitvector_.rep_->vector[block_id_++];
                cur_block_ = bitvector_.GetBitVectorBlock(block_id_++);
                num_word_units_ = cur_block_->GetNumWordUnits();
                word_unit_pos_ = 0;
            }
            word = cur_block_->GetWordUnit(word_unit_pos_++);
        }
        // Extract all 1-bit in this word unit and push their position to stack
        size_t offset = block_offset_ + (word_unit_pos_ - 1) * kNumBitsPerWord;
        while (word != 0) {
            assert(stack_size_ < kNumBitsPerWord);
            // Calculate the offset of the rightmost 1 in the word
            stack_[stack_size_++] = offset + Popcnt(RemoveSmearRightmostOne(word));
            assert(stack_[stack_size_ - 1] < bitvector_.GetNumBits());
            // Remove the rightmost 1
            word = RemoveRightmostOne(word);
        }
    }

    // If there are remaining positions in the stack buffer
    // Pop the position at the head of the stack
    pos_ = stack_[--stack_size_];
    return true;
}

}  // namespace bitweaving

