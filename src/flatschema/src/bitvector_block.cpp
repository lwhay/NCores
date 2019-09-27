// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <bitset>
#include <cstring>
#include <iostream>
#include <memory>

#include "src/bitvector_block.h"

#include "src/bitwise.h"
#include "src/file.h"
#include "src/macros.h"
#include "src/utility.h"

namespace bitweaving {

BitVectorBlock::BitVectorBlock()
        : num_(0) {
    num_word_units_ = CEIL(num_, kNumBitsPerWord);
    data_ = new WordUnit[CEIL(kNumCodesPerBlock, kNumBitsPerWord)];
}

BitVectorBlock::BitVectorBlock(size_t num)
        : num_(num) {
    num_word_units_ = CEIL(num_, kNumBitsPerWord);
    data_ = new WordUnit[CEIL(kNumCodesPerBlock, kNumBitsPerWord)];
}

BitVectorBlock::~BitVectorBlock() {
    delete[] data_;
}

void BitVectorBlock::Clone(const BitVectorBlock &block) {
    if (num_word_units_ != block.num_word_units_) {
        delete[] data_;
        data_ = new WordUnit[block.num_word_units_];
    }
    num_ = block.num_;
    num_word_units_ = block.num_word_units_;
    memcpy(data_, block.data_, sizeof(WordUnit) * num_word_units_);
}

bool BitVectorBlock::Equal(const BitVectorBlock &block) const {
    if (num_ != block.num_)
        return false;
    for (size_t i = 0; i < num_word_units_; i++) {
        if (data_[i] != block.data_[i])
            return false;
    }
    return true;
}

void BitVectorBlock::SetZeros() {
    memset(data_, 0, sizeof(WordUnit) * num_word_units_);
    Finalize();
}

void BitVectorBlock::SetOnes() {
    uint8_t mask = -1;
    memset(data_, mask, sizeof(WordUnit) * num_word_units_);
    Finalize();
}

size_t BitVectorBlock::Count() {
    size_t count = 0;
    for (size_t i = 0; i < num_word_units_; i++) {
        count += Popcnt(data_[i]);
    }
    return count;
}

void BitVectorBlock::Complement() {
    for (size_t i = 0; i < num_word_units_; i++) {
        data_[i] = ~data_[i];
    }
    Finalize();
}

Status BitVectorBlock::And(const BitVectorBlock &block) {
    if (block.num_ != num_) {
        return Status::InvalidArgument(
                "Logical AND: BitVector length does not match.");
    }
    for (size_t i = 0; i < num_word_units_; i++) {
        data_[i] = data_[i] & block.data_[i];
    }
    Finalize();
    return Status::OK();
}

Status BitVectorBlock::Or(const BitVectorBlock &block) {
    if (block.num_ != num_) {
        return Status::InvalidArgument(
                "Logical OR: BitVector length does not match.");
    }
    for (size_t i = 0; i < num_word_units_; i++) {
        data_[i] = data_[i] | block.data_[i];
    }
    Finalize();
    return Status::OK();
}

void BitVectorBlock::Finalize() {
    size_t word = num_ / kNumBitsPerWord;
    size_t offset = kNumBitsPerWord - (num_ % kNumBitsPerWord);
    if (offset != kNumBitsPerWord)
        data_[word] &= (-1ULL << offset);
}

std::string BitVectorBlock::ToString() {
    std::string str;
    for (size_t i = 0; i < num_word_units_; i++) {
        std::bitset<64> bitset(data_[i]);
        str.append(bitset.to_string());
        str.append(" ");
    }
    return str;
}

Status BitVectorBlock::Load(SequentialReadFile &file) {
    Status status;

    status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(&num_word_units_),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(data_),
                       sizeof(WordUnit) * num_word_units_);
    if (!status.IsOk())
        return status;

    return Status::OK();
}

Status BitVectorBlock::Save(SequentialWriteFile &file) {
    Status status;

    status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(&num_word_units_),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(data_),
                         sizeof(WordUnit) * num_word_units_);
    if (!status.IsOk())
        return status;

    return Status::OK();
}

}  // namespace bitweaving
