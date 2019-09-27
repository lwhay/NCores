// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <memory>
#include <vector>

#include "bitweaving/bitvector.h"

#include "bitweaving/status.h"
#include "src/bitvector_block.h"
#include "src/bitvector_iterator.h"
#include "src/utility.h"

namespace bitweaving {

/**
 * @brief Structure for private members of Class BitVector.
 */
struct BitVector::Rep {

    /**
     * @brief Constructor
     */
    Rep(const Table &table) : table(table), num_bits(table.GetNumTuples()) {
    }

    /**
     * @brief The table that this bit-vector is built on.
     */
    const Table &table;
    /**
     * @brief The number of bits in this bit-vector.
     */
    size_t num_bits;
    /**
     * @brief A list of bit-vector blocks.
     */
    std::vector<BitVectorBlock *> blocks;
};

BitVector::BitVector(const Table &table) {
    rep_ = new Rep(table);
    for (size_t i = 0; i < rep_->num_bits; i += kNumCodesPerBlock) {
        size_t size = std::min(rep_->num_bits - i, kNumCodesPerBlock);
        rep_->blocks.push_back(new BitVectorBlock(size));
    }
    SetOnes();
}

BitVector::~BitVector() {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        delete rep_->blocks[i];
    }
    delete rep_;
}

void BitVector::Clone(const BitVector &bit_vector) {
    rep_->num_bits = bit_vector.rep_->num_bits;
    if (rep_->blocks.size() < bit_vector.rep_->blocks.size()) {
        // Add more bit-vector blocks.
        for (size_t i = rep_->blocks.size(); i < bit_vector.rep_->blocks.size(); i++) {
            rep_->blocks.push_back(new BitVectorBlock(0));
        }
    } else {
        // Remove unneccesary bit-vector blocks.
        for (size_t i = bit_vector.rep_->blocks.size(); i < rep_->blocks.size(); i++) {
            BitVectorBlock *block = rep_->blocks.back();
            delete block;
            rep_->blocks.pop_back();
        }
    }
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->Clone(*bit_vector.rep_->blocks[i]);
    }
}

bool BitVector::Equal(const BitVector &bit_vector) const {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        if (!rep_->blocks[i]->Equal(*bit_vector.rep_->blocks[i]))
            return false;
    }
    return true;
}

size_t BitVector::GetNumBits() const {
    return rep_->num_bits;
}

const Table &BitVector::GetTable() const {
    return rep_->table;
}

Status BitVector::SetBit(TupleId bit_id, bool bit) {
    if (bit_id >= rep_->num_bits) {
        return Status::InvalidArgument("Tuple ID overflow in bit-vector.");
    }
    size_t block_id = bit_id / kNumCodesPerBlock;
    size_t bit_id_in_block = bit_id % kNumCodesPerBlock;
    rep_->blocks[block_id]->SetBit(bit_id_in_block, bit);
    return Status::OK();
}

Status BitVector::GetBit(TupleId bit_id, bool *bit) const {
    if (bit_id >= rep_->num_bits) {
        return Status::InvalidArgument("Tuple ID overflow in bit-vector.");
    }
    size_t block_id = bit_id / kNumCodesPerBlock;
    size_t bit_id_in_block = bit_id % kNumCodesPerBlock;
    rep_->blocks[block_id]->SetBit(bit_id_in_block, *bit);
    return Status::OK();
}

/**
 * maxSize = STORE->NUM_POINTS
 */
void BitVector::toTids(vector<uint32_t> *result, uint32_t maxSize) {

    result->resize(maxSize);
    uint32_t *result_tids = &((*result)[0]);
    WordUnit pos = 0;
    uint32_t maxBlock = this->GetNumBitVectorBlock();
    uint32_t block_id = 0;
    WordUnit *data;
    uint32_t maxWord;
    uint32_t word_id;
    uint32_t bit_id;

    for (; block_id < maxBlock; ++block_id) {
        maxWord = rep_->blocks[block_id]->num_word_units_;
        word_id = 0;
        for (; word_id < maxWord - 1; ++word_id) {
            if (rep_->blocks[block_id]->data_[word_id] != 0) {
                data = rep_->blocks[block_id]->data_;
                for (bit_id = 0; bit_id < kNumBitsPerWord; ++bit_id) {
                    result_tids[pos] = word_id * kNumBitsPerWord + bit_id;
                    // WordUnit bitmask = 1 << bit_id;
                    // pos += (bitmask & rep_->blocks[block_id]->data_[word_id]) >> bit_id;
                    pos += (data[word_id] >> (kNumBitsPerWord - 1 - bit_id)) & 0x1;
                }
            }
        }
        //process last word which is not fully filled
        if (rep_->blocks[block_id]->data_[word_id] != 0) {
            data = rep_->blocks[block_id]->data_;
            for (bit_id = 0; bit_id + word_id * kNumBitsPerWord < rep_->blocks[block_id]->num_; ++bit_id) {
                result_tids[pos] = word_id * kNumBitsPerWord + bit_id;
                //WordUnit bitmask = 1 << bit_id;
                pos += (data[word_id] >> (kNumBitsPerWord - 1 - bit_id)) & 0x1;
            }
        }
    }

    //process remaining block
    /*for(;block_id<(maxSize+kNumCodesPerBlock-1)/kNumCodesPerBlock;++block_id){
            for(uint32_t bit_id=0;bit_id<maxSize%kNumCodesPerBlock;++bit_id){
                    result_tids[pos]=block_id *kNumCodesPerBlock+bit_id;
                    WordUnit bitmask = 1 << bit_id;
                    pos+=(bitmask &  rep_->blocks[block_id]->data_[block_id]) >> bit_id;
            }
    }*/
    result->resize(pos);
}

void BitVector::SetZeros() {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->SetZeros();
    }
}

void BitVector::SetOnes() {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->SetOnes();
    }
}

size_t BitVector::CountOnes() const {
    size_t count = 0;
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        count += rep_->blocks[i]->Count();
    }
    return count;
}

void BitVector::Complement() {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->Complement();
    }
}

Status BitVector::And(const BitVector &bit_vector) {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->And(*bit_vector.rep_->blocks[i]);
    }
    return Status::OK();
}

Status BitVector::Or(const BitVector &bit_vector) {
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->Or(*bit_vector.rep_->blocks[i]);
    }
    return Status::OK();
}

std::string BitVector::ToString() const {
    std::string str;
    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        if (i != 0)
            str += "-";
        str += rep_->blocks[i]->ToString();
    }
    return str;
}

size_t BitVector::GetNumBitVectorBlock() const {
    return rep_->blocks.size();
}

BitVectorBlock *BitVector::GetBitVectorBlock(size_t block_id) const {
    assert(block_id < rep_->blocks.size());
    return rep_->blocks[block_id];
}

Status BitVector::Save(const std::string &path) const {
    Status status;
    SequentialWriteFile file;
    status = file.Open(path);
    if (!status.IsOk()) {
        return status;
    }

    status = file.Append(reinterpret_cast<const char *> (&rep_->num_bits),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    size_t num_blocks = rep_->blocks.size();
    status = file.Append(reinterpret_cast<const char *> (&num_blocks),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    for (size_t i = 0; i < rep_->blocks.size(); i++) {
        rep_->blocks[i]->Save(file);
    }

    status = file.Flush();
    if (!status.IsOk()) {
        return status;
    }
    status = file.Close();
    if (!status.IsOk()) {
        return status;
    }

    return Status::OK();
}

Status BitVector::Load(const std::string &path) {
    Status status;
    SequentialReadFile file;
    status = file.Open(path);
    if (!status.IsOk()) {
        return status;
    }

    size_t num_bits;
    status = file.Read(reinterpret_cast<char *> (&num_bits),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    if (num_bits != rep_->num_bits) {
        return Status::InvalidArgument(
                "The number of bits in data file does not match table size.");
    }

    size_t num_blocks;
    status = file.Read(reinterpret_cast<char *> (&num_blocks),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    assert(num_blocks == rep_->blocks.size());
    for (size_t i = 0; i < num_blocks; i++) {
        rep_->blocks[i]->Load(file);
    }

    status = file.Close();
    if (!status.IsOk()) {
        return status;
    }

    return Status::OK();
}

} // namespace bitweaving
