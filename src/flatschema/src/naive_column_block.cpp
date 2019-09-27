// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include "src/naive_column_block.h"

#include <cassert>

#include "src/file.h"
#include "src/macros.h"
#include "src/utility.h"

namespace bitweaving {

NaiveColumnBlock::NaiveColumnBlock()
        : ColumnBlock(kNaive, sizeof(Code) * kNumBitsPerByte) {
    data_ = new WordUnit[kNumCodesPerBlock];
}

NaiveColumnBlock::~NaiveColumnBlock() {
    delete[] data_;
}

Status NaiveColumnBlock::Resize(size_t num) {
    assert(num <= kNumCodesPerBlock);
    num_ = num;
    return Status::OK();
}

Status NaiveColumnBlock::Bulkload(const Code *codes, size_t num, TupleId start_tuple_id) {
    assert(start_tuple_id + num <= num_);
    for (size_t i = 0; i < num; i++) {
        data_[start_tuple_id + i] = codes[i];
    }
    return Status::OK();
}

Status NaiveColumnBlock::Load(SequentialReadFile &file) {
    Status status;

    status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(data_),
                       sizeof(WordUnit) * kNumCodesPerBlock);
    if (!status.IsOk())
        return status;

    return Status::OK();
}

Status NaiveColumnBlock::Save(SequentialWriteFile &file) {
    Status status;

    status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(data_),
                         sizeof(WordUnit) * kNumCodesPerBlock);
    if (!status.IsOk())
        return status;

    return Status::OK();
}

Status NaiveColumnBlock::Scan(Comparator comparator, Code code,
                              BitVectorBlock *bitvector, BitVectorOpt opt) {
    for (size_t offset = 0; offset < num_; offset += kNumBitsPerWord) {
        WordUnit word_unit = 0;
        for (size_t i = 0; i < kNumBitsPerWord; i++) {
            size_t id = offset + i;
            if (id >= num_) {
                break;
            }
            WordUnit bit;
            switch (comparator) {
                case kEqual:
                    bit = (data_[id] == code);
                    break;
                case kInequal:
                    bit = (data_[id] != code);
                    break;
                case kGreater:
                    bit = (data_[id] > code);
                    break;
                case kLess:
                    bit = (data_[id] < code);
                    break;
                case kGreaterEqual:
                    bit = (data_[id] >= code);
                    break;
                case kLessEqual:
                    bit = (data_[id] <= code);
                    break;
                default:
                    return Status::InvalidArgument("Unknown comparator.");
            }
            word_unit |= bit << (kNumBitsPerWord - 1 - i);
        }
        size_t word_unit_id = offset / kNumBitsPerWord;
        WordUnit x;
        switch (opt) {
            case kSet:
                break;
            case kAnd:
                x = bitvector->GetWordUnit(word_unit_id);
                word_unit &= x;
                break;
            case kOr:
                word_unit |= bitvector->GetWordUnit(word_unit_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(word_unit_id, word_unit);
    }
    // The column is all NULL in this region, set all bits to be 0s.
    for (size_t offset = CEIL(num_, kNumBitsPerWord) * kNumBitsPerWord;
         offset < bitvector->GetNum(); offset += kNumBitsPerWord) {
        size_t word_unit_id = offset / kNumBitsPerWord;
        bitvector->SetWordUnit(word_unit_id, 0);
    }
    return Status::OK();
}

Status NaiveColumnBlock::Scan_between(Code code1, Code code2,
                                      BitVectorBlock *bitvector, BitVectorOpt opt) {
    for (size_t offset = 0; offset < num_; offset += kNumBitsPerWord) {
        WordUnit word_unit = 0;
        for (size_t i = 0; i < kNumBitsPerWord; i++) {
            size_t id = offset + i;
            if (id >= num_) {
                break;
            }
            WordUnit bit = (data_[id] <= code2 && data_[id] >= code1);
            word_unit |= bit << (kNumBitsPerWord - 1 - i);
        }
        size_t word_unit_id = offset / kNumBitsPerWord;
        WordUnit x;
        switch (opt) {
            case kSet:
                break;
            case kAnd:
                x = bitvector->GetWordUnit(word_unit_id);
                word_unit &= x;
                break;
            case kOr:
                word_unit |= bitvector->GetWordUnit(word_unit_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(word_unit_id, word_unit);
    }
    // The column is all NULL in this region, set all bits to be 0s.
    for (size_t offset = CEIL(num_, kNumBitsPerWord) * kNumBitsPerWord;
         offset < bitvector->GetNum(); offset += kNumBitsPerWord) {
        size_t word_unit_id = offset / kNumBitsPerWord;
        bitvector->SetWordUnit(word_unit_id, 0);
    }
    return Status::OK();
}

Status NaiveColumnBlock::Scan(Comparator comparator, const ColumnBlock &column_block,
                              BitVectorBlock *bitvector, BitVectorOpt opt) {
    if (bitvector->GetNum() != num_)
        return Status::InvalidArgument(
                "Bit vector length does not match column length.");

    size_t num = std::min(num_, column_block.GetNumCodes());
    for (size_t offset = 0; offset < num; offset += kNumBitsPerWord) {
        WordUnit word_unit = 0;
        for (size_t i = 0; i < kNumBitsPerWord; i++) {
            size_t id = offset + i;
            if (id >= num_) {
                break;
            }
            WordUnit bit;
            Code code;
            column_block.GetCode(id, &code);
            switch (comparator) {
                case kEqual:
                    bit = (data_[id] == code);
                    break;
                case kInequal:
                    bit = (data_[id] != code);
                    break;
                case kGreater:
                    bit = (data_[id] > code);
                    break;
                case kLess:
                    bit = (data_[id] < code);
                    break;
                case kGreaterEqual:
                    bit = (data_[id] >= code);
                    break;
                case kLessEqual:
                    bit = (data_[id] <= code);
                    break;
                default:
                    return Status::InvalidArgument("Unknown comparator.");
            }
            word_unit |= bit << (kNumBitsPerWord - 1 - i);
        }
        size_t word_unit_id = offset / kNumBitsPerWord;
        switch (opt) {
            case kSet:
                break;
            case kAnd:
                word_unit &= bitvector->GetWordUnit(word_unit_id);
                break;
            case kOr:
                word_unit |= bitvector->GetWordUnit(word_unit_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(word_unit_id, word_unit);
    }
    // At least one column is all NULL in this region, set all bits to be 0s.
    for (size_t offset = CEIL(num, kNumBitsPerWord) * kNumBitsPerWord;
         offset < bitvector->GetNum(); offset += kNumBitsPerWord) {
        size_t word_unit_id = offset / kNumBitsPerWord;
        bitvector->SetWordUnit(word_unit_id, 0);
    }
    return Status::OK();
}

}  // namespace bitweaving
