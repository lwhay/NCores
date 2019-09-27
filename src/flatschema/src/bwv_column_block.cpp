// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <cstring>
#include <iostream>

#include "src/bwv_column_block.h"

namespace bitweaving {

template<size_t CODE_SIZE>
BwVColumnBlock<CODE_SIZE>::BwVColumnBlock()
        : ColumnBlock(kBitWeavingV, CODE_SIZE),
          num_used_words_(0) {
    memset(data_, 0, sizeof(WordUnit * ) * kMaxNumGroups);
    for (size_t group_id = 0; group_id < kNumGroups; group_id++) {
        data_[group_id] = new WordUnit[kNumWords];
        assert(data_[group_id] != NULL);
        memset(data_[group_id], 0, sizeof(WordUnit) * kNumWords);
    }
}

template<size_t CODE_SIZE>
BwVColumnBlock<CODE_SIZE>::~BwVColumnBlock() {
    for (size_t group_id = 0; group_id < kNumGroups; group_id++) {
        delete[] data_[group_id];
    }
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::GetCode(TupleId pos, Code *code) const {
    if (pos >= num_)
        return Status::InvalidArgument("GetCode: position overflow");

    size_t segment_id = pos / kNumCodesPerSegment;
    size_t offset_in_segment = kNumCodesPerSegment - 1
                               - (pos % kNumCodesPerSegment);
    WordUnit mask = 1ULL << offset_in_segment;
    WordUnit code_word = 0;

    size_t bit_id = 0;
    for (size_t group_id = 0; group_id < kNumFullGroups; group_id++) {
        size_t word_id = segment_id * kNumWordsPerSegment;
        for (size_t bit_id_in_group = 0; bit_id_in_group < kNumBitsPerGroup;
             bit_id_in_group++) {
            code_word |= ((data_[group_id][word_id] & mask) >> offset_in_segment)
                    << (kNumBitsPerCode - 1 - bit_id);
            word_id++;
            bit_id++;
        }
    }

    size_t word_id = segment_id * kNumBitsLastGroup;
    for (size_t bit_id_in_group = 0; bit_id_in_group < kNumBitsLastGroup;
         bit_id_in_group++) {
        code_word |= ((data_[kLastGroup][word_id] & mask) >> offset_in_segment)
                << (kNumBitsPerCode - 1 - bit_id);
        word_id++;
        bit_id++;
    }
    *code = (Code) code_word;
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::SetCode(TupleId pos, Code code) {
    if (pos >= num_)
        return Status::InvalidArgument("GetCode: position overflow");

    size_t segment_id = pos / kNumCodesPerSegment;
    size_t offset_in_segment = kNumCodesPerSegment - 1
                               - (pos % kNumCodesPerSegment);
    WordUnit mask = 1ULL << offset_in_segment;
    WordUnit code_word = (WordUnit) code;

    size_t bit_id = 0;
    for (size_t group_id = 0; group_id < kNumFullGroups; group_id++) {
        size_t word_id = segment_id * kNumWordsPerSegment;
        for (size_t bit_id_in_group = 0; bit_id_in_group < kNumBitsPerGroup;
             bit_id_in_group++) {
            data_[group_id][word_id] &= ~mask;
            data_[group_id][word_id] |= ((code_word
                    >> (kNumBitsPerCode - 1 - bit_id)) << offset_in_segment) & mask;
            word_id++;
            bit_id++;
        }
    }

    size_t word_id = segment_id * kNumBitsLastGroup;
    for (size_t bit_id_in_group = 0; bit_id_in_group < kNumBitsLastGroup;
         bit_id_in_group++) {
        data_[kLastGroup][word_id] &= ~mask;
        data_[kLastGroup][word_id] |= ((code_word
                >> (kNumBitsPerCode - 1 - bit_id)) << offset_in_segment) & mask;
        word_id++;
        bit_id++;
    }
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Resize(size_t num) {
    assert(num <= kNumCodesPerBlock);
    num_ = num;
    num_used_words_ = CEIL(num_, kNumCodesPerSegment) * kNumWordsPerSegment;
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Bulkload(const Code *codes, size_t num, size_t start_tuple_id) {
    assert(start_tuple_id + num <= num_);

    for (size_t i = 0; i < num; i++) {
        SetCode(start_tuple_id + i, codes[i]);
    }
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Load(SequentialReadFile &file) {
    Status status;

    status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(&num_used_words_),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    for (size_t group_id = 0; group_id < kNumGroups; group_id++) {
        status = file.Read(reinterpret_cast<char *>(data_[group_id]),
                           sizeof(WordUnit) * kNumWords);
        if (!status.IsOk())
            return status;
    }

    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Save(SequentialWriteFile &file) {
    Status status;

    status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(&num_used_words_),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    for (size_t group_id = 0; group_id < kNumGroups; group_id++) {
        status = file.Append(reinterpret_cast<char *>(data_[group_id]),
                             sizeof(WordUnit) * kNumWords);
        if (!status.IsOk())
            return status;
    }

    return Status::OK();
}

template<size_t CODE_SIZE>
template<Comparator CMP, size_t GROUP_ID>
void BwVColumnBlock<CODE_SIZE>::ScanIteration(size_t segment_offset,
                                              WordUnit &mask_equal,
                                              WordUnit &mask_less,
                                              WordUnit &mask_greater,
                                              WordUnit *&literal_bit_ptr) {
    static const size_t NUM_BITS =
            GROUP_ID == kLastGroup ? kNumBitsLastGroup : kNumBitsPerGroup;
    WordUnit *word_ptr = &data_[GROUP_ID][segment_offset];
    __builtin_prefetch(word_ptr + kPrefetchDistance);
    for (size_t bit_id = 0; bit_id < NUM_BITS; bit_id++) {
        switch (CMP) {
            case kEqual:
            case kInequal:
                mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                break;
            case kLess:
            case kLessEqual:
                mask_less = mask_less | (mask_equal & ~*word_ptr & *literal_bit_ptr);
                mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                break;
            case kGreater:
            case kGreaterEqual:
                mask_greater = mask_greater
                               | (mask_equal & *word_ptr & ~*literal_bit_ptr);
                mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                break;
        }
        word_ptr++;
        literal_bit_ptr++;
    }
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Scan(Comparator comparator, Code literal,
                                       BitVectorBlock *bitvector,
                                       BitVectorOpt opt) {
    if (bitvector->GetNum() != num_)
        return Status::InvalidArgument(
                "Bit vector length does not match column length.");
    if (literal >= (1LL << bit_width_))
        return Status::InvalidArgument("The code in predicate overflows.");

    switch (comparator) {
        case kEqual:
            return ScanHelper1<kEqual>(literal, bitvector, opt);
        case kInequal:
            return ScanHelper1<kInequal>(literal, bitvector, opt);
        case kGreater:
            return ScanHelper1<kGreater>(literal, bitvector, opt);
        case kLess:
            return ScanHelper1<kLess>(literal, bitvector, opt);
        case kGreaterEqual:
            return ScanHelper1<kGreaterEqual>(literal, bitvector, opt);
        case kLessEqual:
            return ScanHelper1<kLessEqual>(literal, bitvector, opt);
    }
    return Status::InvalidArgument("Unknown comparator in BitWeaving/H scan.");
}


template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Scan_between(Code literal1, Code literal2,
                                               BitVectorBlock *bitvector,
                                               BitVectorOpt opt) {
    if (bitvector->GetNum() != num_)
        return Status::InvalidArgument(
                "Bit vector length does not match column length.");
    if (literal1 >= (1LL << bit_width_))
        return Status::InvalidArgument("The code in predicate 1 overflows.");
    if (literal2 >= (1LL << bit_width_))
        return Status::InvalidArgument("The code in predicate 2 overflows.");
    switch (opt) {
        case kSet:
            return ScanHelper2_between<kSet>(literal1, literal2, bitvector);
        case kAnd:
            return ScanHelper2_between<kAnd>(literal1, literal2, bitvector);
        case kOr:
            return ScanHelper2_between<kOr>(literal1, literal2, bitvector);
    }
    return Status::InvalidArgument(
            "Unkown bit vector operator in BitWeaving/H scan.");
}


template<size_t CODE_SIZE>
template<BitVectorOpt OPT>
Status BwVColumnBlock<CODE_SIZE>::ScanHelper2_between(Code literal1, Code literal2,
                                                      BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(literal1 < (1LL << bit_width_));
    assert(literal2 < (1LL << bit_width_));

    WordUnit literal1_bits[kNumBitsPerCode];
    for (size_t bit_id = 0; bit_id < kNumBitsPerCode; bit_id++) {
        literal1_bits[bit_id] = 0ULL
                                - ((literal1 >> (kNumBitsPerCode - 1 - bit_id)) & 1ULL);
    }
    WordUnit literal2_bits[kNumBitsPerCode];
    for (size_t bit_id = 0; bit_id < kNumBitsPerCode; bit_id++) {
        literal2_bits[bit_id] = 0ULL
                                - ((literal2 >> (kNumBitsPerCode - 1 - bit_id)) & 1ULL);
    }

    for (size_t segment_offset = 0; segment_offset < num_used_words_;
         segment_offset += kNumWordsPerSegment) {
        size_t segment_id = segment_offset / kNumWordsPerSegment;
        WordUnit mask_less = 0;
        WordUnit mask_greater = 0;
        WordUnit mask_equal1;
        WordUnit mask_equal2;

        WordUnit mask_bitvector = bitvector->GetWordUnit(segment_id);
        switch (OPT) {
            case kSet:
                mask_equal1 = -1ULL;
                mask_equal2 = -1ULL;
                break;
            case kAnd:
                mask_equal1 = mask_bitvector;
                mask_equal2 = mask_bitvector;
                break;
            case kOr:
                mask_equal1 = ~mask_bitvector;
                mask_equal2 = ~mask_bitvector;
                break;
        }

        WordUnit *literal1_bit_ptr = literal1_bits;
        WordUnit *literal2_bit_ptr = literal2_bits;
        if (kNumBitsPerCode >= kNumBitsPerGroup && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
            ScanIteration_between<0>(segment_offset, mask_equal1, mask_equal2, mask_less, mask_greater,
                                     literal1_bit_ptr, literal2_bit_ptr);
            if (kNumBitsPerCode >= kNumBitsPerGroup * 2 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                ScanIteration_between<1>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                         mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                if (kNumBitsPerCode >= kNumBitsPerGroup * 3 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                    ScanIteration_between<2>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                             mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                    if (kNumBitsPerCode >= kNumBitsPerGroup * 4 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                        ScanIteration_between<3>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                                 mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                        if (kNumBitsPerCode >= kNumBitsPerGroup * 5 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                            ScanIteration_between<4>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                                     mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                            if (kNumBitsPerCode >= kNumBitsPerGroup * 6 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                                ScanIteration_between<5>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                                         mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                                if (kNumBitsPerCode >= kNumBitsPerGroup * 7 &&
                                    ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                                    ScanIteration_between<6>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                                             mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                                    if (kNumBitsPerCode >= kNumBitsPerGroup * 8 &&
                                        ((mask_equal1 != 0) | (mask_equal2 != 0))) {
                                        ScanIteration_between<7>(segment_offset, mask_equal1, mask_equal2, mask_less,
                                                                 mask_greater, literal1_bit_ptr, literal2_bit_ptr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if (kNumBitsLastGroup != 0 && ((mask_equal1 != 0) | (mask_equal2 != 0))) {
            size_t last_group_segment_offset = segment_id * kNumBitsLastGroup;
            ScanIteration_between<kLastGroup>(last_group_segment_offset, mask_equal1, mask_equal2, mask_less,
                                              mask_greater, literal1_bit_ptr, literal2_bit_ptr);
        }

        WordUnit mask = (mask_less | mask_equal2) & (mask_equal1 | mask_greater);

        switch (OPT) {
            case kAnd:
                bitvector->SetWordUnit(segment_id, mask & mask_bitvector);
                break;
            case kSet:
                bitvector->SetWordUnit(segment_id, mask);
                break;
            case kOr:
                bitvector->SetWordUnit(segment_id, mask | mask_bitvector);
                break;
        }
    }

    bitvector->Finalize();

    return Status::OK();
}

template<size_t CODE_SIZE>
template<size_t GROUP_ID>
void BwVColumnBlock<CODE_SIZE>::ScanIteration_between(size_t segment_offset,
                                                      WordUnit &mask_equal1,
                                                      WordUnit &mask_equal2,
                                                      WordUnit &mask_less,
                                                      WordUnit &mask_greater,
                                                      WordUnit *&literal1_bit_ptr,
                                                      WordUnit *&literal2_bit_ptr) {
    static const size_t NUM_BITS =
            GROUP_ID == kLastGroup ? kNumBitsLastGroup : kNumBitsPerGroup;
    WordUnit *word_ptr = &data_[GROUP_ID][segment_offset];
    __builtin_prefetch(word_ptr + kPrefetchDistance);
    for (size_t bit_id = 0; bit_id < NUM_BITS; bit_id++) {
        mask_less = mask_less | (mask_equal2 & ~*word_ptr & *literal2_bit_ptr);
        mask_greater = mask_greater | (mask_equal1 & *word_ptr & ~*literal1_bit_ptr);
        mask_equal1 = mask_equal1 & ~(*word_ptr ^ *literal1_bit_ptr);
        mask_equal2 = mask_equal2 & ~(*word_ptr ^ *literal2_bit_ptr);

        word_ptr++;
        literal1_bit_ptr++;
        literal2_bit_ptr++;
    }
}


template<size_t CODE_SIZE>
template<Comparator CMP>
Status BwVColumnBlock<CODE_SIZE>::ScanHelper1(Code literal, BitVectorBlock *bitvector,
                                              BitVectorOpt opt) {
    switch (opt) {
        case kSet:
            return ScanHelper2<CMP, kSet>(literal, bitvector);
        case kAnd:
            return ScanHelper2<CMP, kAnd>(literal, bitvector);
        case kOr:
            return ScanHelper2<CMP, kOr>(literal, bitvector);
    }
    return Status::InvalidArgument(
            "Unkown bit vector operator in BitWeaving/H scan.");
}

template<size_t CODE_SIZE>
template<Comparator CMP, BitVectorOpt OPT>
Status BwVColumnBlock<CODE_SIZE>::ScanHelper2(Code literal,
                                              BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(literal < (1LL << bit_width_));

    WordUnit literal_bits[kNumBitsPerCode];
    for (size_t bit_id = 0; bit_id < kNumBitsPerCode; bit_id++) {
        literal_bits[bit_id] = 0ULL
                               - ((literal >> (kNumBitsPerCode - 1 - bit_id)) & 1ULL);
    }

    for (size_t segment_offset = 0; segment_offset < num_used_words_;
         segment_offset += kNumWordsPerSegment) {
        size_t segment_id = segment_offset / kNumWordsPerSegment;
        WordUnit mask_less = 0;
        WordUnit mask_greater = 0;
        WordUnit mask_equal;

        WordUnit mask_bitvector = bitvector->GetWordUnit(segment_id);
        switch (OPT) {
            case kSet:
                mask_equal = -1ULL;
                break;
            case kAnd:
                mask_equal = mask_bitvector;
                break;
            case kOr:
                mask_equal = ~mask_bitvector;
                break;
        }

        WordUnit *literal_bit_ptr = literal_bits;
        if (kNumBitsPerCode >= kNumBitsPerGroup && mask_equal != 0) {
            ScanIteration<CMP, 0>(segment_offset, mask_equal, mask_less, mask_greater,
                                  literal_bit_ptr);
            if (kNumBitsPerCode >= kNumBitsPerGroup * 2 && mask_equal != 0) {
                ScanIteration<CMP, 1>(segment_offset, mask_equal, mask_less,
                                      mask_greater, literal_bit_ptr);
                if (kNumBitsPerCode >= kNumBitsPerGroup * 3 && mask_equal != 0) {
                    ScanIteration<CMP, 2>(segment_offset, mask_equal, mask_less,
                                          mask_greater, literal_bit_ptr);
                    if (kNumBitsPerCode >= kNumBitsPerGroup * 4 && mask_equal != 0) {
                        ScanIteration<CMP, 3>(segment_offset, mask_equal, mask_less,
                                              mask_greater, literal_bit_ptr);
                        if (kNumBitsPerCode >= kNumBitsPerGroup * 5
                            && mask_equal != 0) {
                            ScanIteration<CMP, 4>(segment_offset, mask_equal, mask_less,
                                                  mask_greater, literal_bit_ptr);
                            if (kNumBitsPerCode >= kNumBitsPerGroup * 6
                                && mask_equal != 0) {
                                ScanIteration<CMP, 5>(segment_offset, mask_equal, mask_less,
                                                      mask_greater, literal_bit_ptr);
                                if (kNumBitsPerCode >= kNumBitsPerGroup * 7
                                    && mask_equal != 0) {
                                    ScanIteration<CMP, 6>(segment_offset, mask_equal, mask_less,
                                                          mask_greater, literal_bit_ptr);
                                    if (kNumBitsPerCode >= kNumBitsPerGroup * 8
                                        && mask_equal != 0) {
                                        ScanIteration<CMP, 7>(segment_offset, mask_equal, mask_less,
                                                              mask_greater, literal_bit_ptr);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if (kNumBitsLastGroup != 0 && mask_equal != 0) {
            size_t last_group_segment_offset = segment_id * kNumBitsLastGroup;
            ScanIteration<CMP, kLastGroup>(last_group_segment_offset, mask_equal,
                                           mask_less, mask_greater, literal_bit_ptr);
        }

        WordUnit mask;
        switch (CMP) {
            case kEqual:
                mask = mask_equal;
                break;
            case kInequal:
                mask = ~mask_equal;
                break;
            case kGreater:
                mask = mask_greater;
                break;
            case kLess:
                mask = mask_less;
                break;
            case kGreaterEqual:
                mask = mask_greater | mask_equal;
                break;
            case kLessEqual:
                mask = mask_less | mask_equal;
                break;
        }

        switch (OPT) {
            case kSet:
                bitvector->SetWordUnit(segment_id, mask);
                break;
            case kAnd:
                bitvector->SetWordUnit(segment_id, mask & mask_bitvector);
                break;
            case kOr:
                bitvector->SetWordUnit(segment_id, mask | mask_bitvector);
                break;
        }
    }

    bitvector->Finalize();

    return Status::OK();
}

template<size_t CODE_SIZE>
template<Comparator CMP, size_t GROUP_ID>
void BwVColumnBlock<CODE_SIZE>::ScanIteration(size_t segment_offset,
                                              WordUnit &mask_equal,
                                              WordUnit &mask_less,
                                              WordUnit &mask_greater,
                                              WordUnit *const *other_data) {
    static const size_t NUM_BITS =
            GROUP_ID == kLastGroup ? kNumBitsLastGroup : kNumBitsPerGroup;
    WordUnit *word_ptr = &data_[GROUP_ID][segment_offset];
    WordUnit *other_word_ptr = &other_data[GROUP_ID][segment_offset];
    __builtin_prefetch(word_ptr + kPrefetchDistance);
    __builtin_prefetch(other_word_ptr + kPrefetchDistance);
    for (size_t bit_id = 0; bit_id < NUM_BITS; bit_id++) {
        switch (CMP) {
            case kEqual:
            case kInequal:
                mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                break;
            case kLess:
            case kLessEqual:
                mask_less = mask_less | (mask_equal & ~*word_ptr & *other_word_ptr);
                mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                break;
            case kGreater:
            case kGreaterEqual:
                mask_greater = mask_greater
                               | (mask_equal & *word_ptr & ~*other_word_ptr);
                mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                break;
        }
        word_ptr++;
        other_word_ptr++;
    }
}

template<size_t CODE_SIZE>
Status BwVColumnBlock<CODE_SIZE>::Scan(Comparator comparator,
                                       const ColumnBlock &column_block,
                                       BitVectorBlock *bitvector,
                                       BitVectorOpt opt) {
    if (bitvector->GetNum() != num_)
        return Status::InvalidArgument(
                "Bit vector length does not match column length.");
    if (column_block.GetNumCodes() != num_)
        return Status::InvalidArgument("Column length does not match.");
    if (column_block.GetColumnType() != kBitWeavingV)
        return Status::InvalidArgument(
                "Column type does not match. Input column should be in BitWeaving/H format.");
    if (column_block.GetCodeSize() != bit_width_)
        return Status::InvalidArgument("Code size does not match.");

    switch (comparator) {
        case kEqual:
            return ScanHelper1<kEqual>(column_block, bitvector, opt);
        case kInequal:
            return ScanHelper1<kInequal>(column_block, bitvector, opt);
        case kGreater:
            return ScanHelper1<kGreater>(column_block, bitvector, opt);
        case kLess:
            return ScanHelper1<kLess>(column_block, bitvector, opt);
        case kGreaterEqual:
            return ScanHelper1<kGreaterEqual>(column_block, bitvector, opt);
        case kLessEqual:
            return ScanHelper1<kLessEqual>(column_block, bitvector, opt);
    }
    return Status::InvalidArgument("Unknown comparator in BitWeaving/H scan.");
}

template<size_t CODE_SIZE>
template<Comparator CMP>
Status BwVColumnBlock<CODE_SIZE>::ScanHelper1(const ColumnBlock &column_block,
                                              BitVectorBlock *bitvector,
                                              BitVectorOpt opt) {
    switch (opt) {
        case kSet:
            return ScanHelper2<CMP, kSet>(column_block, bitvector);
        case kAnd:
            return ScanHelper2<CMP, kAnd>(column_block, bitvector);
        case kOr:
            return ScanHelper2<CMP, kOr>(column_block, bitvector);
    }
    return Status::InvalidArgument(
            "Unkown bit vector operator in BitWeaving/H scan.");
}

template<size_t CODE_SIZE>
template<Comparator CMP, BitVectorOpt OPT>
Status BwVColumnBlock<CODE_SIZE>::ScanHelper2(const ColumnBlock &column_block,
                                              BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(column_block.GetNumCodes() == num_);
    assert(column_block.GetColumnType() == kBitWeavingV);
    assert(column_block.GetCodeSize() == bit_width_);

    // Now safe to cast to BwHColumnBlock
    const BwVColumnBlock <CODE_SIZE> *other_block =
            static_cast<const BwVColumnBlock <CODE_SIZE> *>(&column_block);
    WordUnit *const *other_data = other_block->data_;
    for (size_t segment_offset = 0; segment_offset < num_used_words_;
         segment_offset += kNumWordsPerSegment) {
        size_t segment_id = segment_offset / kNumWordsPerSegment;
        WordUnit mask_less = 0;
        WordUnit mask_greater = 0;
        WordUnit mask_equal;

        WordUnit mask_bitvector = bitvector->GetWordUnit(segment_id);
        switch (OPT) {
            case kSet:
                mask_equal = -1ULL;
                break;
            case kAnd:
                mask_equal = mask_bitvector;
                break;
            case kOr:
                mask_equal = ~mask_bitvector;
                break;
        }

        if (kNumBitsPerCode >= kNumBitsPerGroup && mask_equal != 0) {
            ScanIteration<CMP, 0>(segment_offset, mask_equal, mask_less, mask_greater,
                                  other_data);
            if (kNumBitsPerCode >= kNumBitsPerGroup * 2 && mask_equal != 0) {
                ScanIteration<CMP, 1>(segment_offset, mask_equal, mask_less,
                                      mask_greater, other_data);
                if (kNumBitsPerCode >= kNumBitsPerGroup * 3 && mask_equal != 0) {
                    ScanIteration<CMP, 2>(segment_offset, mask_equal, mask_less,
                                          mask_greater, other_data);
                    if (kNumBitsPerCode >= kNumBitsPerGroup * 4 && mask_equal != 0) {
                        ScanIteration<CMP, 3>(segment_offset, mask_equal, mask_less,
                                              mask_greater, other_data);
                        if (kNumBitsPerCode >= kNumBitsPerGroup * 5
                            && mask_equal != 0) {
                            ScanIteration<CMP, 4>(segment_offset, mask_equal, mask_less,
                                                  mask_greater, other_data);
                            if (kNumBitsPerCode >= kNumBitsPerGroup * 6
                                && mask_equal != 0) {
                                ScanIteration<CMP, 5>(segment_offset, mask_equal, mask_less,
                                                      mask_greater, other_data);
                                if (kNumBitsPerCode >= kNumBitsPerGroup * 7
                                    && mask_equal != 0) {
                                    ScanIteration<CMP, 6>(segment_offset, mask_equal, mask_less,
                                                          mask_greater, other_data);
                                    if (kNumBitsPerCode >= kNumBitsPerGroup * 8
                                        && mask_equal != 0) {
                                        ScanIteration<CMP, 7>(segment_offset, mask_equal, mask_less,
                                                              mask_greater, other_data);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if (kNumBitsLastGroup != 0 && mask_equal != 0) {
            size_t last_group_segment_offset = segment_id * kNumBitsLastGroup;
            ScanIteration<CMP, kLastGroup>(last_group_segment_offset, mask_equal,
                                           mask_less, mask_greater, other_data);
        }

        WordUnit mask;
        switch (CMP) {
            case kEqual:
                mask = mask_equal;
                break;
            case kInequal:
                mask = ~mask_equal;
                break;
            case kGreater:
                mask = mask_greater;
                break;
            case kLess:
                mask = mask_less;
                break;
            case kGreaterEqual:
                mask = mask_greater | mask_equal;
                break;
            case kLessEqual:
                mask = mask_less | mask_equal;
                break;
        }

        switch (OPT) {
            case kSet:
                bitvector->SetWordUnit(segment_id, mask);
                break;
            case kAnd:
                bitvector->SetWordUnit(segment_id, mask & mask_bitvector);
                break;
            case kOr:
                bitvector->SetWordUnit(segment_id, mask | mask_bitvector);
                break;
        }
    }

    bitvector->Finalize();

    return Status::OK();
}

/**
 * explicit instantiations
 */
template
class BwVColumnBlock<1>;

template
class BwVColumnBlock<2>;

template
class BwVColumnBlock<3>;

template
class BwVColumnBlock<4>;

template
class BwVColumnBlock<5>;

template
class BwVColumnBlock<6>;

template
class BwVColumnBlock<7>;

template
class BwVColumnBlock<8>;

template
class BwVColumnBlock<9>;

template
class BwVColumnBlock<10>;

template
class BwVColumnBlock<11>;

template
class BwVColumnBlock<12>;

template
class BwVColumnBlock<13>;

template
class BwVColumnBlock<14>;

template
class BwVColumnBlock<15>;

template
class BwVColumnBlock<16>;

template
class BwVColumnBlock<17>;

template
class BwVColumnBlock<18>;

template
class BwVColumnBlock<19>;

template
class BwVColumnBlock<20>;

template
class BwVColumnBlock<21>;

template
class BwVColumnBlock<22>;

template
class BwVColumnBlock<23>;

template
class BwVColumnBlock<24>;

template
class BwVColumnBlock<25>;

template
class BwVColumnBlock<26>;

template
class BwVColumnBlock<27>;

template
class BwVColumnBlock<28>;

template
class BwVColumnBlock<29>;

template
class BwVColumnBlock<30>;

template
class BwVColumnBlock<31>;

template
class BwVColumnBlock<32>;

}  // namespace bitweaving
