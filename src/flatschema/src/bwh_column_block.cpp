// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <cstring>

#include "src/bwh_column_block.h"

#include "src/file.h"
#include "src/macros.h"
#include "src/utility.h"

namespace bitweaving {

template<size_t CODE_SIZE>
BwHColumnBlock<CODE_SIZE>::BwHColumnBlock()
        : ColumnBlock(kBitWeavingH, CODE_SIZE),
          num_used_words_(0) {
    data_ = new WordUnit[kNumWords];
    assert(data_ != NULL);
    memset(data_, 0, sizeof(WordUnit) * kNumWords);
}

template<size_t CODE_SIZE>
BwHColumnBlock<CODE_SIZE>::~BwHColumnBlock() {
    delete[] data_;
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::GetCode(TupleId pos, Code *code) const {
    if (pos >= num_)
        return Status::InvalidArgument("GetCode: position overflow");

    size_t segment_id = pos / kNumCodesPerSegment;
    size_t code_id_in_segment = pos % kNumCodesPerSegment;
    size_t word_id = segment_id * kNumWordsPerSegment
                     + code_id_in_segment % kNumWordsPerSegment;
    size_t code_id_in_word = code_id_in_segment / kNumWordsPerSegment;

    static const WordUnit mask = (1ULL << CODE_SIZE) - 1;
    assert(word_id < kNumWords);
    int shift = (kNumCodesPerWord - 1 - code_id_in_word) * kNumBitsPerCode;
    *code = (data_[word_id] >> shift) & mask;
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::SetCode(TupleId pos, Code code) {
    if (pos >= num_)
        return Status::InvalidArgument("GetCode: position overflow");

    size_t segment_id = pos / kNumCodesPerSegment;
    size_t code_id_in_segment = pos % kNumCodesPerSegment;
    size_t word_id = segment_id * kNumWordsPerSegment
                     + code_id_in_segment % kNumWordsPerSegment;
    size_t code_id_in_word = code_id_in_segment / kNumWordsPerSegment;

    assert(word_id < kNumWords);
    int shift = (kNumCodesPerWord - 1 - code_id_in_word) * kNumBitsPerCode;
    data_[word_id] |= (WordUnit) code << shift;

    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Resize(size_t num) {
    assert(num <= kNumCodesPerBlock);
    num_ = num;
    num_used_words_ = CEIL(num_, kNumCodesPerSegment) * kNumWordsPerSegment;
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Bulkload(const Code *codes, size_t num, size_t start_tuple_id) {
    assert(start_tuple_id + num <= num_);

    for (size_t i = 0; i < num; i++) {
        SetCode(start_tuple_id + i, codes[i]);
    }
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Load(SequentialReadFile &file) {
    Status status;

    status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(&num_used_words_),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(data_),
                       sizeof(WordUnit) * kNumWords);
    if (!status.IsOk())
        return status;

    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Save(SequentialWriteFile &file) {
    Status status;

    status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(&num_used_words_),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<char *>(data_),
                         sizeof(WordUnit) * kNumWords);
    if (!status.IsOk())
        return status;

    return Status::OK();
}


template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Scan(Comparator comparator, Code literal,
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
Status BwHColumnBlock<CODE_SIZE>::Scan_between(Code literal1, Code literal2,
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
Status BwHColumnBlock<CODE_SIZE>::ScanHelper2_between(Code literal1, Code literal2,
                                                      BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(literal1 < (1LL << bit_width_));
    assert(literal2 < (1LL << bit_width_));

    // Generate masks
    // k = number of bits per code
    // base_mask: 0^k 1 0^k1 1 ... 0^k 1
    // predicate_mask: 0 code 0 code ... 0 code
    // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
    // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
    WordUnit base_mask = 0;
    for (size_t i = 0; i < kNumCodesPerWord; i++) {
        base_mask = (base_mask << kNumBitsPerCode) | 1ULL;
    }
    WordUnit complement_mask = base_mask * ((1ULL << bit_width_) - 1ULL);
    WordUnit result_mask = base_mask << bit_width_;

    WordUnit less_than_mask = base_mask * literal1;
    WordUnit greater_than_mask = (base_mask * literal2) ^complement_mask;
    //WordUnit equal_mask = base_mask    * (~literal & (-1ULL >> (kNumBitsPerWord - bit_width_)));
    // WordUnit inequal_mask = base_mask * literal;

    // Done for initialization

    WordUnit word_bitvector, output_bitvector = 0;
    int64_t output_offset = 0;
    size_t output_word_id = 0;
    for (size_t segment_offset = 0; segment_offset < num_used_words_; segment_offset += kNumWordsPerSegment) {
        WordUnit segment_bitvector = 0;
        size_t word_id = 0;

        // A loop over all words inside a segment.
        // We break down the loop into several fixed-length small loops, making it easy to
        // unroll these loops.
        // This optimization provides about ~10% improvement
        for (size_t bit_group_id = 0; bit_group_id < kNumWordGroups;
             bit_group_id++) {
            // Prefetch data into CPU data cache
            // This optimization provides about ~10% improvement
            __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);

            for (size_t bit = 0; bit < kNumWordsPerWordGroup; bit++) {
                word_bitvector = ~(less_than_mask
                                   + (data_[segment_offset + word_id] ^ complement_mask))
                                 & result_mask & ~(data_[segment_offset + word_id]
                                                   + greater_than_mask);

                segment_bitvector |= word_bitvector >> word_id++;
            }
        }

        // Prefetch
        __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);

        // A small loop on the rest of bit positions
        for (size_t bit = 0; bit < kNumUngroupedWords; bit++) {

            word_bitvector = ~(less_than_mask
                               + (data_[segment_offset + word_id] ^ complement_mask))
                             & result_mask & ~(data_[segment_offset + word_id] + greater_than_mask);
            segment_bitvector |= word_bitvector >> word_id++;
        }

        output_bitvector |= (segment_bitvector << kNumEmptyBits) >> output_offset;
        output_offset += kNumCodesPerSegment;

        if (output_offset >= (int64_t) kNumBitsPerWord) {
            switch (OPT) {
                case kSet:
                    break;
                case kAnd:
                    output_bitvector &= bitvector->GetWordUnit(output_word_id);
                    break;
                case kOr:
                    output_bitvector |= bitvector->GetWordUnit(output_word_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector->SetWordUnit(output_word_id, output_bitvector);

            output_offset -= kNumBitsPerWord;
            size_t output_shift = kNumBitsPerWord - output_offset;
            output_bitvector = segment_bitvector << output_shift;
            //clear up outputBitvector if outputOffset = NUM_BITS_PER_WORD
            output_bitvector &= 0ULL - (output_shift != kNumBitsPerWord);
            output_word_id++;
        }
    }

    output_offset -= num_used_words_ * kNumCodesPerWord - num_;
    if (output_offset > 0) {
        switch (OPT) {
            case kSet:
                break;
            case kAnd:
                output_bitvector &= bitvector->GetWordUnit(output_word_id);
                break;
            case kOr:
                output_bitvector |= bitvector->GetWordUnit(output_word_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(output_word_id, output_bitvector);
    }

    bitvector->Finalize();
    return Status::OK();
}


template<size_t CODE_SIZE>
template<Comparator CMP>
Status BwHColumnBlock<CODE_SIZE>::ScanHelper1(Code literal, BitVectorBlock *bitvector,
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
Status BwHColumnBlock<CODE_SIZE>::ScanHelper2(Code literal,
                                              BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(literal < (1LL << bit_width_));

    // Generate masks
    // k = number of bits per code
    // base_mask: 0^k 1 0^k1 1 ... 0^k 1
    // predicate_mask: 0 code 0 code ... 0 code
    // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
    // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
    WordUnit base_mask = 0;
    for (size_t i = 0; i < kNumCodesPerWord; i++) {
        base_mask = (base_mask << kNumBitsPerCode) | 1ULL;
    }
    WordUnit complement_mask = base_mask * ((1ULL << bit_width_) - 1ULL);
    WordUnit result_mask = base_mask << bit_width_;

    WordUnit less_than_mask = base_mask * literal;
    WordUnit greater_than_mask = (base_mask * literal) ^complement_mask;
    WordUnit equal_mask = base_mask
                          * (~literal & (-1ULL >> (kNumBitsPerWord - bit_width_)));
    WordUnit inequal_mask = base_mask * literal;

    // Done for initialization

    WordUnit word_bitvector, output_bitvector = 0;
    int64_t output_offset = 0;
    size_t output_word_id = 0;
    for (size_t segment_offset = 0; segment_offset < num_used_words_;
         segment_offset += kNumWordsPerSegment) {
        WordUnit segment_bitvector = 0;
        size_t word_id = 0;

        // A loop over all words inside a segment.
        // We break down the loop into several fixed-length small loops, making it easy to
        // unroll these loops.
        // This optimization provides about ~10% improvement
        for (size_t bit_group_id = 0; bit_group_id < kNumWordGroups;
             bit_group_id++) {
            // Prefetch data into CPU data cache
            // This optimization provides about ~10% improvement
            __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);

            for (size_t bit = 0; bit < kNumWordsPerWordGroup; bit++) {
                switch (CMP) {
                    case kEqual:
                        word_bitvector = ((data_[segment_offset + word_id] ^ equal_mask)
                                          + base_mask) & result_mask;
                        break;
                    case kInequal:
                        word_bitvector = ((data_[segment_offset + word_id] ^ inequal_mask)
                                          + complement_mask) & result_mask;
                        break;
                    case kGreater:
                        word_bitvector = (data_[segment_offset + word_id]
                                          + greater_than_mask) & result_mask;
                        break;
                    case kLess:
                        word_bitvector = (less_than_mask
                                          + (data_[segment_offset + word_id] ^ complement_mask))
                                         & result_mask;
                        break;
                    case kGreaterEqual:
                        word_bitvector = ~(less_than_mask
                                           + (data_[segment_offset + word_id] ^ complement_mask))
                                         & result_mask;
                        break;
                    case kLessEqual:
                        word_bitvector = ~(data_[segment_offset + word_id]
                                           + greater_than_mask) & result_mask;
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                segment_bitvector |= word_bitvector >> word_id++;
            }
        }

        // Prefetch
        __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);

        // A small loop on the rest of bit positions
        for (size_t bit = 0; bit < kNumUngroupedWords; bit++) {
            switch (CMP) {
                case kEqual:
                    word_bitvector = ((data_[segment_offset + word_id] ^ equal_mask)
                                      + base_mask) & result_mask;
                    break;
                case kInequal:
                    word_bitvector = ((data_[segment_offset + word_id] ^ inequal_mask)
                                      + complement_mask) & result_mask;
                    break;
                case kGreater:
                    word_bitvector = (data_[segment_offset + word_id] + greater_than_mask)
                                     & result_mask;
                    break;
                case kLess:
                    word_bitvector = (less_than_mask
                                      + (data_[segment_offset + word_id] ^ complement_mask))
                                     & result_mask;
                    break;
                case kGreaterEqual:
                    word_bitvector = ~(less_than_mask
                                       + (data_[segment_offset + word_id] ^ complement_mask))
                                     & result_mask;
                    break;
                case kLessEqual:
                    word_bitvector =
                            ~(data_[segment_offset + word_id] + greater_than_mask)
                            & result_mask;
                    break;
                default:
                    return Status::InvalidArgument("Unknown comparator.");
            }
            segment_bitvector |= word_bitvector >> word_id++;
        }

        output_bitvector |= (segment_bitvector << kNumEmptyBits) >> output_offset;
        output_offset += kNumCodesPerSegment;

        if (output_offset >= (int64_t) kNumBitsPerWord) {
            switch (OPT) {
                case kSet:
                    break;
                case kAnd:
                    output_bitvector &= bitvector->GetWordUnit(output_word_id);
                    break;
                case kOr:
                    output_bitvector |= bitvector->GetWordUnit(output_word_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector->SetWordUnit(output_word_id, output_bitvector);

            output_offset -= kNumBitsPerWord;
            size_t output_shift = kNumBitsPerWord - output_offset;
            output_bitvector = segment_bitvector << output_shift;
            //clear up outputBitvector if outputOffset = NUM_BITS_PER_WORD
            output_bitvector &= 0ULL - (output_shift != kNumBitsPerWord);
            output_word_id++;
        }
    }

    output_offset -= num_used_words_ * kNumCodesPerWord - num_;
    if (output_offset > 0) {
        switch (OPT) {
            case kSet:
                break;
            case kAnd:
                output_bitvector &= bitvector->GetWordUnit(output_word_id);
                break;
            case kOr:
                output_bitvector |= bitvector->GetWordUnit(output_word_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(output_word_id, output_bitvector);
    }

    bitvector->Finalize();
    return Status::OK();
}

template<size_t CODE_SIZE>
Status BwHColumnBlock<CODE_SIZE>::Scan(Comparator comparator,
                                       const ColumnBlock &column_block,
                                       BitVectorBlock *bitvector,
                                       BitVectorOpt opt) {
    if (bitvector->GetNum() != num_)
        return Status::InvalidArgument(
                "Bit vector length does not match column length.");
    if (column_block.GetNumCodes() != num_)
        return Status::InvalidArgument("Column length does not match.");
    if (column_block.GetColumnType() != kBitWeavingH)
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
Status BwHColumnBlock<CODE_SIZE>::ScanHelper1(const ColumnBlock &column_block,
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
Status BwHColumnBlock<CODE_SIZE>::ScanHelper2(const ColumnBlock &column_block,
                                              BitVectorBlock *bitvector) {
    assert(bitvector->GetNum() == num_);
    assert(column_block.GetNumCodes() == num_);
    assert(column_block.GetColumnType() == kBitWeavingH);
    assert(column_block.GetCodeSize() == bit_width_);

    // Now safe to cast to BwHColumnBlock
    const BwHColumnBlock <CODE_SIZE> *other_block =
            static_cast<const BwHColumnBlock <CODE_SIZE> *>(&column_block);

    size_t min_used_words = std::min(num_used_words_,
                                     other_block->num_used_words_);

    // Generate masks
    // k = number of bits per code
    // base_mask: 0^k 1 0^k1 1 ... 0^k 1
    // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
    // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
    WordUnit base_mask = 0;
    for (size_t i = 0; i < kNumCodesPerWord; i++) {
        base_mask = (base_mask << kNumBitsPerCode) | 1ULL;
    }
    WordUnit complement_mask = base_mask * ((1ULL << bit_width_) - 1ULL);
    WordUnit result_mask = base_mask << bit_width_;

    // Done for initialization

    WordUnit word_bitvector, output_bitvector = 0;
    int64_t output_offset = 0;
    size_t output_word_id = 0;
    WordUnit *other_data = other_block->data_;
    for (size_t segment_offset = 0; segment_offset < min_used_words;
         segment_offset += kNumWordsPerSegment) {
        WordUnit segment_bitvector = 0;
        size_t word_id = 0;

        // A loop over all words inside a segment.
        // We break down the loop into several fixed-length small loops, making it easy to
        // unroll these loops.
        // This optimization provides about ~10% improvement
        for (size_t bit_group_id = 0; bit_group_id < kNumWordGroups;
             bit_group_id++) {
            // Prefetch data into CPU data cache
            // This optimization provides about ~10% improvement
            __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);
            __builtin_prefetch(
                    other_data + segment_offset + word_id + kPrefetchDistance);

            for (size_t bit = 0; bit < kNumWordsPerWordGroup; bit++) {
                size_t id = segment_offset + word_id;
                switch (CMP) {
                    case kEqual:
                        word_bitvector = ~((data_[id] ^ other_data[id]) + complement_mask)
                                         & result_mask;
                        break;
                    case kInequal:
                        word_bitvector = ((data_[id] ^ other_data[id]) + complement_mask)
                                         & result_mask;
                        break;
                    case kGreater:
                        word_bitvector = (data_[id] + (other_data[id] ^ complement_mask))
                                         & result_mask;
                        break;
                    case kLess:
                        word_bitvector = (other_data[id] + (data_[id] ^ complement_mask))
                                         & result_mask;
                        break;
                    case kGreaterEqual:
                        word_bitvector = (~(other_data[id] + (data_[id] ^ complement_mask)))
                                         & result_mask;
                        break;
                    case kLessEqual:
                        word_bitvector = (~(data_[id] + (other_data[id] ^ complement_mask)))
                                         & result_mask;
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                segment_bitvector |= word_bitvector >> word_id++;
            }
        }

        // Prefetch
        __builtin_prefetch(data_ + segment_offset + word_id + kPrefetchDistance);
        __builtin_prefetch(
                other_data + segment_offset + word_id + kPrefetchDistance);

        // A small loop on the rest of bit positions
        for (size_t bit = 0; bit < kNumUngroupedWords; bit++) {
            size_t id = segment_offset + word_id;
            switch (CMP) {
                case kEqual:
                    word_bitvector = ~((data_[id] ^ other_data[id]) + complement_mask)
                                     & result_mask;
                    break;
                case kInequal:
                    word_bitvector = ((data_[id] ^ other_data[id]) + complement_mask)
                                     & result_mask;
                    break;
                case kGreater:
                    word_bitvector = (data_[id] + (other_data[id] ^ complement_mask))
                                     & result_mask;
                    break;
                case kLess:
                    word_bitvector = (other_data[id] + (data_[id] ^ complement_mask))
                                     & result_mask;
                    break;
                case kGreaterEqual:
                    word_bitvector = (~(other_data[id] + (data_[id] ^ complement_mask)))
                                     & result_mask;
                    break;
                case kLessEqual:
                    word_bitvector = (~(data_[id] + (other_data[id] ^ complement_mask)))
                                     & result_mask;
                    break;
                default:
                    return Status::InvalidArgument("Unknown comparator.");
            }
            segment_bitvector |= word_bitvector >> word_id++;
        }

        output_bitvector |= (segment_bitvector << kNumEmptyBits) >> output_offset;
        output_offset += kNumCodesPerSegment;

        if (output_offset >= (int64_t) kNumBitsPerWord) {
            switch (OPT) {
                case kSet:
                    break;
                case kAnd:
                    output_bitvector &= bitvector->GetWordUnit(output_word_id);
                    break;
                case kOr:
                    output_bitvector |= bitvector->GetWordUnit(output_word_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector->SetWordUnit(output_word_id, output_bitvector);

            output_offset -= kNumBitsPerWord;
            size_t output_shift = kNumBitsPerWord - output_offset;
            output_bitvector = segment_bitvector << output_shift;
            //clear up outputBitvector if outputOffset = NUM_BITS_PER_WORD
            output_bitvector &= 0ULL - (output_shift != kNumBitsPerWord);
            output_word_id++;
        }
    }

    output_offset -= num_used_words_ * kNumCodesPerWord - num_;
    if (output_offset > 0) {
        switch (OPT) {
            case kSet:
                break;
            case kAnd:
                output_bitvector &= bitvector->GetWordUnit(output_word_id);
                break;
            case kOr:
                output_bitvector |= bitvector->GetWordUnit(output_word_id);
                break;
            default:
                return Status::InvalidArgument("Unknown bit vector operator.");
        }
        bitvector->SetWordUnit(output_word_id, output_bitvector);
    }

    bitvector->Finalize();
    return Status::OK();
}

/**
 * @brief explicit instantiations.
 */
template
class BwHColumnBlock<1>;

template
class BwHColumnBlock<2>;

template
class BwHColumnBlock<3>;

template
class BwHColumnBlock<4>;

template
class BwHColumnBlock<5>;

template
class BwHColumnBlock<6>;

template
class BwHColumnBlock<7>;

template
class BwHColumnBlock<8>;

template
class BwHColumnBlock<9>;

template
class BwHColumnBlock<10>;

template
class BwHColumnBlock<11>;

template
class BwHColumnBlock<12>;

template
class BwHColumnBlock<13>;

template
class BwHColumnBlock<14>;

template
class BwHColumnBlock<15>;

template
class BwHColumnBlock<16>;

template
class BwHColumnBlock<17>;

template
class BwHColumnBlock<18>;

template
class BwHColumnBlock<19>;

template
class BwHColumnBlock<20>;

template
class BwHColumnBlock<21>;

template
class BwHColumnBlock<22>;

template
class BwHColumnBlock<23>;

template
class BwHColumnBlock<24>;

template
class BwHColumnBlock<25>;

template
class BwHColumnBlock<26>;

template
class BwHColumnBlock<27>;

template
class BwHColumnBlock<28>;

template
class BwHColumnBlock<29>;

template
class BwHColumnBlock<30>;

template
class BwHColumnBlock<31>;

template
class BwHColumnBlock<32>;

}  // namespace bitweaving

