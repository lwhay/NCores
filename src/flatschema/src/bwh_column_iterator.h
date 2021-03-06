// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_
#define BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_

#include <cassert>

#include "bitweaving/types.h"
#include "bitweaving/status.h"
#include "bitweaving/column.h"
#include "src/column_iterator.h"
#include "src/bwh_column_block.h"

namespace bitweaving {

/**
 * @brief Class for accessing codes in a BitWeaving/H column.
 * This class stores state information to speedup in-order accesses
 * on codes in this column.
 */
template<size_t CODE_SIZE>
class BwHColumnIterator : public ColumnIterator {
public:
    /**
     * @brief Constructs a new iterator for a column.
     * @ param column The column to access.
     */
    BwHColumnIterator(Column *const column);

    /**
     * @brief Destructor.
     */
    virtual ~BwHColumnIterator();

    /**
     * @brief Gets the code at the current position (TupleId)
     * in this column.
     * @param tuple_id The ID of the code to access.
     * @param code   Code to return.
     * @return A Status object to indicate success or failure.
     */
    virtual Status GetCode(TupleId tuple_id, Code &code);

    /**
     * @brief Sets the code at the current position (TupleId)
     * in this column.
     * @param tuple_id The ID of the code to set.
     * @param code   Code to set.
     * @return A Status object to indicate success or failure.
     */
    virtual Status SetCode(TupleId tuple_id, Code code);

private:
    inline void Seek(TupleId tuple_id);

    static const size_t kNumBitsPerCode = CODE_SIZE + 1;
    static const size_t kNumWordsPerSegment = CODE_SIZE + 1;
    static const size_t kNumCodesPerWord = kNumBitsPerWord
                                           / kNumBitsPerCode;
    static const size_t kNumCodesPerSegment = kNumWordsPerSegment
                                              * kNumCodesPerWord;
    static const WordUnit kCodeMask = (1ULL << CODE_SIZE) - 1;

    TupleId last_tuple_id_;
    size_t segment_word_id_;
    size_t code_id_in_segment_;
    size_t code_id_in_block_;
    size_t column_block_id_;
    BwHColumnBlock<CODE_SIZE> *column_block_;

    struct PrecomputedData {
        size_t word_id_in_segment;
        size_t shift_in_word;
    };

    PrecomputedData pre_data_[kNumCodesPerSegment];
};

}  // namespace bitweaving

#endif // BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_
