// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BWV_COLUMN_ITERATOR_H_
#define BITWEAVING_SRC_BWV_COLUMN_ITERATOR_H_

#include <cassert>

#include "bitweaving/column.h"
#include "bitweaving/status.h"
#include "bitweaving/types.h"
#include "src/bwv_column_block.h"
#include "src/column_iterator.h"
#include "src/macros.h"

namespace bitweaving {

/**
 * @brief Class for accessing codes in a BitWeaving/V column.
 * This class stores state information to speedup in-order accesses
 * on codes in this column.
 */
template<size_t CODE_SIZE>
class BwVColumnIterator : public ColumnIterator {
public:
    /**
     * @brief Constructs a new iterator for a column.
     * @ param column The column to access.
     */
    BwVColumnIterator(Column *const column);

    /**
     * @brief Destructor.
     */
    virtual ~BwVColumnIterator();

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
    inline void Seed(TupleId tuple_id);

    static const size_t kNumBitsPerCode = CODE_SIZE;
    static const size_t kMaxNumGroups = kNumBitsPerWord / kNumBitsPerGroup;
    static const size_t kNumGroups = CEIL(kNumBitsPerCode, kNumBitsPerGroup);
    static const size_t kNumFullGroups = kNumBitsPerCode / kNumBitsPerGroup;
    static const size_t kNumBitsLastGroup = kNumBitsPerCode
                                            - kNumBitsPerGroup * kNumFullGroups;
    static const size_t kLastGroup = kNumFullGroups;
    static const size_t kNumWordsPerSegment = kNumBitsPerGroup;
    static const size_t kNumCodesPerSegment = kNumBitsPerWord;
    static const size_t kNumSegments =
            CEIL(kNumCodesPerBlock, kNumCodesPerSegment);
    static const size_t kNumWords = kNumSegments * kNumWordsPerSegment;

    TupleId last_tuple_id_;
    size_t group_word_id_;
    size_t last_group_word_id_;
    size_t code_id_in_segment_;
    size_t code_id_in_block_;
    size_t column_block_id_;
    BwVColumnBlock<CODE_SIZE> *column_block_;
};

}  // namespace bitweaving

#endif // BITWEAVING_SRC_BWV_COLUMN_ITERATOR_H_
