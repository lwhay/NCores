// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_ITERATOR_H_
#define BITWEAVING_INCLUDE_ITERATOR_H_

#include <cassert>

#include "bitweaving/bitvector.h"
#include "bitweaving/column.h"
#include "bitweaving/status.h"
#include "bitweaving/table.h"
#include "bitweaving/types.h"

namespace bitweaving {

class BitVector;

class Column;

class Table;

/**
 * @brief Class for accessing codes in a table for matching
 * tuples defined by a bit vector.
 */
class Iterator {
public:
    /**
     * @brief Constructs a new iterator for the input bit-vector.
     * This iterator accesses every matching tuple indicated by this
     * bit-vector.
     * @ param bitvector BitVector object that is used to indicate
     * matching tuples
     */
    Iterator(const BitVector &bitvector);

    /**
     * @brief Constructs a new iterator for the input table. This
     * iterator accesses every tuple in this table.
     * @ param bitvector BitVector object that is used to indicate
     * matching tuples
     */
    Iterator(const Table &table);

    /**
     * @brief Destructor.
     */
    ~Iterator();

    /**
     * @brief Seeks to the position of the first tuple in the table.
     */
    void Begin();

    /**
     * @brief Moves to the position of the next matching tuple in the
     * table, whose associated bit is set in the input bit vector.
     * @return Ture iff the next tuple exists.
     */
    bool Next();

    /**
     * @brief Get the current tuple ID of this iterator.
     * @return The current TupleId.
     */
    TupleId GetPosition() const;

    /**
     * @brief Gets the code at the current position (TupleId) in the
     * input column.
     * @param column Column object to access.
     * @param code   Code to return.
     * @return A Status object to indicate success or failure.
     */
    Status GetCode(const Column &column, Code *code) const;

    /**
     * @brief Sets the code at the current position (TupleId) in the
     * input column.
     * @param column Column object to set.
     * @param code   Code to set.
     * @return A Status object to indicate success or failure.
     */
    Status SetCode(const Column &column, Code code) const;

private:
    struct Rep;
    Rep *rep_;
};

}  // namespace bitweaving

#endif  // BITWEAVING_INCLUDE_ITERATOR_H_
