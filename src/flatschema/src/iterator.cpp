// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <cassert>

#include "src/bitwise.h"
#include "src/bitvector_iterator.h"
#include "src/column_iterator.h"

namespace bitweaving {

/**
 * @brief Structure for private members of Class Iterator.
 */
struct Iterator::Rep {
    Rep(const BitVector &bit_vector)
            : bitvector(&bit_vector),
              is_bitvector_owner(false),
              bitvector_iterator(bit_vector) {
        const Table &table = bitvector->GetTable();
        max_column_id = table.GetMaxColumnId();
        column_iterators = new ColumnIterator *[max_column_id];
        for (ColumnId id = 0; id < max_column_id; id++) {
            column_iterators[id] = table.GetColumn(id)->CreateColumnIterator();
        }
    }

    Rep(const Table &table)
            : bitvector(new BitVector(table)),
              is_bitvector_owner(true),
              bitvector_iterator(*bitvector) {
        max_column_id = table.GetMaxColumnId();
        column_iterators = new ColumnIterator *[max_column_id];
        for (ColumnId id = 0; id < max_column_id; id++) {
            column_iterators[id] = table.GetColumn(id)->CreateColumnIterator();
        }
    }

    ~Rep() {
        delete[] column_iterators;
        if (is_bitvector_owner) {
            delete bitvector;
        }
    }

    /**
     * @brief The associated bit-vector.
     */
    const BitVector *bitvector;
    /**
     * @brief Is this structure the owner of the bit-vector.
     */
    bool is_bitvector_owner;
    /**
     * @brief A bit-vector iterator on its associated bit-vector.
     */
    BitVectorIterator bitvector_iterator;
    /**
     * @brief The number of columns in its associated table.
     */
    ColumnId max_column_id;
    /**
     * @brief A list of column iterators on columns of its associated table.
     */
    ColumnIterator **column_iterators;
};

Iterator::Iterator(const Table &table)
        : rep_(new Rep(table)) {
}

Iterator::Iterator(const BitVector &bitvector)
        : rep_(new Rep(bitvector)) {
}

Iterator::~Iterator() {
    delete rep_;
}

void Iterator::Begin() {
    rep_->bitvector_iterator.Begin();
}

bool Iterator::Next() {
    return rep_->bitvector_iterator.Next();
}

TupleId Iterator::GetPosition() const {
    return rep_->bitvector_iterator.GetPosition();
}

Status Iterator::GetCode(const Column &column, Code *code) const {
    ColumnId column_id = column.GetColumnId();
    assert(column_id < rep_->max_column_id);
    return rep_->column_iterators[column_id]->GetCode(
            rep_->bitvector_iterator.GetPosition(), code);
}

Status Iterator::SetCode(const Column &column, Code code) const {
    ColumnId column_id = column.GetColumnId();
    assert(column_id < rep_->max_column_id);
    return rep_->column_iterators[column_id]->SetCode(
            rep_->bitvector_iterator.GetPosition(), code);
}

}  // namespace bitweaving

