// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_BITVECTOR_H_
#define BITWEAVING_INCLUDE_BITVECTOR_H_

#include <cassert>
#include <string>

#include "bitweaving/bitvector.h"
#include "bitweaving/status.h"
#include "bitweaving/table.h"
#include "bitweaving/types.h"
#include <vector>

using namespace std;
namespace bitweaving {

class BitVector;

class BitVectorIterator;

class BitVectorBlock;

class Column;

class Table;

/**
 * @brief Class of a bit-vector to indicate the results of a scan.
 */
class BitVector {
public:
    /**
     * @brief Construct a bit-vector for a table.
     * @param The table that this bit-vector is built on.
     */
    explicit BitVector(const Table &table);

    /**
     * @brief Destructor.
     */
    ~BitVector();

    /**
     * @brief Copy the bit-vector.
     * @param bit_vector The bit-vector to copy.
     */
    void Clone(const BitVector &bit_vector);

    /**
     * @brief Comparator
     * @param bit_vector The bit-vector to be compared with.
     * @return True iff the two bit-vectors are identical.
     */
    bool Equal(const BitVector &bit_vector) const;

    /**
     * @brief Get the number of bits in this bit-vector.
     * @return The number of bits in this bit-vector.
     */
    size_t GetNumBits() const;

    /**
     * @brief Get the table that this bit-vector is built on.
     * @return The table that this bit-vector is built on.
     */
    const Table &GetTable() const;

    /**
     * @brief Set the bit at the given position to the given value.
     * @param tuple_id The position of bit to set.
     * @param bit The value of bit to set.
     * @return A Status object to indicate success or failure.
     */
    Status SetBit(TupleId tuple_id, bool bit);

    /**
     * @brief Set the bit at the given position to the given value.
     * @param tuple_id The position of bit to set.
     * @param bit The value of bit to set.
     * @return A Status object to indicate success or failure.
     */
    void toTids(vector<uint32_t> *result, uint32_t maxSize);

    /**
     * @brief Get the bit at the given position.
     * @param tuple_id The position of bit to set.
     * @param bit The value of the returned bit value.
     * @return A Status object to indicate success or failure.
     */
    Status GetBit(TupleId tuple_id, bool *bit) const;

    /**
     * @brief Set all bits in this bit-vector to be 0-bit.
     */
    void SetZeros();

    /**
     * @brief Set all bits in this bit-vector to be 1-bit.
     */
    void SetOnes();

    /**
     * @brief Count the number of 1-bit in this bit-vector.
     * @return The number of 1-bit.
     */
    size_t CountOnes() const;

    /**
     * @brief Perform logical complement on this bit-vector.
     * Change all 1-bit to 0-bit, and 0-bit to 1-bit.
     */
    void Complement();

    /**
     * @brief Perform logical AND with the input bit-vector.
     * @param bit_vector The input bit-vector for logical AND operation.
     * @return A Status object to indicate success or failure.
     */
    Status And(const BitVector &bit_vector);

    /**
     * @brief Perform logical OR with the input bit-vector.
     * @param bit_vector The input bit-vector for logical OR operation.
     * @return A Status object to indicate success or failure.
     */
    Status Or(const BitVector &bit_vector);

    /**
     * @brief Conver this bit-vector to a string.
     * @warning This function is only used for debugging.
     * @return A string that represents this bit-vector.
     */
    std::string ToString() const;

    /**
     * @brief Save the bitvector into a file.
     * @param path The path of the file holds the data of this bitvector.
     * @return A Status object to indicate success or failure.
     */
    Status Save(const std::string &path) const;

    /**
     * @brief Load the bitvector from a file.
     * @param path The path of the file holds the data of this bitvector.
     * @return A Status object to indicate success or failure.
     */
    Status Load(const std::string &path);

private:
    /**
     * @brief Get the number of BitVectorBlock in this bit-vector.
     * Each BitVectorBlock is a block that contains a fixed-length bitmap.
     * @return The block with the given ID.
     */
    size_t GetNumBitVectorBlock() const;

    /**
     * @brief Get a BitVectorBlock by its ID.
     * @param block_id The ID to search for.
     * @return The block with the given ID.
     */
    BitVectorBlock *GetBitVectorBlock(size_t block_id) const;

    struct Rep;
    Rep *rep_;

    friend class BitVectorIterator;

    friend class Column;
};

}  // namespace bitweaving

#endif  // BITWEAVING_INCLUDE_BITVECTOR_H_
