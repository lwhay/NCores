// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_
#define BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_

#include <cassert>

#include "bitweaving/status.h"
#include "bitweaving/types.h"
#include "gtest/gtest_prod.h"
#include "src/column_block.h"
#include "src/bitvector_block.h"
#include "src/macros.h"

namespace bitweaving {

template<size_t CODE_SIZE>
class BwHColumnIterator;

/**
 * @brief Class for storing and accessing codes in a fixed-length
 * BitWeaving/H column block. We use template to generate specialized
 * class for each code size (1~32 bits). The template prarameter (the
 * code size) is a compile-time const, thus enables many compilation
 * optimizations to generate code.
 */
template<size_t CODE_SIZE>
class BwHColumnBlock : public ColumnBlock {
public:
    /**
     * @brief Constructor.
     */
    BwHColumnBlock();

    /**
     * @brief Destructor.
     */
    virtual ~BwHColumnBlock();

    /**
     * @brief Reset the number of codes in this column block.
     * @param num The number of codes in this column block.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Resize(size_t num);

    /**
     * @brief Load an set of codes into this column block. Each code in this array
     * should be in the domain of the codes, i.e. the number of bits that are
     * used to represent each code should be less than or equal to GetBitWidth().
     * @param codes The starting address of the input code array.
     * @param num The number of codes in the input code array.
     * @param start_tuple_id The starting tuple id that the batch is inserted to.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Bulkload(const Code *codes, size_t num, TupleId start_tuple_id);

    /**
     * @brief Load this column block from the file system.
     * @param filename The name of data file.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Load(SequentialReadFile &file);

    /**
     * @brief Save this column block to the file system.
     * @param filename The name of data file.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Save(SequentialWriteFile &file);

    /**
     * @brief Scan this column block with an input literal-based predicate. The scan results
     * are combined with the input bit-vector block based on a logical operator.
     * @param comparator The comparator between column codes and the literal.
     * @param literal The code to be compared with.
     * @param bitvector The bit-vector block to be updated based on scan results.
     * @param opt The operator indicates how to combine the scan results to the input
     * bit-vector block.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Scan(Comparator comparator, Code literal,
                        BitVectorBlock *bitvector, BitVectorOpt opt = kSet);

    virtual Status Scan_between(Code literal1, Code literal2,
                                BitVectorBlock *bitvector, BitVectorOpt opt = kSet);

    /**
     * @brief Scan this column block with an predicate that compares this column block with
     * another column block. The scan results are combined with the input bit-vector block
     * based on a logical operator.
     * @param comparator The comparator between column codes and the literal.
     * @param column_block The column block to be compared with.
     * @param bitvector The bit-vector block to be updated based on scan results.
     * @param opt The operator indicates how to combine the scan results to the input
     * bit-vector block.
     * @return A Status object to indicate success or failure.
     */
    virtual Status Scan(Comparator comparator, const ColumnBlock &column_block,
                        BitVectorBlock *bitvector, BitVectorOpt opt = kSet);

    /**
     * @brief Get a code at a given position.
     * @warning This function is used for debugging. It is much more efficient to use
     *  Iterator::GetCode.
     * @param pos The code position to get.
     * @param code The code value.
     * @return A Status object to indicate success or failure.
     */
    virtual Status GetCode(TupleId pos, Code *code) const;

    /**
     * @brief Set a code at a given position.
     * @warning This function is used for debugging. It is much more efficient to use
     *  Iterator::SetCode.
     * @param pos The code position to set.
     * @param code The code value.
     * @return A Status object to indicate success or failure.
     */
    virtual Status SetCode(TupleId pos, Code code);

private:
    template<Comparator CMP>
    Status ScanHelper1(Code literal, BitVectorBlock *bitvector, BitVectorOpt opt);

    template<Comparator CMP, BitVectorOpt OPT>
    Status ScanHelper2(Code literal, BitVectorBlock *bitvector);


    template<BitVectorOpt OPT>
    Status ScanHelper2_between(Code literal, Code literal2, BitVectorBlock *bitvector);


    template<Comparator CMP>
    Status ScanHelper1(const ColumnBlock &column_block, BitVectorBlock *bitvector,
                       BitVectorOpt opt);

    template<Comparator CMP, BitVectorOpt OPT>
    Status ScanHelper2(const ColumnBlock &column_block, BitVectorBlock *bitvector);

    static const size_t kNumBitsPerCode = CODE_SIZE + 1;
    static const size_t kNumWordsPerSegment = CODE_SIZE + 1;
    static const size_t kNumCodesPerWord = kNumBitsPerWord
                                           / kNumBitsPerCode;
    static const size_t kNumEmptyBits = kNumBitsPerWord
                                        - kNumCodesPerWord * kNumBitsPerCode;
    static const size_t kNumCodesPerSegment = kNumWordsPerSegment
                                              * kNumCodesPerWord;
    static const size_t kNumSegments =
            CEIL(kNumCodesPerBlock, kNumCodesPerSegment);
    static const size_t kNumWords = kNumSegments * kNumWordsPerSegment;
    static const size_t kNumWordsPerWordGroup = 7;
    static const size_t kNumWordGroups = kNumWordsPerSegment
                                         / kNumWordsPerWordGroup;
    static const size_t kNumUngroupedWords = kNumWordsPerSegment
                                             - kNumWordsPerWordGroup * kNumWordGroups;
    static const size_t kPrefetchDistance = 64;

    /**
     * @brief The data space.
     */
    WordUnit *data_;
    /**
     * @brief The size of data space.
     */
    size_t num_used_words_;

    friend class BwHColumnIterator<CODE_SIZE>;

    FRIEND_TEST(BwHColumnBlockTest, FileOperationsTest
    );
};

}  // namespace bitweaving

#endif // BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_
