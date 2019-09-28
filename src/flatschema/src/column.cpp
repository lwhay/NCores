// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <iostream>

#include "bitweaving/column.h"

#include "src/bitvector_block.h"
#include "src/bwh_column_block.h"
#include "src/bwh_column_iterator.h"
#include "src/bwv_column_block.h"
#include "src/bwv_column_iterator.h"
#include "src/column_block.h"
#include "src/column_iterator.h"
#include "src/file.h"
#include "src/macros.h"
#include "src/naive_column_block.h"

namespace bitweaving {

/**
 * @brief Structure for private members of Class Column.
 */
struct Column::Rep {
    Rep(Table *table, ColumnId id, ColumnType type, size_t code_size) : table(table), id(id), type(type),
                                                                        code_size(code_size) {}

    /**
     * @brief The ID of this column.
     */
    Table *table;
    /**
     * @brief The ID of this column.
     */
    ColumnId id;
    /**
     * @brief The type of this column (Naive/BitWeavingH/BitWeavingV).
     */
    ColumnType type;
    /**
     * @brief The size of code in this column.
     */
    size_t code_size;
    /**
     * @brief A list of column blocks.
     */
    std::vector<ColumnBlock *> blocks;
};

Column::Column(Table *table, ColumnId id, ColumnType type, size_t code_size) : rep_(
        new Rep(table, id, type, code_size)) {}

Column::Column(Table *table, ColumnId id) : rep_(new Rep(table, id, kNaive, 0)) {}

Column::~Column() {
    delete rep_;
}

ColumnType Column::GetType() const {
    return rep_->type;
}

size_t Column::GetCodeSize() const {
    return rep_->code_size;
}

ColumnId Column::GetColumnId() const {
    return rep_->id;
}

const Table &Column::GetTable() const {
    return *const_cast<const Table *>(rep_->table);
}

Status Column::Bulkload(const Code *codes, size_t num) {
    return Bulkload(codes, num, 0);
}

Status Column::Bulkload(const Code *codes, size_t num, TupleId pos) {
    assert(CEIL(rep_->table->GetNumTuples(), kNumCodesPerBlock) == rep_->blocks.size());
    size_t block_id = pos / kNumCodesPerBlock;
    size_t code_id_in_block = pos % kNumCodesPerBlock;
    size_t num_tuples = rep_->table->GetNumTuples();
    const Code *data_ptr = codes;

    if (pos + num > num_tuples) {
        num_tuples = pos + num;
        rep_->table->Resize(num_tuples);
    }

    size_t num_remain_tuples = num;
    while (num_remain_tuples > 0) {
        assert(block_id < rep_->blocks.size());
        size_t size = std::min(num_remain_tuples, rep_->blocks[block_id]->GetNumCodes() - code_id_in_block);
        rep_->blocks[block_id]->Bulkload(data_ptr, size, code_id_in_block);
        data_ptr += size;
        num_remain_tuples -= size;
        code_id_in_block = 0;
        block_id++;
    }

    return Status::OK();
}

Status Column::Scan(Code literal, Comparator comparator, BitVectorOpt opt, BitVector *bitvector) const {
    Status status;
    for (uint32_t i = 0; i < bitvector->GetNumBitVectorBlock(); i++) {
        if (i < rep_->blocks.size() && rep_->blocks[i] != NULL) {
            status = rep_->blocks[i]->Scan(comparator, literal,
                                           bitvector->GetBitVectorBlock(i), opt);
            if (!status.IsOk())
                return status;
        } else {
            // The block has not been allocated. All Null values.
            if (opt == kAnd || opt == kSet) {
                bitvector->GetBitVectorBlock(i)->SetZeros();
            }
        }
    }
    return Status::OK();
}

Status Column::Scan_between(Code literal1, Code literal2, BitVectorOpt opt, BitVector *bitvector) const {
    Status status;
    for (uint32_t i = 0; i < bitvector->GetNumBitVectorBlock(); i++) {
        if (i < rep_->blocks.size() && rep_->blocks[i] != NULL) {
            status = rep_->blocks[i]->Scan_between(literal1, literal2,
                                                   bitvector->GetBitVectorBlock(i), opt);
            if (!status.IsOk())
                return status;
        } else {
            // The block has not been allocated. All Null values.
            if (opt == kAnd || opt == kSet) {
                bitvector->GetBitVectorBlock(i)->SetZeros();
            }
        }
    }
    return Status::OK();
}

Status Column::Scan(const Column &column, Comparator comparator, BitVectorOpt opt, BitVector *bitvector) const {
    Status status;
    for (uint32_t i = 0; i < rep_->blocks.size(); i++) {
        if (i < rep_->blocks.size() && rep_->blocks[i] != NULL
            && i < column.rep_->blocks.size() && column.rep_->blocks[i] != NULL) {
            status = rep_->blocks[i]->Scan(comparator, *(column.rep_->blocks[i]),
                                           bitvector->GetBitVectorBlock(i), opt);
            if (!status.IsOk())
                return status;
        } else {
            // The block has not been allocated. All Null values.
            if (opt == kAnd || opt == kSet) {
                bitvector->GetBitVectorBlock(i)->SetZeros();
            }
        }
    }
    return Status::OK();
}

Status Column::GetCode(TupleId pos, Code *code) const {
    size_t block_id = pos / kNumCodesPerBlock;
    size_t block_code_id = pos % kNumCodesPerBlock;
    assert(block_id < rep_->blocks.size());
    ColumnBlock *block = rep_->blocks[block_id];
    if (block == NULL) {
        *code = kNullCode;
        return Status::OK();
    }
    return block->GetCode(block_code_id, code);
}

Status Column::SetCode(TupleId pos, Code code) {
    size_t block_id = pos / kNumCodesPerBlock;
    size_t block_code_id = pos % kNumCodesPerBlock;
    assert(block_id < rep_->blocks.size());
    ColumnBlock *block = rep_->blocks[block_id];
    if (block == NULL) {
        // Lazy allocate block
        rep_->blocks[block_id] = block = CreateColumnBlock();
    }
    return block->SetCode(block_code_id, code);
}

Status Column::Resize(size_t num) {
    size_t num_blocks = (num + kNumCodesPerBlock - 1) / kNumCodesPerBlock;
    size_t num_exist_blocks = rep_->blocks.size();
    size_t num_codes_last_block = num % kNumCodesPerBlock;
    if (num_blocks > num_exist_blocks) {
        // Add more blocks
        if (num_exist_blocks > 0) {
            rep_->blocks[num_exist_blocks - 1]->Resize(kNumCodesPerBlock);
        }
        for (size_t block_id = num_exist_blocks; block_id < num_blocks; block_id++) {
            ColumnBlock *column_block = CreateColumnBlock();
            column_block->Resize(kNumCodesPerBlock);
            rep_->blocks.push_back(column_block);
        }
    } else if (num_blocks < num_exist_blocks) {
        // Remove exist blocks
        for (size_t block_id = num_blocks; block_id < num_exist_blocks; block_id++) {
            ColumnBlock *column_block = rep_->blocks[block_id];
            column_block->Resize(0);
            delete column_block;
        }
        rep_->blocks.resize(num_blocks);
    }
    // Resize the last column block
    if (num_blocks > 0) {
        rep_->blocks[num_blocks - 1]->Resize(num_codes_last_block);
    }
    assert(num_blocks == rep_->blocks.size());
    return Status::OK();
}

ColumnBlock *Column::GetColumnBlock(size_t column_block_id) const {
    if (column_block_id < rep_->blocks.size())
        return rep_->blocks[column_block_id];
    else
        return NULL;
}

Status Column::Save(const std::string &path) const {
    Status status;
    SequentialWriteFile file;
    status = file.Open(path);
    if (!status.IsOk()) {
        return status;
    }

    status = file.Append(reinterpret_cast<const char *>(&rep_->type),
                         sizeof(ColumnType));
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<const char *>(&rep_->code_size),
                         sizeof(size_t));
    if (!status.IsOk())
        return status;

    size_t num_blocks = rep_->blocks.size();
    status = file.Append(reinterpret_cast<const char *>(&num_blocks),
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

Status Column::Load(const std::string &path) const {
    Status status;
    SequentialReadFile file;
    status = file.Open(path);
    if (!status.IsOk()) {
        return status;
    }

    status = file.Read(reinterpret_cast<char *>(&rep_->type),
                       sizeof(ColumnType));
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(&rep_->code_size),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    size_t num_blocks;
    status = file.Read(reinterpret_cast<char *>(&num_blocks),
                       sizeof(size_t));
    if (!status.IsOk())
        return status;

    for (size_t i = 0; i < num_blocks; i++) {
        ColumnBlock *column_block = CreateColumnBlock();
        column_block->Load(file);
        rep_->blocks.push_back(column_block);
    }

    status = file.Close();
    if (!status.IsOk()) {
        return status;
    }

    return Status::OK();
}

ColumnBlock *Column::CreateColumnBlock() const {
    assert(rep_->code_size > 0);
    switch (rep_->type) {
        case kNaive:
            return new NaiveColumnBlock();
        case kBitWeavingH:
            switch (rep_->code_size) {
                case 1:
                    return new BwHColumnBlock<1>();
                case 2:
                    return new BwHColumnBlock<2>();
                case 3:
                    return new BwHColumnBlock<3>();
                case 4:
                    return new BwHColumnBlock<4>();
                case 5:
                    return new BwHColumnBlock<5>();
                case 6:
                    return new BwHColumnBlock<6>();
                case 7:
                    return new BwHColumnBlock<7>();
                case 8:
                    return new BwHColumnBlock<8>();
                case 9:
                    return new BwHColumnBlock<9>();
                case 10:
                    return new BwHColumnBlock<10>();
                case 11:
                    return new BwHColumnBlock<11>();
                case 12:
                    return new BwHColumnBlock<12>();
                case 13:
                    return new BwHColumnBlock<13>();
                case 14:
                    return new BwHColumnBlock<14>();
                case 15:
                    return new BwHColumnBlock<15>();
                case 16:
                    return new BwHColumnBlock<16>();
                case 17:
                    return new BwHColumnBlock<17>();
                case 18:
                    return new BwHColumnBlock<18>();
                case 19:
                    return new BwHColumnBlock<19>();
                case 20:
                    return new BwHColumnBlock<20>();
                case 21:
                    return new BwHColumnBlock<21>();
                case 22:
                    return new BwHColumnBlock<22>();
                case 23:
                    return new BwHColumnBlock<23>();
                case 24:
                    return new BwHColumnBlock<24>();
                case 25:
                    return new BwHColumnBlock<25>();
                case 26:
                    return new BwHColumnBlock<26>();
                case 27:
                    return new BwHColumnBlock<27>();
                case 28:
                    return new BwHColumnBlock<28>();
                case 29:
                    return new BwHColumnBlock<29>();
                case 30:
                    return new BwHColumnBlock<30>();
                case 31:
                    return new BwHColumnBlock<31>();
                case 32:
                    return new BwHColumnBlock<32>();
                default:
                    std::cerr << "Code size overflow in creating BitWeaving/H block"
                              << std::endl;
            }
            break;
        case kBitWeavingV:
            switch (rep_->code_size) {
                case 1:
                    return new BwVColumnBlock<1>();
                case 2:
                    return new BwVColumnBlock<2>();
                case 3:
                    return new BwVColumnBlock<3>();
                case 4:
                    return new BwVColumnBlock<4>();
                case 5:
                    return new BwVColumnBlock<5>();
                case 6:
                    return new BwVColumnBlock<6>();
                case 7:
                    return new BwVColumnBlock<7>();
                case 8:
                    return new BwVColumnBlock<8>();
                case 9:
                    return new BwVColumnBlock<9>();
                case 10:
                    return new BwVColumnBlock<10>();
                case 11:
                    return new BwVColumnBlock<11>();
                case 12:
                    return new BwVColumnBlock<12>();
                case 13:
                    return new BwVColumnBlock<13>();
                case 14:
                    return new BwVColumnBlock<14>();
                case 15:
                    return new BwVColumnBlock<15>();
                case 16:
                    return new BwVColumnBlock<16>();
                case 17:
                    return new BwVColumnBlock<17>();
                case 18:
                    return new BwVColumnBlock<18>();
                case 19:
                    return new BwVColumnBlock<19>();
                case 20:
                    return new BwVColumnBlock<20>();
                case 21:
                    return new BwVColumnBlock<21>();
                case 22:
                    return new BwVColumnBlock<22>();
                case 23:
                    return new BwVColumnBlock<23>();
                case 24:
                    return new BwVColumnBlock<24>();
                case 25:
                    return new BwVColumnBlock<25>();
                case 26:
                    return new BwVColumnBlock<26>();
                case 27:
                    return new BwVColumnBlock<27>();
                case 28:
                    return new BwVColumnBlock<28>();
                case 29:
                    return new BwVColumnBlock<29>();
                case 30:
                    return new BwVColumnBlock<30>();
                case 31:
                    return new BwVColumnBlock<31>();
                case 32:
                    return new BwVColumnBlock<32>();
                default:
                    std::cerr << "Code size overflow in creating BitWeaving/V block" << std::endl;
            }
            break;
    }
    return NULL;
}

ColumnIterator *Column::CreateColumnIterator() {
    switch (rep_->type) {
        case kBitWeavingH:
            switch (rep_->code_size) {
                case 1:
                    return new BwHColumnIterator<1>(this);
                case 2:
                    return new BwHColumnIterator<2>(this);
                case 3:
                    return new BwHColumnIterator<3>(this);
                case 4:
                    return new BwHColumnIterator<4>(this);
                case 5:
                    return new BwHColumnIterator<5>(this);
                case 6:
                    return new BwHColumnIterator<6>(this);
                case 7:
                    return new BwHColumnIterator<7>(this);
                case 8:
                    return new BwHColumnIterator<8>(this);
                case 9:
                    return new BwHColumnIterator<9>(this);
                case 10:
                    return new BwHColumnIterator<10>(this);
                case 11:
                    return new BwHColumnIterator<11>(this);
                case 12:
                    return new BwHColumnIterator<12>(this);
                case 13:
                    return new BwHColumnIterator<13>(this);
                case 14:
                    return new BwHColumnIterator<14>(this);
                case 15:
                    return new BwHColumnIterator<15>(this);
                case 16:
                    return new BwHColumnIterator<16>(this);
                case 17:
                    return new BwHColumnIterator<17>(this);
                case 18:
                    return new BwHColumnIterator<18>(this);
                case 19:
                    return new BwHColumnIterator<19>(this);
                case 20:
                    return new BwHColumnIterator<20>(this);
                case 21:
                    return new BwHColumnIterator<21>(this);
                case 22:
                    return new BwHColumnIterator<22>(this);
                case 23:
                    return new BwHColumnIterator<23>(this);
                case 24:
                    return new BwHColumnIterator<24>(this);
                case 25:
                    return new BwHColumnIterator<25>(this);
                case 26:
                    return new BwHColumnIterator<26>(this);
                case 27:
                    return new BwHColumnIterator<27>(this);
                case 28:
                    return new BwHColumnIterator<28>(this);
                case 29:
                    return new BwHColumnIterator<29>(this);
                case 30:
                    return new BwHColumnIterator<30>(this);
                case 31:
                    return new BwHColumnIterator<31>(this);
                case 32:
                    return new BwHColumnIterator<32>(this);
            }
            break;
        case kBitWeavingV:
            switch (rep_->code_size) {
                case 1:
                    return new BwVColumnIterator<1>(this);
                case 2:
                    return new BwVColumnIterator<2>(this);
                case 3:
                    return new BwVColumnIterator<3>(this);
                case 4:
                    return new BwVColumnIterator<4>(this);
                case 5:
                    return new BwVColumnIterator<5>(this);
                case 6:
                    return new BwVColumnIterator<6>(this);
                case 7:
                    return new BwVColumnIterator<7>(this);
                case 8:
                    return new BwVColumnIterator<8>(this);
                case 9:
                    return new BwVColumnIterator<9>(this);
                case 10:
                    return new BwVColumnIterator<10>(this);
                case 11:
                    return new BwVColumnIterator<11>(this);
                case 12:
                    return new BwVColumnIterator<12>(this);
                case 13:
                    return new BwVColumnIterator<13>(this);
                case 14:
                    return new BwVColumnIterator<14>(this);
                case 15:
                    return new BwVColumnIterator<15>(this);
                case 16:
                    return new BwVColumnIterator<16>(this);
                case 17:
                    return new BwVColumnIterator<17>(this);
                case 18:
                    return new BwVColumnIterator<18>(this);
                case 19:
                    return new BwVColumnIterator<19>(this);
                case 20:
                    return new BwVColumnIterator<20>(this);
                case 21:
                    return new BwVColumnIterator<21>(this);
                case 22:
                    return new BwVColumnIterator<22>(this);
                case 23:
                    return new BwVColumnIterator<23>(this);
                case 24:
                    return new BwVColumnIterator<24>(this);
                case 25:
                    return new BwVColumnIterator<25>(this);
                case 26:
                    return new BwVColumnIterator<26>(this);
                case 27:
                    return new BwVColumnIterator<27>(this);
                case 28:
                    return new BwVColumnIterator<28>(this);
                case 29:
                    return new BwVColumnIterator<29>(this);
                case 30:
                    return new BwVColumnIterator<30>(this);
                case 31:
                    return new BwVColumnIterator<31>(this);
                case 32:
                    return new BwVColumnIterator<32>(this);
            }
            break;
        case kNaive:
            return new ColumnIterator(this);
    }
    return new ColumnIterator(this);
}

}  // namespace bitweaving

