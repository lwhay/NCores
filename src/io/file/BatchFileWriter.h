//
// Created by Vince Tu on 2019/9/1.
// ckReader(istream 
//

#ifndef CORES_BATCHFILEWRITER_H
#define CORES_BATCHFILEWRITER_H

#include <memory.h>
#include "Generic.h"
#include "vector"
#include "string"
#include "iostream"
#include "ColumnarData.h"
#include <assert.h>

using namespace std;

void SplitString(const std::string &s, std::vector<std::string> &v, const std::string &c) {
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while (std::string::npos != pos2) {
        v.push_back(s.substr(pos1, pos2 - pos1));
        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != s.length())
        v.push_back(s.substr(pos1));
}

class BlockReader {
    int rowCount;
    int lengthOffset;
    int size;

public:
    BlockReader() : rowCount(0), lengthOffset(0), size(0) {
    }

    BlockReader(istream is) {
        is.read((char *) &rowCount, sizeof(rowCount));
        is.read((char *) &size, sizeof(size));
    }

    void writeTo(ostream os) {
        os.write((char *) &rowCount, sizeof(rowCount));
        os.write((char *) size, sizeof(size));
    }

    int getOffset() {
        return size;
    }

    int getSize() {
        return size;
    }

    int getRowcount() {
        return rowCount;
    }

    void read(istream &is) {
        is.read((char *) &this->rowCount, sizeof(this->rowCount));
        is.read((char *) &this->size, sizeof(this->size));
    }
};

class ColumnReader {
    long offset;
    string metaData;
    int blockCount;
    vector<BlockReader> blocks;

public:
    ColumnReader() {}

    ColumnReader(const ColumnReader &columnreader) {
        offset = columnreader.offset;
        metaData = columnreader.metaData;
        blockCount = columnreader.blockCount;
        blocks = columnreader.blocks;
    }

    ColumnReader(int count, long _offset) : blockCount(count), offset(_offset) {
        //metaData=meta;
    }

    long getOffset() {
        return offset;
    }

    long getRowcount() {
        return blocks.back().getOffset();
    }

    void setBlocks(vector<BlockReader> _blocks) {
        this->blocks.assign(_blocks.begin(), _blocks.begin() + _blocks.size());
    }

    BlockReader &getBlock(int b) {
        return blocks[b];
    }

    const BlockReader &getBlock(int b) const {
        return blocks[b];
    }

    int getblockCount() {
        return blockCount;
    }

    void pushBlock(BlockReader block) {
        blocks.push_back(block);
    }

    int findOff(int off, int &roff) {
        int low = 1, high = blockCount - 1, middle = (low + high) / 2;

        if (off < blocks[0].getOffset()) {
            roff = off;
            return 0;
        }
        while (low < high) {
            middle = (low + high) / 2;
            if (off < blocks[middle].getOffset() && off >= blocks[middle - 1].getOffset()) {
                roff = off - blocks[middle - 1].getOffset();
                return middle;
            } else if (off < blocks[middle - 1].getOffset()) {
                high = middle;
            } else if (off >= blocks[middle].getOffset()) {
                low = middle + 1;
            }
        }
        if (off < blocks[middle].getOffset() && off >= blocks[middle - 1].getOffset()) {
            roff = off - blocks[middle - 1].getOffset();
            return middle;
        }
        return -1;
    }

    int findOff(int bgn, int off, int &roff) {
        int low = bgn, high = blockCount - 1, middle = (low + high) / 2;

        if (off < blocks[0].getOffset()) {
            roff = off;
            return 0;
        }
        while (low < high) {
            middle = (low + high) / 2;
            if (off < blocks[middle].getOffset() && off >= blocks[middle - 1].getOffset()) {
                roff = off - blocks[middle - 1].getOffset();
                return middle;
            } else if (off < blocks[middle - 1].getOffset()) {
                high = middle;
            } else if (off >= blocks[middle].getOffset()) {
                low = middle + 1;
            }
        }
        if (off < blocks[middle].getOffset() && off >= blocks[middle - 1].getOffset()) {
            roff = off - blocks[middle - 1].getOffset();
            return middle;
        }
        return -1;
    }

};

class HeadReader {
    int rowCount;
    int blockSize;
    int columnCount;
    string metaData;
    vector<ColumnReader> columns;

public:
    HeadReader() {}

    int getRowCount() {
        return rowCount;
    }

    int getBlockSize() {
        return blockSize;
    }

    int getColumnCount() {
        return columnCount;
    }

    vector<ColumnReader> &getColumns() {
        return columns;
    }

    ColumnReader &getColumn(int c) {
        return columns[c];
    }

    string getMetaData() {
        return metaData;
    }

    void setColumns(vector<ColumnReader> _columns) {
        this->columns.assign(_columns.begin(), _columns.begin() + _columns.size());
    }

    void readHeader(istream &is) {
        is.read((char *) &this->blockSize, sizeof(this->blockSize));
        is.read((char *) &this->rowCount, sizeof(this->rowCount));
        is.read((char *) &this->columnCount, sizeof(this->columnCount));
        //getline(is,metaData);

        unique_ptr<vector<ColumnReader>> columns(new vector<ColumnReader>());
        for (int i = 0; i < columnCount; ++i) {
            int blockCount;
            is.read((char *) &blockCount, sizeof(blockCount));
            long offset;
            is.read((char *) &offset, sizeof(offset));
            unique_ptr<vector<BlockReader>> blocks(new vector<BlockReader>());
            for (int j = 0; j < blockCount; ++j) {
                unique_ptr<BlockReader> block(new BlockReader);
                block->read(is);
                blocks->push_back(*block);
            }
            int tmp = blocks->size();
            unique_ptr<ColumnReader> column(new ColumnReader(blockCount, offset));
            column->setBlocks(*blocks);
            columns->push_back(*column);
        }
        this->setColumns(*columns);
    }
};

class ColumnWriter {
public:
    ColumnWriter() {

    }

    ColumnWriter(int _blocksize, Type t, string path, bool _required) : c_type(t), blocksize(_blocksize),
                                                                        required(_required) {

        if (blocksize < 1 << 8) offsize = 1;
        else if (blocksize < 1 << 16) offsize = 2;
        else if (blocksize < 1 << 24) offsize = 3;
        else if (blocksize < 1 << 32) offsize = 4;
        else cout << "blocksize is too large" << endl;

        d_index = 0;
        h_index = 0;
        file_offset = 0;
        block_num = 0;
        strl = 0;

        h_file = fopen((path + ".head").data(), "wb+");
        d_file = fopen((path + ".data").data(), "wb+");
        d_buffers = new char[blocksize];
        h_info = new int[blocksize];
        if (!h_file) {
            cout << ("File opening failed");
        }
    }

private:
    bool required;
    Type c_type;
    int blocksize;
    FILE *d_file;
    FILE *h_file;
    char *d_buffers;
    int *h_info;
    int d_index;
    int h_index;
    vector<int> off_index;
    vector<unsigned char> valid;
    int file_offset;
    int block_num;
    int strl;
    int offsize;

public:
    void write(GenericDatum g) {
        if (required)
            fixWrite(g);
        else {
            if (g.type() == CORES_NULL)
                nullWrite();
            else
                flxWrite(g);
        }
    }

    void fixWrite(GenericDatum data) {
        switch (c_type) {
            case CORES_INT: {
                int tmp = data.value<int>();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(int);
                    file_offset += blocksize / sizeof(int);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                *(int *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(int);
                break;
            }
            case CORES_LONG: {
                int64_t tmp = data.value<int64_t>();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(int64_t);
                    file_offset += blocksize / sizeof(int64_t);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                *(int64_t *) (&(d_buffers[d_index])) = tmp;
                d_index += sizeof(int64_t);
                break;
            }
            case CORES_DOUBLE: {
                double tmp = data.value<double>();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(double);
                    file_offset += blocksize / sizeof(double);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                *(double *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(double);
                break;
            }
            case CORES_FLOAT: {
                float tmp = data.value<float>();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(float);
                    file_offset += blocksize / sizeof(float);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                *(float *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(float);
                break;
            }
            case CORES_BYTES: {
                char tmp = data.value<char>();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(char);
                    file_offset += blocksize / sizeof(char);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(char), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(char));
                        h_index = 0;
                    }
                }
                *(char *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(char);
                break;
            }
            case CORES_STRING: {
                string tmp = data.value<string>();
                if ((d_index + tmp.length() + 1 + offsize * strl) > blocksize) {
                    int tmpoff = offsize * strl;
                    for (int j :off_index) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                    }
                    fwrite(d_buffers, sizeof(char), blocksize - tmpoff, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    d_index = 0;
                    strl = 0;
                    off_index.clear();
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                off_index.push_back(d_index);
                std::strcpy(&d_buffers[d_index], tmp.c_str());
                d_index += tmp.length() + 1;
                strl++;
                break;
            }
            case CORES_ARRAY: {
                GenericArray ga = data.value<GenericArray>();
                int tmp = ga.value().size();
                if (d_index >= blocksize) {
                    d_index = 0;
                    fwrite(d_buffers, sizeof(char), blocksize, d_file);
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = blocksize / sizeof(int);
                    file_offset += blocksize / sizeof(int);
                    h_info[h_index++] = file_offset;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                *(int *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(int);
                break;
            }

        }
    }

    void nullWrite() {
        if (c_type == CORES_STRING) {
            int zero = 0;
            if (strl % 8 == 0) zero = 1;
            if ((d_index + 1 + offsize * strl + valid.size()) > blocksize) {
                fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                int tmpoff = offsize * strl;
                for (int j :off_index) {
                    int tmp = j + tmpoff;
                    fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                }
                fwrite(d_buffers, sizeof(char),
                       blocksize - valid.size() - offsize * off_index.size(), d_file);
                valid.clear();
                block_num++;
                memset(d_buffers, 0, blocksize * sizeof(char));
                h_info[h_index++] = strl;
                file_offset += strl;
                h_info[h_index++] = file_offset;
                d_index = 0;
                strl = 0;
                off_index.clear();
                if (h_index >= blocksize) {
                    fwrite(h_info, sizeof(int), blocksize, h_file);
                    memset(h_info, 0, blocksize * sizeof(int));
                    h_index = 0;
                }
            }
            if (zero) {
                valid.push_back(0x0);
            }
            strl++;
        } else {
            int zero = 0;
            if (strl % 8 == 0) zero = 1;
            if (d_index + zero + valid.size() >= blocksize) {
                fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                valid.clear();
                d_index = 0;
                block_num++;
                memset(d_buffers, 0, blocksize * sizeof(char));
                h_info[h_index++] = strl;
                file_offset += strl;
                h_info[h_index++] = file_offset;
                strl = 0;
                if (h_index >= blocksize) {
                    fwrite(h_info, sizeof(int), blocksize, h_file);
                    memset(h_info, 0, blocksize * sizeof(int));
                    h_index = 0;
                }
            }
            if (zero) {
                valid.push_back(0x0);
            }
            strl++;
        }
    }

    void flxWrite(GenericDatum data) {
        switch (c_type) {
            case CORES_INT: {
                int tmp = data.value<int>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);
                *(int *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(int);
                strl++;
                break;
            }
            case CORES_LONG: {
                int64_t tmp = data.value<int64_t>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;

                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                *(int64_t *) (&(d_buffers[d_index])) = tmp;
                d_index += sizeof(int64_t);
                strl++;
                break;
            }
            case CORES_DOUBLE: {
                double tmp = data.value<double>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                *(double *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(double);
                strl++;
                break;
            }
            case CORES_FLOAT: {
                float tmp = data.value<float>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                *(float *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(float);
                strl++;
                break;
            }
            case CORES_BYTES: {
                char tmp = data.value<char>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                *(char *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(char);
                strl++;
                break;
            }
            case CORES_STRING: {
                string tmp = data.value<string>();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if ((d_index + tmp.length() + 1 + offsize * strl + valid.size()) > blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    int tmpoff = offsize * strl;
                    for (int j :off_index) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                    }
                    fwrite(d_buffers, sizeof(char),
                           blocksize - valid.size() - offsize * off_index.size(), d_file);
                    valid.clear();
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    d_index = 0;
                    strl = 0;
                    off_index.clear();
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                const char *tmpc = tmp.c_str();
                int tmpi = tmp.length();
                off_index.push_back(d_index);
                std::strcpy(&d_buffers[d_index], tmp.c_str());
                d_index += tmp.length() + 1;
                strl++;
                break;
            }
            case CORES_ARRAY: {
                int tmp = data.value<GenericArray>().value().size();
                int zero = 0;
                if (strl % 8 == 0) zero = 1;
                if (d_index + zero + valid.size() >= blocksize) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                    valid.clear();
                    d_index = 0;
                    block_num++;
                    memset(d_buffers, 0, blocksize * sizeof(char));
                    h_info[h_index++] = strl;
                    file_offset += strl;
                    h_info[h_index++] = file_offset;
                    strl = 0;
                    if (h_index >= blocksize) {
                        fwrite(h_info, sizeof(int), blocksize, h_file);
                        memset(h_info, 0, blocksize * sizeof(int));
                        h_index = 0;
                    }
                }
                if (strl % 8 == 0) zero = 1;
                if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (strl % 8);

                *(int *) (&d_buffers[d_index]) = tmp;
                d_index += sizeof(int);
                strl++;
                break;
            }
        }
    }

    void writeRest() {
        if (!required) {
            if (d_index != 0) {
                if (c_type == CORES_STRING) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    int tmpoff = offsize * strl;
                    for (int j :off_index) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                    }
                    fwrite(d_buffers, sizeof(char),
                           blocksize - valid.size() - offsize * off_index.size(), d_file);
                } else {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
                }
                block_num++;
                h_info[h_index++] = strl;
                file_offset += strl;
                h_info[h_index++] = file_offset;
            }
        } else {
            if (c_type == CORES_STRING) {
                int tmpoff = offsize * strl;
                for (int j :off_index) {
                    int tmp = j + tmpoff;
                    fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                }
                fwrite(d_buffers, sizeof(char),
                       blocksize - offsize * off_index.size(), d_file);
            } else {
                fwrite(d_buffers, sizeof(char), blocksize - valid.size(), d_file);
            }
            block_num++;
            h_info[h_index++] = strl;
            file_offset += strl;
            h_info[h_index++] = file_offset;
        }
        if (h_index != 0) {
            fwrite(h_info, sizeof(int), blocksize, h_file);
        }


        fflush(d_file);
        fflush(h_file);
        fclose(d_file);
        fclose(h_file);
    }


    int getBlocknum() {
        return block_num;
    }
};

class BatchFileWriter {
    BatchFileWriter() {

    }

public:

    BatchFileWriter(GenericDatum record, string path, int blocksize, bool flexible) {
        this->record = new GenericRecord(record.value<GenericRecord>());
        this->path = path;
        this->blocksize = blocksize;
        column_num = this->record->fieldCount();

        head_files = (FILE **) (malloc(column_num * sizeof(FILE *)));
        data_files = (FILE **) (malloc(column_num * sizeof(FILE *)));

        for (int j = 0; j < column_num; ++j) {
            if (this->record->schema()->isRequired(j)) {
                if (this->record->schema()->leafAt(j)->type() == CORES_STRING)
                    cs.push_back(new VRColWriter(blocksize, this->record->schema()->leafAt(j)->type(),
                                                 path + "/file" + to_string(j)));
                else
                    cs.push_back(new FRColWriter(blocksize, this->record->schema()->leafAt(j)->type(),
                                                 path + "/file" + to_string(j)));
            } else {
                if (this->record->schema()->leafAt(j)->type() == CORES_STRING)
                    cs.push_back(new VOColWriter(blocksize, this->record->schema()->leafAt(j)->type(),
                                                 path + "/file" + to_string(j)));
                else
                    cs.push_back(new FOColWriter(blocksize, this->record->schema()->leafAt(j)->type(),
                                                 path + "/file" + to_string(j)));
            }
        }
    }

    BatchFileWriter(GenericDatum record, string path, int blocksize = 1024) {
        this->record = new GenericRecord(record.value<GenericRecord>());
        this->path = path;
        this->blocksize = blocksize;
        column_num = this->record->fieldCount();

        if (blocksize < 1 << 8) offsize = 1;
        else if (blocksize < 1 << 16) offsize = 2;
        else if (blocksize < 1 << 24) offsize = 3;
        else if (blocksize < 1 << 32) offsize = 4;
        else cout << "blocksize is too large" << endl;
        head_files = (FILE **) (malloc(column_num * sizeof(FILE *)));
        data_files = (FILE **) (malloc(column_num * sizeof(FILE *)));
        data_buffers = (char **) (malloc(column_num * sizeof(char *)));
        heads_info = (int **) (malloc(column_num * sizeof(int *)));
        head_index = new int[column_num]();
        data_index = new int[column_num]();
        off_index = new vector<int>[column_num]();
        valid = new vector<unsigned char>[column_num]();
        file_offset = new int[column_num]();
        block_num = new int[column_num]();
        strl = new int[column_num]();

        for (int i = 0; i < column_num; ++i) {
            head_files[i] = fopen((path + "/file" + to_string(i) + ".head").data(), "wb+");
            data_files[i] = fopen((path + "/file" + to_string(i) + ".data").data(), "wb+");
            data_buffers[i] = new char[blocksize];
            heads_info[i] = new int[blocksize];
        }
        if (!head_files[0]) {
            cout << ("File opening failed");
        }
    }

private:
    string path;
    int blocksize;
    int offsize;
    GenericRecord *record;
    int column_num;
    FILE **data_files;
    FILE **head_files;
    char **data_buffers;
    int **heads_info;
    int *data_index;
    int *head_index;
    vector<int> *off_index;
    vector<unsigned char> *valid;
    int *file_offset;
    int *block_num;
    int *strl;

    vector<ColumnWriter> cws;

    vector<ColWriterImpl *> cs;

    FILE *outfile;


public:

    int64_t getInd(int ind) {
        return record->fieldAt(ind).value<int64_t>();
    }

    void setArr(int ind, vector<GenericDatum> arr) {
        record->fieldAt(ind).value<GenericArray>().value() = arr;
    }

    int getOffsize() {
        return offsize;
    }

    GenericRecord getRecord() {
        return *record;
    }

    void setRecord(GenericRecord _record) {
        delete (record);
        this->record = new GenericRecord(_record);
        int tmp = this->record->fieldAt(1).value<int64_t>();
    }

//    GenericDatum getRecord(){
//        return GenericDatum(*record);
//    }

    void readLine(string line) {
        vector<string> v;
        SplitString(line, v, "|");
        for (int i = 0; i < column_num; ++i) {
            switch (record->schema()->leafAt(i)->type()) {
                case CORES_INT: {
                    int tmp = stoi(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = stof(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case CORES_LONG: {
                    int64_t tmp = stol(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case CORES_DOUBLE: {
                    double tmp = stod(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = ((char *) v[i].c_str())[0];
                    record->fieldAt(i) = tmp;
                    break;
                }
                case CORES_STRING: {
                    record->fieldAt(i) = v[i];
                    break;
                }
            }
        }
    }

    //flexible write
    void write(const GenericRecord &r) {
        for (int i = 0; i < column_num; ++i) {
            cs[i]->write(r.fieldAt(i));
        }
    }

    //required write
    void writeRecord() {
        for (int i = 0; i < column_num; ++i) {
            switch (record->schema()->leafAt(i)->type()) {
                case CORES_INT: {
                    int tmp = record->fieldAt(i).value<int>();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(int);
                        file_offset[i] += blocksize / sizeof(int);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
                    break;
                }
                case CORES_LONG: {
                    int64_t tmp = record->fieldAt(i).value<int64_t>();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(int64_t);
                        file_offset[i] += blocksize / sizeof(int64_t);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    *(int64_t *) (&(data_buffers[i][data_index[i]])) = tmp;
                    data_index[i] += sizeof(int64_t);
                    break;
                }
                case CORES_DOUBLE: {
                    double tmp = record->fieldAt(i).value<double>();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(double);
                        file_offset[i] += blocksize / sizeof(double);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    *(double *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(double);
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = record->fieldAt(i).value<float>();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(float);
                        file_offset[i] += blocksize / sizeof(float);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    *(float *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(float);
                    break;
                }
                case CORES_BYTES: {
                    char tmp = record->fieldAt(i).value<char>();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(char);
                        file_offset[i] += blocksize / sizeof(char);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(char), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(char));
                            head_index[i] = 0;
                        }
                    }
                    *(char *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(char);
                    break;
                }
                case CORES_STRING: {
                    string tmp = record->fieldAt(i).value<string>();
                    if ((data_index[i] + tmp.length() + 1 + offsize * strl[i]) > blocksize) {
                        int tmpoff = offsize * strl[i];
                        for (int j :off_index[i]) {
                            int tmp = j + tmpoff;
                            fwrite((char *) &tmp, offsize, sizeof(char), data_files[i]);
                        }
                        fwrite(data_buffers[i], sizeof(char), blocksize - tmpoff, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        data_index[i] = 0;
                        strl[i] = 0;
                        off_index[i].clear();
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    off_index[i].push_back(data_index[i]);
                    std::strcpy(&data_buffers[i][data_index[i]], tmp.c_str());
                    data_index[i] += tmp.length() + 1;
                    strl[i]++;
                    break;
                }
                case CORES_ARRAY: {
                    int tmp = record->fieldAt(i).value<GenericArray>().value().size();
                    if (data_index[i] >= blocksize) {
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(int);
                        file_offset[i] += blocksize / sizeof(int);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
                    break;
                }
            }
        }

    }

    //optional write
    void writeRecord(const GenericRecord &r) {
        for (int i = 0; i < column_num; ++i) {
            switch (r.schema()->leafAt(i)->type()) {
                case CORES_INT: {
                    int tmp = r.fieldAt(i).value<int>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
                    strl[i]++;
                    break;
                }
                case CORES_LONG: {
                    int64_t tmp = r.fieldAt(i).value<int64_t>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;

                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(int64_t *) (&(data_buffers[i][data_index[i]])) = tmp;
                    data_index[i] += sizeof(int64_t);
                    strl[i]++;
                    break;
                }
                case CORES_DOUBLE: {
                    double tmp = r.fieldAt(i).value<double>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(double *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(double);
                    strl[i]++;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = r.fieldAt(i).value<float>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(float *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(float);
                    strl[i]++;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = r.fieldAt(i).value<char>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(char *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(char);
                    strl[i]++;
                    break;
                }
                case CORES_STRING: {
                    string tmp = r.fieldAt(i).value<string>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if ((data_index[i] + tmp.length() + 1 + offsize * strl[i] + valid[i].size()) > blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        int tmpoff = offsize * strl[i];
                        for (int j :off_index[i]) {
                            int tmp = j + tmpoff;
                            fwrite((char *) &tmp, offsize, sizeof(char), data_files[i]);
                        }
                        fwrite(data_buffers[i], sizeof(char),
                               blocksize - valid[i].size() - offsize * off_index[i].size(), data_files[i]);
                        valid[i].clear();
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        data_index[i] = 0;
                        strl[i] = 0;
                        off_index[i].clear();
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    const char *tmpc = tmp.c_str();
                    int tmpi = tmp.length();
                    off_index[i].push_back(data_index[i]);
                    std::strcpy(&data_buffers[i][data_index[i]], tmp.c_str());
                    data_index[i] += tmp.length() + 1;
                    strl[i]++;
                    break;
                }
                case CORES_ARRAY: {
                    int tmp = r.fieldAt(i).value<GenericArray>().value().size();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    if (strl[i] % 8 == 0) zero = 1;
                    if (zero) {
                        valid[i].push_back(0x0);
                    }
                    if (r.schema()->getValid(i)) {
                        valid[i].back() |= 1L << (strl[i] % 8);
                    }
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
                    strl[i]++;
                    break;
                }
            }
        }
    }

    void writeRest(bool flexible) {
        for (int i = 0; i < column_num; ++i) {
            cs[i]->writeRest();
        }
    }

    void writeRest() {
        for (int k = 0; k < column_num; ++k) {
            if (data_index[k] != 0) {
                if (record->fieldAt(k).type() == CORES_STRING) {
                    fwrite((char *) &valid[k][0], valid[k].size(), sizeof(char), data_files[k]);
                    int tmpoff = offsize * strl[k];
                    for (int j :off_index[k]) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), data_files[k]);
                    }
                    fwrite(data_buffers[k], sizeof(char), blocksize - valid[k].size() - offsize * off_index[k].size(),
                           data_files[k]);
                } else {
                    fwrite((char *) &valid[k][0], valid[k].size(), sizeof(char), data_files[k]);
                    fwrite(data_buffers[k], sizeof(char), blocksize - valid[k].size(), data_files[k]);
                }
                block_num[k]++;
                heads_info[k][head_index[k]++] = strl[k];
                file_offset[k] += strl[k];
                heads_info[k][head_index[k]++] = file_offset[k];
            }
            if (head_index[k] != 0) {
                fwrite(heads_info[k], sizeof(int), blocksize, head_files[k]);
            }
        }

        for (int l = 0; l < column_num; ++l) {
            fflush(data_files[l]);
            fflush(head_files[l]);
            fclose(data_files[l]);
            fclose(head_files[l]);
        }
    }

    void mergeFiles(bool fle) {
        for (int i = 0; i < column_num; ++i) {
            head_files[i] = fopen((path + "/file" + to_string(i) + ".head").data(), "rb");
            data_files[i] = fopen((path + "/file" + to_string(i) + ".data").data(), "rb");
        }
        fstream fo;
        fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
        long *foffsets = new long[column_num];
        int rowcount = 0;
        fo.write((char *) &blocksize, sizeof(blocksize));
        fo.write((char *) &rowcount, sizeof(rowcount));
        fo.write((char *) &column_num, sizeof(column_num));
        //fo << schema ;
        //metadata
        long *result = new long[column_num];
        std::vector<int> bc(2);
        vector<int> bnums;
        for (int k = 0; k < column_num; ++k) {
            bnums.push_back(cs[k]->getBlocknum());
        }
        for (int j = 0; j < column_num; ++j) {
            fo.write((char *) &bnums[j], sizeof(int));
            foffsets[j] = fo.tellg();
            fo.write((char *) &foffsets[j], sizeof(long));
            for (int i = 0; i < cs[j]->getBlocknum(); ++i) {
                fread(&bc[0], sizeof bc[0], bc.size(), head_files[j]);
                fo.write((char *) &bc[0], sizeof bc[0]);
                fo.write((char *) &bc[1], sizeof bc[1]);
            }
        }
        result[0] = fo.tellg();
        fo.close();
        FILE *fp = fopen((path + "/fileout.dat").data(), "ab+");
        char *buffer = new char[blocksize];
        long *tmpl = (long *) buffer;
        for (int j = 0; j < column_num; ++j) {
            if (j != 0)result[j] = ftell(fp);
            for (int i = 0; i < cs[j]->getBlocknum(); ++i) {
                fread(buffer, sizeof(char), blocksize, data_files[j]);
                fwrite(buffer, sizeof(char), blocksize, fp);
            }
        }
        fflush(fp);
        fclose(fp);
        fp = fopen((path + "/fileout.dat").data(), "rb+");
        for (int m = 0; m < column_num; ++m) {
            fseek(fp, foffsets[m], SEEK_SET);
            fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
        }
        fflush(fp);
        fclose(fp);
    }

    void mergeFiles() {
        for (int i = 0; i < column_num; ++i) {
            head_files[i] = fopen((path + "/file" + to_string(i) + ".head").data(), "rb");
            data_files[i] = fopen((path + "/file" + to_string(i) + ".data").data(), "rb");
        }
        fstream fo;
        fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
        long *foffsets = new long[column_num];
        int rowcount = 0;
        fo.write((char *) &blocksize, sizeof(blocksize));
        fo.write((char *) &rowcount, sizeof(rowcount));
        fo.write((char *) &column_num, sizeof(column_num));
        //fo << schema ;
        //metadata
        long *result = new long[column_num];
        std::vector<int> bc(2);
        for (int j = 0; j < column_num; ++j) {
            fo.write((char *) &block_num[j], sizeof(int));
            foffsets[j] = fo.tellg();
            fo.write((char *) &foffsets[j], sizeof(long));
            for (int i = 0; i < block_num[j]; ++i) {
                fread(&bc[0], sizeof bc[0], bc.size(), head_files[j]);
                fo.write((char *) &bc[0], sizeof bc[0]);
                fo.write((char *) &bc[1], sizeof bc[1]);
            }
        }
        result[0] = fo.tellg();
        fo.close();
        FILE *fp = fopen((path + "/fileout.dat").data(), "ab+");
        char *buffer = new char[blocksize];
        long *tmpl = (long *) buffer;
        for (int j = 0; j < column_num; ++j) {
            if (j != 0)result[j] = ftell(fp);
            for (int i = 0; i < block_num[j]; ++i) {
                fread(buffer, sizeof(char), blocksize, data_files[j]);
                fwrite(buffer, sizeof(char), blocksize, fp);
            }
        }
        fflush(fp);
        fclose(fp);
        fp = fopen((path + "/fileout.dat").data(), "rb+");
        for (int m = 0; m < column_num; ++m) {
            fseek(fp, foffsets[m], SEEK_SET);
            fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
        }
        fflush(fp);
        fclose(fp);
    }
};


class fileReader {
private:
    GenericRecord r;
    vector<FILE *> fpp;
    shared_ptr<HeadReader> headreader;
    vector<int> rind;
    vector<int> bind;
    vector<int> rcounts;
    int blocksize;
    vector<Block *> blockreaders;
    int ind;
    int begin;
    int end;
    int arsize;
    vector<vector<int> *> offarrs;
    int offsize;
    vector<int> g_offset;

    vector<bool> required;
    vector<Type> types;

    inline bool getRequired(int i) {
        return required[i];
    }

public:
    fileReader(GenericDatum c, shared_ptr<HeadReader> _headreader, int _begin, int _end, char *resultfile) : begin(
            _begin), end(_end + 1), r(GenericRecord(c.value<GenericRecord>())) {
        headreader = _headreader;
        ind = 0;
        ifstream file_in;
        int colnum = end - begin;
        fpp = vector<FILE *>(colnum);
        rind = vector<int>(colnum);
        bind = vector<int>(colnum);
        rcounts = vector<int>(colnum);
        blockreaders = vector<Block *>(colnum);
        offarrs = vector<vector<int> *>(colnum);
        blocksize = headreader->getBlockSize();

        if (blocksize < 1 << 8) offsize = 1;
        else if (blocksize < 1 << 16) offsize = 2;
        else if (blocksize < 1 << 24) offsize = 3;
        else if (blocksize < 1 << 32) offsize = 4;
        for (int i = begin; i < end; ++i) {
            fpp[i - begin] = fopen(resultfile, "rb");
            fseek(fpp[i - begin], headreader->getColumns()[i].getOffset(), SEEK_SET);
            rcounts[i - begin] = headreader->getColumns()[i].getBlock(bind[i - begin]).getRowcount();
            blockreaders[i - begin] = new Block(fpp[i - begin], 0L, 0, blocksize);
            if (r.schema()->isRequired(i - begin))
                blockreaders[i - begin]->loadFromFile();
            else
                blockreaders[i - begin]->loadFromFile(rcounts[i - begin]);
            if (r.schema()->leafAt(i - begin)->type() == CORES_STRING) {
                offarrs[i - begin] = blockreaders[i - begin]->initString(offsize);
            }
        }

        for (int i = 0; i < end - begin; ++i) {
            required.push_back(r.schema()->isRequired(i));
            types.push_back(r.schema()->leafAt(i)->type());
        }
    }

    template<typename type>
    type skipColRead(int off, int i) {
        if (off < headreader->getColumn(i + begin).getBlock(bind[i]).getOffset()) {
            rind[i] = (bind[i] == 0) ? off :
                      off - headreader->getColumn(i + begin).getBlock(bind[i] - 1).getOffset();
        } else {
            bind[i] = headreader->getColumn(i + begin).findOff(bind[i] + 1, off, rind[i]);
            blockreaders[i]->skipload(headreader->getColumn(i + begin).getOffset() + blocksize * bind[i]);
            rcounts[i] = headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount();
            if (r.schema()->leafAt(i)->type() == CORES_STRING) {
//                delete offarrs[i];
                blockreaders[i]->initString(offarrs[i], offsize);
//                offarrs[i] = blockreaders[i]->initString(offsize);
            }
        }
        if (r.schema()->leafAt(i)->type() == CORES_STRING) {
            if (rind[i] == 0) {
                offarrs[i] = blockreaders[i]->initString(offsize);
            }
            int tmpi = (*offarrs[i])[blockreaders[i]->getValidOff(rind[i])];
            return blockreaders[i]->get<type>(tmpi);
        }
        return blockreaders[i]->get<type>(blockreaders[i]->getValidOff(rind[i]));

    }

    void skipread(int off) {
        for (int i = 0; i < end - begin; ++i) {
            int vr_ind;
            int tmp = headreader->getColumn(i + begin).getBlock(1).getSize();
            if (off < headreader->getColumn(i + begin).getBlock(bind[i]).getOffset())
                rind[i] = (bind[i] == 0) ? off :
                          off - headreader->getColumn(i + begin).getBlock(bind[i] - 1).getOffset();
            else {
                bind[i] = headreader->getColumn(i + begin).findOff(bind[i] + 1, off, rind[i]);
                blockreaders[i]->skipload(headreader->getColumn(i + begin).getOffset() + blocksize * bind[i],
                                          !r.schema()->isRequired(i) ?
                                          headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount() : 0);
                rcounts[i] = headreader->getColumns()[i + begin].getBlock(bind[i]).getRowcount();
                if (r.schema()->leafAt(i)->type() == CORES_STRING) {
                    offarrs[i] = blockreaders[i]->initString(offsize);
                }
            }

            if (!r.schema()->isRequired(i))
                if (!blockreaders[i]->isvalid(rind[i])) {
                    r.fieldAt(i) = GenericDatum();
                    cout << "||" << " ";
                    continue;
                }
            switch (r.schema()->leafAt(i)->type()) {
                case CORES_LONG: {
                    int64_t tmp = blockreaders[i]->get<int64_t>(blockreaders[i]->getValidOff(rind[i]));
                    r.fieldAt(i) = tmp;
                    break;
                }
                case CORES_INT: {
                    int tmp = blockreaders[i]->get<int>(blockreaders[i]->getValidOff(rind[i]));
                    r.fieldAt(i) = tmp;
                    break;
                }
                case CORES_STRING: {
                    if (rind[i] == 0) {
                        offarrs[i] = blockreaders[i]->initString(offsize);
                    }
                    int tmpi = (*offarrs[i])[blockreaders[i]->getValidOff(rind[i])];
                    char *tmp = blockreaders[i]->getoffstring(tmpi);
                    r.fieldAt(i) = tmp;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = blockreaders[i]->get<float>(blockreaders[i]->getValidOff(rind[i]));
                    r.fieldAt(i) = tmp;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = blockreaders[i]->get<char>(blockreaders[i]->getValidOff(rind[i]));
                    r.fieldAt(i) = tmp;
                    break;
                }
                default:
                    assert(true);
            }
        }
    }

    void fresh() {
        int colnum = end - begin;
        rind = vector<int>(colnum, 0);
        bind = vector<int>(colnum, 0);
        rcounts = vector<int>(colnum, 0);
        for (int i = begin; i < end; ++i) {
            rcounts[i - begin] = headreader->getColumns()[i].getBlock(0).getRowcount();
            blockreaders[i - begin]->bseek(headreader->getColumns()[i].getOffset());
            if (r.schema()->isRequired(i - begin))
                blockreaders[i - begin]->loadFromFile();
            else
                blockreaders[i - begin]->loadFromFile(rcounts[i - begin]);
            if (r.schema()->leafAt(i - begin)->type() == CORES_STRING) {
                offarrs[i - begin] = blockreaders[i - begin]->initString(offsize);
            }
        }
    }

    bool next() {
        for (int i = 0; i < end - begin; ++i) {
            if (rind[i] == rcounts[i]) {
                bind[i]++;
                if (bind[i] == headreader->getColumn(begin + i).getblockCount())
                    return false;
                rcounts[i] = headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount();
                if (getRequired(i))
                    blockreaders[i]->loadFromFile();
                else
                    blockreaders[i]->loadFromFile(rcounts[i]);
                rind[i] = 0;
                if (r.schema()->leafAt(i)->type() == CORES_STRING) {
                    offarrs[i] = blockreaders[i]->initString(offsize);
                }
            }
            if (!getRequired(i) && !blockreaders[i]->isvalid(rind[i])) {
                r.fieldAt(i) = GenericDatum();
                rind[i]++;
                continue;
            }
            switch (types[i]) {
                case CORES_LONG: {
                    int64_t tmp = blockreaders[i]->next<int64_t>();
                    r.fieldAt(i) = tmp;
//                        cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_INT: {
                    int tmp = blockreaders[i]->next<int>();
                    r.fieldAt(i) = tmp;
//                        cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_STRING: {
                    int tmpi = (*offarrs[i])[rind[i]];
                    char *tmp = blockreaders[i]->getoffstring(tmpi);
                    r.fieldAt(i) = tmp;
//                        cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = blockreaders[i]->next<float>();
                    r.fieldAt(i) = tmp;
//                        cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = blockreaders[i]->next<char>();
                    r.fieldAt(i) = tmp;
//                        cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_ARRAY: {
                    arsize = blockreaders[i]->next<int>();
                    rind[i]++;
                    break;
                }
            }
        }
        return true;
    }

    bool next(int n) {
        for (int i = 0; i < end - begin; ++i) {
            bool flag = false;
            int tmpind;
            rind[i] += n;
            while (rind[i] >= headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount()) {
                rind[i] = rind[i] - headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount();
                bind[i]++;
                if (bind[i] == headreader->getColumn(i + begin).getblockCount())
                    return false;
                flag = true;
            }
            if (flag) {
                if (!r.schema()->isRequired(i))
                    blockreaders[i]->skipload(
                            (int64_t) headreader->getColumn(i + begin).getOffset() + blocksize * bind[i],
                            headreader->getColumn(i + begin).getBlock(bind[i]).getRowcount());
                else
                    blockreaders[i]->skipload(
                            (int64_t) headreader->getColumn(i + begin).getOffset() + blocksize * bind[i], 0);
                rcounts[i] = headreader->getColumns()[i + begin].getBlock(bind[i]).getRowcount();
                if (r.schema()->leafAt(i)->type() == CORES_STRING) {
                    offarrs[i] = blockreaders[i]->initString(offsize);
                }
            }
            if (!r.schema()->isRequired(i) && !blockreaders[i]->isvalid(rind[i])) {
                r.fieldAt(i) = GenericDatum();
                rind[i]++;
                continue;
            } else if (!r.schema()->isRequired(i)) {
                tmpind = blockreaders[i]->getValidOff(rind[i]);
            } else {
                tmpind = rind[i];
            }
            switch (r.schema()->leafAt(i)->type()) {
                case CORES_LONG: {
                    int64_t tmp = blockreaders[i]->get<int64_t>(tmpind);
                    r.fieldAt(i) = tmp;
                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_INT: {
                    int tmp = blockreaders[i]->get<int>(tmpind);
                    r.fieldAt(i) = tmp;
                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_STRING: {
                    int tmpi = (*offarrs[i])[tmpind];
                    char *tmp = blockreaders[i]->getoffstring(tmpi);
                    r.fieldAt(i) = tmp;
                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = blockreaders[i]->get<float>(tmpind);
                    r.fieldAt(i) = tmp;
                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = blockreaders[i]->get<char>(tmpind);
                    r.fieldAt(i) = tmp;
                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case CORES_ARRAY: {
                    arsize = blockreaders[i]->next<int>();
                    rind[i]++;
                    break;
                }
            }
        }
        return true;
    }

    void printRecord() {
        for (int i = 0; i < end - begin; ++i) {
            switch (r.schema()->leafAt(i)->type()) {
                case CORES_LONG: {
                    cout << r.fieldAt(i).value<int64_t>() << " ";
                    break;
                }
                case CORES_INT: {
                    cout << r.fieldAt(i).value<int>() << " ";
                    break;
                }
                case CORES_STRING: {
                    cout << r.fieldAt(i).value<const char *>() << " ";
                    break;
                }
                case CORES_FLOAT: {
                    cout << r.fieldAt(i).value<float>() << " ";
                    break;
                }
                case CORES_BYTES: {
                    cout << r.fieldAt(i).value<char>() << " ";
                    break;
                }
                case CORES_ARRAY: {
                    break;
                }
            }
        }
    }

    void initOffset(int aInd) {
        if (r.schema()->leafAt(aInd)->type() != CORES_ARRAY) {
            cout << "arr ind error" << endl;
            return;
        }
        bind[aInd] = 0;
        rcounts[aInd] = headreader->getColumns()[aInd + begin].getBlock(bind[aInd]).getRowcount();
        int tmp = 0;
        while (1) {
            if (rind[aInd] == rcounts[aInd]) {
                bind[aInd]++;
                if (bind[aInd] == headreader->getColumn(begin + aInd).getblockCount())
                    return;
                rcounts[aInd] = headreader->getColumns()[aInd + begin].getBlock(bind[aInd]).getRowcount();
                blockreaders[aInd]->loadFromFile(rcounts[aInd]);
                rind[aInd] = 0;
            }
            tmp += blockreaders[aInd]->next<int>();
            rind[aInd]++;
            g_offset.push_back(tmp);
        }
    }

    int getOff(int ind) {
        return g_offset[ind];
    }

    int getArrsize() {
        return arsize;
    }

    void setArr(const vector<GenericDatum> &rs, int i) {
        if (r.schema()->leafAt(i)->type() == CORES_ARRAY)
            r.fieldAt(i).value<GenericArray>().value() = rs;
        else
            cout << "this is not a arr" << endl;
    }

    vector<GenericDatum> &getArr(int i) {
        if (r.schema()->leafAt(i)->type() == CORES_ARRAY)
            return r.fieldAt(i).value<GenericArray>().value();
        cout << "this is not a arr" << endl;
    }

/*
    bool next(vector<int> r1, vector<int> r2) {
        if (ind < headreader->getRowCount()) {
            ind++;
            for (int i :r1) {
                switch (r.schema()->leafAt(i)->type()) {
                    case CORES_LONG: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int64_t tmp = blockreaders[i]->next<int64_t>();
                        r.fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case CORES_INT: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int tmp = blockreaders[i]->next<int>();
                        r[0]->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case CORES_STRING: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        string tmp = blockreaders[i]->next<char *>();
                        r[0]->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case CORES_FLOAT: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        float tmp = blockreaders[i]->next<float>();
                        r[0]->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case CORES_BYTES: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        char tmp = blockreaders[i]->next<char>();
                        r[0]->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case CORES_ARRAY: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int arrsize = blockreaders[i]->next<int>();
                        rind[i]++;
                        //coutarrsize<<endl;
                        vector<GenericDatum> records;
                        for (int j = 0; j < arrsize; ++j) {
                            for (int k :r2) {
                                switch (r[1]->fieldAt(k - 10).type()) {
                                    case CORES_LONG: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        int64_t tmp = blockreaders[k]->next<int64_t>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        //couttmp<<" ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_INT: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        int tmp = blockreaders[k]->next<int>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        //couttmp<<" ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_STRING: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        string tmp = blockreaders[k]->next<char *>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        //couttmp<<" ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_FLOAT: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        float tmp = blockreaders[k]->next<float>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        //couttmp<<" ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_BYTES: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        char tmp = blockreaders[k]->next<char>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        //cout<<tmp<<" ";
                                        rind[k]++;
                                        break;
                                    }
                                }
                            }
                            records.push_back(GenericDatum(r[1]));
                        }
                        r[0]->fieldAt(9).value<GenericArray>().value() = records;
                        records.clear();
//                    cout<<endl;
                    }
//            cout<<endl;
//                cout<<endl;
                }
            }
            return true;
        }
        return false;
    }

    bool nextprint(vector<int> r1, vector<int> r2) {
        if (ind < headreader->getRowCount()) {
            ind++;
            for (int i :r1) {
                switch (r[0]->schema()->leafAt(i)->type()) {
                    case CORES_LONG: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int64_t tmp = blockreaders[i]->next<int64_t>();
                        r[0]->fieldAt(i) = tmp;
                        cout << tmp << " ";
                        rind[i]++;
                        break;
                    }
                    case CORES_INT: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int tmp = blockreaders[i]->next<int>();
                        r[0]->fieldAt(i) = tmp;
                        cout << tmp << " ";
                        rind[i]++;
                        break;
                    }
                    case CORES_STRING: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        string tmp = blockreaders[i]->next<char *>();
                        r[0]->fieldAt(i) = tmp;
                        cout << tmp << " ";
                        rind[i]++;
                        break;
                    }
                    case CORES_FLOAT: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        float tmp = blockreaders[i]->next<float>();
                        r[0]->fieldAt(i) = tmp;
                        cout << tmp << " ";
                        rind[i]++;
                        break;
                    }
                    case CORES_BYTES: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        char tmp = blockreaders[i]->next<char>();
                        r[0]->fieldAt(i) = tmp;
                        cout << tmp << " ";
                        rind[i]++;
                        break;
                    }
                    case CORES_ARRAY: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        int arrsize = blockreaders[i]->next<int>();
                        rind[i]++;
                        cout << arrsize << endl;
                        vector<GenericDatum> records;
                        for (int j = 0; j < arrsize; ++j) {
                            for (int k :r2) {
                                switch (r[1]->fieldAt(k - 10).type()) {
                                    case CORES_LONG: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        int64_t tmp = blockreaders[k]->next<int64_t>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        cout << tmp << " ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_INT: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        int tmp = blockreaders[k]->next<int>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        cout << tmp << " ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_STRING: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        string tmp = blockreaders[k]->next<char *>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        cout << tmp << " ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_FLOAT: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        float tmp = blockreaders[k]->next<float>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        cout << tmp << " ";
                                        rind[k]++;
                                        break;
                                    }
                                    case CORES_BYTES: {
                                        if (rind[k] == rcounts[k]) {
                                            blockreaders[k]->loadFromFile();
                                            rind[k] = 0;
                                            rcounts[k] = headreader->getColumns()[k].getBlock(bind[k]).getRowcount();
                                            bind[k]++;
                                        }
                                        char tmp = blockreaders[k]->next<char>();
                                        r[1]->fieldAt(k - 10) = tmp;
                                        cout << tmp << " ";
                                        rind[k]++;
                                        break;
                                    }
                                }
                            }
                            records.push_back(GenericDatum(r[1]));
                        }
                        r[0]->fieldAt(9).value<GenericArray>().value() = records;
                        records.clear();
//                    cout<<endl;
                    }
//            cout<<endl;
//                cout<<endl;
                }
            }
            return true;
        }
        return false;
    }
*/
    GenericRecord &getRecord() {
        return r;
    }
};

#endif //CORES_BATCHFILEWRITER_H
