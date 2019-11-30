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
    BlockReader() : lengthOffset(0) {}

    BlockReader(istream is) {
        is.read((char *) &rowCount, sizeof(rowCount));
        is.read((char *) &size, sizeof(size));
    }

    void writeTo(ostream os) {
        os.write((char *) &rowCount, sizeof(rowCount));
        os.write((char *) size, sizeof(size));
    }

    int getOffset() {
        return lengthOffset;
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

    void setBlocks(vector<BlockReader> _blocks) {
        this->blocks.assign(_blocks.begin(), _blocks.begin() + _blocks.size());
    }

    vector<BlockReader> getBlocks() {
        return blocks;
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
        int low = 0, high = blockCount, middle = 0;
        while (low < high) {
            middle = (low + high) / 2;
            if (off <= blocks[middle].getOffset() && off > blocks[middle - 1].getOffset()) {
                roff = off - blocks[middle - 1].getOffset();
                return middle;
            } else if (off < blocks[middle - 1].getOffset()) {
                high = middle;
            } else if (off > blocks[middle].getOffset()) {
                low = middle + 1;
            }
        }
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


class BatchFileWriter {
    BatchFileWriter() {

    }

public:

    BatchFileWriter(GenericDatum record, string path, int blocksize = 1024) {
        this->record = new GenericRecord(record.value<GenericRecord>());
        this->path = path;
        this->blocksize = blocksize;
        if (blocksize < 1 << 8) offsize = 1;
        else if (blocksize < 1 << 16) offsize = 2;
        else if (blocksize < 1 << 24) offsize = 3;
        else if (blocksize < 1 << 32) offsize = 4;
        else cout << "blocksize is too large" << endl;
        column_num = this->record->fieldCount();
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
                case AVRO_INT: {
                    int tmp = stoi(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case AVRO_FLOAT: {
                    float tmp = stof(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case AVRO_LONG: {
                    int64_t tmp = stol(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case AVRO_DOUBLE: {
                    double tmp = stod(v[i]);
                    record->fieldAt(i) = tmp;
                    break;
                }
                case AVRO_BYTES: {
                    char tmp = ((char *) v[i].c_str())[0];
                    record->fieldAt(i) = tmp;
                    break;
                }
                case AVRO_STRING: {
                    record->fieldAt(i) = v[i];
                    break;
                }
            }
        }
    }

    void writeRecord() {
        for (int i = 0; i < column_num; ++i) {
            switch (record->schema()->leafAt(i)->type()) {
                case AVRO_INT: {
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
                case AVRO_LONG: {
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
                case AVRO_DOUBLE: {
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
                case AVRO_FLOAT: {
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
                case AVRO_BYTES: {
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
                case AVRO_STRING: {
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
                case AVRO_ARRAY: {
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

    void writeRecord(const GenericRecord &r) {
        for (int i = 0; i < column_num; ++i) {
            switch (r.schema()->leafAt(i)->type()) {
                case AVRO_INT: {
                    int tmp = r.fieldAt(i).value<int>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size(), data_files[i]);
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
                case AVRO_LONG: {
                    int64_t tmp = r.fieldAt(i).value<int64_t>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size(), data_files[i]);
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
                case AVRO_DOUBLE: {
                    double tmp = r.fieldAt(i).value<double>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size(), data_files[i]);
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
                case AVRO_FLOAT: {
                    float tmp = r.fieldAt(i).value<float>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size(), data_files[i]);
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
                case AVRO_BYTES: {
                    char tmp = r.fieldAt(i).value<char>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size(), data_files[i]);
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
                case AVRO_STRING: {
                    string tmp = r.fieldAt(i).value<string>();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if ((data_index[i] + tmp.length() + 1 + offsize * strl[i] + valid[i].size()) > blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        int tmpoff = offsize * strl[i] ;
                        for (int j :off_index[i]) {
                            int tmp = j + tmpoff;
                            fwrite((char *) &tmp, offsize, sizeof(char), data_files[i]);
                        }
                        fwrite(data_buffers[i], sizeof(char), blocksize-valid[i].size()-offsize * off_index[i].size(), data_files[i]);
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
                    const char* tmpc=tmp.c_str();
                    int tmpi=tmp.length();
                    off_index[i].push_back(data_index[i]);
                    std::strcpy(&data_buffers[i][data_index[i]], tmp.c_str());
                    data_index[i] += tmp.length() + 1;
                    strl[i]++;
                    break;
                }
                case AVRO_ARRAY: {
                    int tmp = r.fieldAt(i).value<GenericArray>().value().size();
                    int zero = 0;
                    if (strl[i] % 8 == 0) zero = 1;
                    if (data_index[i] + zero + valid[i].size() >= blocksize) {
                        fwrite((char *) &valid[i][0], valid[i].size(), sizeof(char), data_files[i]);
                        valid[i].clear();
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize - valid[i].size(), data_files[i]);
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

    void writeRest() {
        for (int k = 0; k < column_num; ++k) {
            if (data_index[k] != 0) {
                if(record->fieldAt(k).type()==AVRO_STRING){
                    fwrite((char *) &valid[k][0], valid[k].size(), sizeof(char), data_files[k]);
                    int tmpoff = offsize * strl[k] ;
                    for (int j :off_index[k]) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), data_files[k]);
                    }
                    fwrite(data_buffers[k], sizeof(char), blocksize-valid[k].size()-offsize * off_index[k].size(), data_files[k]);
                }else{
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

    void mergeFiles() {
        for (int i = 0; i < column_num; ++i) {
            head_files[i] = fopen((path + "/file" + to_string(i) + ".head").data(), "rb");
            data_files[i] = fopen((path + "/file" + to_string(i) + ".data").data(), "rb");
        }
        fstream fo;
        fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
        long *foffsets = new long[column_num];
        int rowcount = file_offset[0];
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
    GenericRecord *r;
    vector<FILE *>fpp;
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

public:
    fileReader(GenericDatum c, shared_ptr<HeadReader> _headreader, int _begin, int _end, char *resultfile) : begin(
            _begin), end(_end+1) {
        headreader = _headreader;
        ind = 0;
        r = new GenericRecord(c.value<GenericRecord>());
        ifstream file_in;
        int colnum = end - begin ;
        fpp = vector<FILE *>(colnum);
        rind = vector<int>(colnum);
        bind = vector<int>(colnum);
        rcounts = vector<int>(colnum);
        blockreaders = vector<Block *>(colnum);
        offarrs = vector<vector<int> *>(colnum);
        blocksize = headreader->getBlockSize();
        for (int i = begin; i < end; ++i) {
            fpp[i-begin] = fopen(resultfile, "rb");
            fseek(fpp[i-begin], headreader->getColumns()[i].getOffset(), SEEK_SET);
            rcounts[i-begin] = headreader->getColumns()[i].getBlock(bind[i-begin]).getRowcount();
            blockreaders[i-begin] = new Block(fpp[i-begin], 0L, 0, blocksize);
            blockreaders[i-begin]->loadFromFile(rcounts[i-begin]);
        }
        if (blocksize < 1 << 8) offsize = 1;
        else if (blocksize < 1 << 16) offsize = 2;
        else if (blocksize < 1 << 24) offsize = 3;
        else if (blocksize < 1 << 32) offsize = 4;
    }

//    fileReader(GenericDatum c, int _blocksize) {
//        ind = 0;
//        r = new GenericRecord ();
//        r[0] = new GenericRecord(c.value<GenericRecord>());
//        GenericDatum t = r[0]->fieldAt(9);
//        c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
//        r[1] = new GenericRecord(c.value<GenericRecord>());
//        ifstream file_in;
//        file_in.open("./fileout.dat", ios_base::in | ios_base::binary);
//        unique_ptr<HeadReader> _headreader((new HeadReader()));
//        headreader = std::move(_headreader);
//        headreader->readHeader(file_in);
//        file_in.close();
//        fpp = new FILE *[26];
//        rind = new int[26]();
//        bind = new int[26]();
//        rcounts = new int[26]();
//        blockreaders = new Block *[26];
//        blocksize = _blocksize;
//        for (int i = 0; i < 26; ++i) {
//            fpp[i] = fopen("./fileout.dat", "rb");
//            fseek(fpp[i], headreader->getColumns()[i].getOffset(), SEEK_SET);
//            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
//            blockreaders[i] = new Block(fpp[i], 0L, 0, blocksize);
//            blockreaders[i]->loadFromFile();
//        }
//    }

    void skipread(int off) {
        if (headreader->getColumn(begin).getOffset() < off) {
            cout << "out of index" << endl;
            return;
        }
        r->schema()->invalidate();
        for (int i = begin; i < end; ++i) {
            if (!blockreaders[i]->isvalid(rind[i])) {
                rind[i]++;
                continue;
            }
            switch (r->schema()->leafAt(i)->type()) {
                case AVRO_LONG: {
                    bind[i] = headreader->getColumn(i).findOff(off, rind[i]);
                    blockreaders[i]->skipload(bind[i] * blocksize);
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    int64_t tmp = blockreaders[i]->next<int64_t>();
                    r->schema()->setValid(i);
                    r->fieldAt(i) = tmp;
                    //couttmp<<" ";
                    rind[i]++;
                    break;
                }
                case AVRO_INT: {
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    int tmp = blockreaders[i]->next<int>();
                    r->schema()->setValid(i);
                    r->fieldAt(i) = tmp;
                    //couttmp<<" ";
                    rind[i]++;
                    break;
                }
                case AVRO_STRING: {
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    if (rind[i] == 0) {
                        offarrs[i] = blockreaders[i]->initString(offsize);
                    }
                    int tmpi = (*offarrs[i])[rind[i]];
                    char *tmp = blockreaders[i]->getoffstring((*offarrs[i])[rind[i]]);
                    r->schema()->setValid(i);
                    r->fieldAt(i) = tmp;
                    //couttmp<<" ";
                    rind[i]++;
                    break;
                }
                case AVRO_FLOAT: {
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    float tmp = blockreaders[i]->next<float>();
                    r->schema()->setValid(i);
                    r->fieldAt(i) = tmp;
                    //couttmp<<" ";
                    rind[i]++;
                    break;
                }
                case AVRO_BYTES: {
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    char tmp = blockreaders[i]->next<char>();
                    r->schema()->setValid(i);
                    r->fieldAt(i) = tmp;
                    //couttmp<<" ";
                    rind[i]++;
                    break;
                }
                case AVRO_ARRAY: {
                    if (rind[i] == rcounts[i]) {
                        bind[i]++;
                        rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                        blockreaders[i]->loadFromFile(rcounts[i]);
                        rind[i] = 0;
                    }
                    arsize = blockreaders[i]->next<int>();
                    rind[i]++;
                }
            }
        }
    }

    bool next() {
        if (bind[0] < headreader->getColumn(begin).getblockCount()) {
            r->schema()->invalidate();
//            if(r->schema()->name().fullname().compare("Part")==0)
//            cout<<"\n"<<r->schema()->name().fullname()<<" ";
            for (int i = 0; i < end-begin; ++i) {
                if (rind[i] == rcounts[i]) {
                    bind[i]++;
                    if(bind[i] == headreader->getColumn(begin+i).getblockCount())
                        return false;
                    rcounts[i] = headreader->getColumns()[i+begin].getBlock(bind[i]).getRowcount();
                    blockreaders[i]->loadFromFile(rcounts[i]);
                    rind[i] = 0;
                }
                if (!blockreaders[i]->isvalid(rind[i])) {
                    if(r->schema()->leafAt(i)->type()==AVRO_STRING&&rind[i]==0){
                        offarrs[i] = blockreaders[i]->initString(offsize);
                    }
                    rind[i]++;
                    continue;
                }
//                if(r->schema()->name().fullname().compare("Part")==0)
//                cout<<i<<" ";
                switch (r->schema()->leafAt(i)->type()) {
                    case AVRO_LONG: {
                        int64_t tmp = blockreaders[i]->next<int64_t>();
                        r->schema()->setValid(i);
                        r->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case AVRO_INT: {
                        int tmp = blockreaders[i]->next<int>();
                        r->schema()->setValid(i);
                        r->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case AVRO_STRING: {
                        if (rind[i] == 0) {
                            offarrs[i] = blockreaders[i]->initString(offsize);
                        }
                        int tmpi = (*offarrs[i])[rind[i]];
                        char *tmp = blockreaders[i]->getoffstring(tmpi);
                        r->schema()->setValid(i);
                        r->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case AVRO_FLOAT: {
                        float tmp = blockreaders[i]->next<float>();
                        r->schema()->setValid(i);
                        r->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case AVRO_BYTES: {
                        char tmp = blockreaders[i]->next<char>();
                        r->schema()->setValid(i);
                        r->fieldAt(i) = tmp;
                        //couttmp<<" ";
                        rind[i]++;
                        break;
                    }
                    case AVRO_ARRAY: {
                        arsize = blockreaders[i]->next<int>();
                        rind[i]++;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    int getArrsize() {
        return arsize;
    }

    void setArr(const vector<GenericDatum> &rs, int i) {
        if (r->schema()->leafAt(i)->type() == AVRO_ARRAY)
            r->fieldAt(i).value<GenericArray>().value() = rs;
        else
            cout << "this is not a arr" << endl;
    }

    vector<GenericDatum> &getArr(int i) {
        if (r->schema()->leafAt(i)->type() == AVRO_ARRAY)
            return r->fieldAt(i).value<GenericArray>().value();
        cout << "this is not a arr" << endl;
    }

/*
    bool next(vector<int> r1, vector<int> r2) {
        if (ind < headreader->getRowCount()) {
            ind++;
            for (int i :r1) {
                switch (r.schema()->leafAt(i)->type()) {
                    case AVRO_LONG: {
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
                    case AVRO_INT: {
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
                    case AVRO_STRING: {
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
                    case AVRO_FLOAT: {
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
                    case AVRO_BYTES: {
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
                    case AVRO_ARRAY: {
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
                                    case AVRO_LONG: {
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
                                    case AVRO_INT: {
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
                                    case AVRO_STRING: {
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
                                    case AVRO_FLOAT: {
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
                                    case AVRO_BYTES: {
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
                    case AVRO_LONG: {
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
                    case AVRO_INT: {
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
                    case AVRO_STRING: {
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
                    case AVRO_FLOAT: {
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
                    case AVRO_BYTES: {
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
                    case AVRO_ARRAY: {
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
                                    case AVRO_LONG: {
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
                                    case AVRO_INT: {
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
                                    case AVRO_STRING: {
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
                                    case AVRO_FLOAT: {
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
                                    case AVRO_BYTES: {
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
        return *r;
    }
};

#endif //CORES_BATCHFILEWRITER_H
