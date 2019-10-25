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

    vector<ColumnReader> getColumns() {
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

    BatchFileWriter(GenericDatum record, string path, int blocksize=1024) {
        this->record = new GenericRecord(record.value<GenericRecord>());
        this->path = path;
        this->blocksize = blocksize;
        column_num = this->record->fieldCount();
        head_files = (FILE **) (malloc(column_num * sizeof(FILE *)));
        data_files = (FILE **) (malloc(column_num * sizeof(FILE *)));
        data_buffers = (char **) (malloc(column_num * sizeof(char *)));
        heads_info = (int **) (malloc(column_num * sizeof(int *)));
        head_index = new int[column_num]();
        data_index = new int[column_num]();
        file_offset = new int[column_num]();
        block_num = new int[column_num]();
        strl = new int[column_num]();

        for (int i = 0; i < column_num; ++i) {
            head_files[i] = fopen((path + "/file" + to_string(i) + ".head").data(), "wb+");
            data_files[i] = fopen((path + "/file" + to_string(i) + ".data").data(), "wb+");
            data_buffers[i] = new char[blocksize];
            heads_info[i] = new int[blocksize];
        }
        if(!head_files[0]) {
            std::perror("File opening failed");
            return EXIT_FAILURE;
        }

    }

private:
    string path;
    int blocksize;
    GenericRecord *record;
    int column_num;
    FILE **data_files;
    FILE **head_files;
    char **data_buffers;
    int **heads_info;
    int *data_index;
    int *head_index;
    int *file_offset;
    int *block_num;
    int *strl;

    FILE *outfile;

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

public:

    long getInd(int ind) {
        return record->fieldAt(ind).value<long>();
    }

    void setArr(int ind, vector<GenericDatum> arr) {
        record->fieldAt(ind).value<GenericArray>().value() = arr;
    }

    GenericRecord getRecord() {
        return *record;
    }

    void setRecord(GenericRecord _record) {
        delete (record);
        this->record = new GenericRecord(_record);
        int tmp = this->record->fieldAt(1).value<long>();
    }

//    GenericDatum getRecord(){
//        return GenericDatum(*record);
//    }

    void readLine(string line) {
        vector<string> v;
        SplitString(line, v, "|");
        for (int i = 0; i < column_num; ++i) {
            switch (record->fieldAt(i).type()) {
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
                    long tmp = stol(v[i]);
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
            switch (record->fieldAt(i).type()) {
                case AVRO_INT: {
                    int tmp = record->fieldAt(i).value<int>();
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
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
                    break;
                }
                case AVRO_LONG: {
                    long tmp = record->fieldAt(i).value<long>();
                    *(long *) (&(data_buffers[i][data_index[i]])) = tmp;
                    data_index[i] += sizeof(long);
                    int n = 0;
                    if (data_index[i] >= blocksize) {
                        if (i == 0) {
//                        if(n==0){
//                            long* tmp_l=(long*)(data_buffers[0]);
//                            for (int i = 0; i <blocksize/ sizeof(long) ; ++i) {
//                                cout<<*tmp_l<<" ";
//                                tmp_l++;
//                            }
//                        }
                        }
                        data_index[i] = 0;
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = blocksize / sizeof(long);
                        file_offset[i] += blocksize / sizeof(long);
                        heads_info[i][head_index[i]++] = file_offset[i];
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    break;
                }
                case AVRO_DOUBLE: {
                    double tmp = record->fieldAt(i).value<double>();
                    *(double *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(double);
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
                    break;
                }
                case AVRO_FLOAT: {
                    float tmp = record->fieldAt(i).value<float>();
                    *(float *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(float);
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
                    break;
                }
                case AVRO_BYTES: {
                    char tmp = record->fieldAt(i).value<char>();
                    *(char *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(char);
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
                    break;
                }
                case AVRO_STRING: {
                    string tmp = record->fieldAt(i).value<string>();
                    if ((data_index[i] + tmp.length() + 1) > blocksize) {
                        fwrite(data_buffers[i], sizeof(char), blocksize, data_files[i]);
                        block_num[i]++;
                        memset(data_buffers[i], 0, blocksize * sizeof(char));
                        heads_info[i][head_index[i]++] = strl[i];
                        file_offset[i] += strl[i];
                        heads_info[i][head_index[i]++] = file_offset[i];
                        data_index[i] = 0;
                        strl[i] = 0;
                        if (head_index[i] >= blocksize) {
                            fwrite(heads_info[i], sizeof(int), blocksize, head_files[i]);
                            memset(heads_info[i], 0, blocksize * sizeof(int));
                            head_index[i] = 0;
                        }
                    }
                    std::strcpy(&data_buffers[i][data_index[i]], tmp.c_str());
                    data_index[i] += tmp.length() + 1;
                    strl[i]++;
                    break;
                }
                case AVRO_ARRAY: {
                    int tmp = record->fieldAt(i).value<GenericArray>().value().size();
                    *(int *) (&data_buffers[i][data_index[i]]) = tmp;
                    data_index[i] += sizeof(int);
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
                    break;
                }
            }
        }

    }

    void writeRest() {
        for (int k = 0; k < column_num; ++k) {
            if (data_index[k] != 0) {
                fwrite(data_buffers[k], sizeof(char), blocksize, data_files[k]);
                block_num[k]++;
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
    GenericRecord **r;
    FILE **fpp;
    unique_ptr<HeadReader> headreader;
    int *rind;
    int *bind;
    int *rcounts;
    int blocksize;
    Block **blockreaders;
    int ind;

public:
    fileReader(GenericDatum c, int _blocksize) {
        ind = 0;
        r = new GenericRecord *[2];
        r[0] = new GenericRecord(c.value<GenericRecord>());
        GenericDatum t = r[0]->fieldAt(9);
        c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
        r[1] = new GenericRecord(c.value<GenericRecord>());
        ifstream file_in;
        file_in.open("./fileout.dat", ios_base::in | ios_base::binary);
        unique_ptr<HeadReader> _headreader((new HeadReader()));
        headreader = std::move(_headreader);
        headreader->readHeader(file_in);
        file_in.close();
        fpp = new FILE *[26];
        rind = new int[26]();
        bind = new int[26]();
        rcounts = new int[26]();
        blockreaders = new Block *[26];
        blocksize = _blocksize;
        for (int i = 0; i < 26; ++i) {
            fpp[i] = fopen("./fileout.dat", "rb");
            fseek(fpp[i], headreader->getColumns()[i].getOffset(), SEEK_SET);
            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
            blockreaders[i] = new Block(fpp[i], 0L, 0, blocksize);
            blockreaders[i]->loadFromFile();
        }
    }

    bool next(vector<int> r1, vector<int> r2) {
        if (ind < headreader->getRowCount()) {
            ind++;
            for (int i :r1) {
                switch (r[0]->fieldAt(i).type()) {
                    case AVRO_LONG: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        long tmp = blockreaders[i]->next<long>();
                        r[0]->fieldAt(i) = tmp;
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
                                        long tmp = blockreaders[k]->next<long>();
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
                switch (r[0]->fieldAt(i).type()) {
                    case AVRO_LONG: {
                        if (rind[i] == rcounts[i]) {
                            blockreaders[i]->loadFromFile();
                            rind[i] = 0;
                            rcounts[i] = headreader->getColumns()[i].getBlock(bind[i]).getRowcount();
                            bind[i]++;
                        }
                        long tmp = blockreaders[i]->next<long>();
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
                                        long tmp = blockreaders[k]->next<long>();
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

    GenericRecord &getRecord() {
        return *(r[0]);
    }
};

#endif //CORES_BATCHFILEWRITER_H
