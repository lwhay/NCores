//
// Created by Vince Tu on 2019/11/20.
//

#include <locale>
#include <stack>
#include <string>
#include <sstream>
#include <cstring>
#include <map>
#include <vector>
#include <any>
#include <memory>
#include <iostream>
#include <set>
#include <fstream>
#include "JsonParser.h"
#include "Node.h"
#include "Generic.h"
#include "Schema.h"
#include "BlockCache.h"
#include "utils.h"
#include "BatchFileWriter.h"
#include "Parser.h"

struct recordReader {
    GenericRecord *record;
public:
    recordReader() {}

    recordReader(GenericRecord *r) : record(r) {}

    void readLine(string line) {
        int i = record->fieldCount();
        vector<string> v;
        SplitString(line, v, "|");
        for (int i = 0; i < v.size(); ++i) {
            switch (record->fieldAt(i).type()) {
                case AVRO_INT: {
                    int tmp = stoi(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case AVRO_FLOAT: {
                    float tmp = stof(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case AVRO_LONG: {
                    int64_t tmp = stol(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case AVRO_DOUBLE: {
                    double tmp = stod(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case AVRO_BYTES: {
                    char tmp = ((char *) v[i].c_str())[0];
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case AVRO_STRING: {
                    record->fieldAt(i) = v[i];
                    record->schema()->setValid(i);
                    break;
                }
            }
        }
    }

    GenericRecord &getRecord() {
        return *record;
    }
};


void PPSLfilesMerge(string file1, string file2, string file3, string path) {
    ifstream file_1;
    file_1.open(file1 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader1(new HeadReader());
    headreader1->readHeader(file_1);
    file_1.close();

    ifstream file_2;
    file_2.open(file2 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader2(new HeadReader());
    headreader2->readHeader(file_2);
    file_2.close();

    ifstream file_3;
    file_3.open(file3 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader3(new HeadReader());
    headreader3->readHeader(file_3);
    file_3.close();

    fstream fo;
    int column_num = headreader1->getColumnCount() + headreader2->getColumnCount() + headreader3->getColumnCount();
    int blocksize = headreader1->getBlockSize();
    fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
    long *foffsets = new long[column_num]();
    int rowcount = headreader1->getRowCount();
    fo.write((char *) &blocksize, sizeof(blocksize));
    fo.write((char *) &rowcount, sizeof(rowcount));
    fo.write((char *) &column_num, sizeof(column_num));
    //fo << schema ;
    //metadata
    long *result = new long[column_num]();
    for (int j = 0; j < headreader1->getColumnCount(); ++j) {
        int blockCount = headreader1->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j] = fo.tellg();
        fo.write((char *) &foffsets[j], sizeof(long));
        for (int i = 0; i < headreader1->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader1->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader1->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    for (int j = 0; j < headreader2->getColumnCount(); ++j) {
        int blockCount = headreader2->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j + headreader1->getColumnCount()] = fo.tellg();
        fo.write((char *) &foffsets[j + headreader1->getColumnCount()], sizeof(long));
        for (int i = 0; i < headreader2->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader2->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader2->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    for (int j = 0; j < headreader3->getColumnCount(); ++j) {
        int blockCount = headreader3->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j + headreader1->getColumnCount() + headreader2->getColumnCount()] = fo.tellg();
        fo.write((char *) &foffsets[j + headreader1->getColumnCount() + headreader2->getColumnCount()], sizeof(long));
        for (int i = 0; i < headreader3->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader3->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader3->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    result[0] = fo.tellg();
    fo.close();
    FILE *fp = fopen((path + "/fileout.dat").data(), "ab+");
    char buffer[blocksize];
    long *tmplong = (long *) buffer;
    FILE *f1 = fopen((file1 + "/fileout.dat").data(), "rb");
    fseek(f1, headreader1->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f1)) {
        int tmp = ftell(fp);
        fread(buffer, sizeof(char), blocksize, f1);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f1);
    result[headreader1->getColumnCount()] = ftell(fp);
    FILE *f2 = fopen((file2 + "/fileout.dat").data(), "rb");
    fseek(f2, headreader2->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f2)) {
        fread(buffer, sizeof(char), blocksize, f2);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f2);

    result[headreader1->getColumnCount() + headreader2->getColumnCount()] = ftell(fp);
    FILE *f3 = fopen((file3 + "/fileout.dat").data(), "rb");
    fseek(f3, headreader3->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f3)) {
        fread(buffer, sizeof(char), blocksize, f3);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f3);
    fflush(fp);
    fclose(fp);

    for (int j = 1; j < headreader1->getColumnCount(); ++j) {
        result[j] = result[j - 1] + headreader1->getColumn(j).getOffset() - headreader1->getColumn(j - 1).getOffset();
    }
    for (int j = 1; j < headreader2->getColumnCount(); ++j) {
        result[j + headreader1->getColumnCount()] =
                result[j - 1 + headreader1->getColumnCount()] + headreader2->getColumn(j).getOffset() -
                headreader2->getColumn(j - 1).getOffset();
    }
    for (int j = 1; j < headreader3->getColumnCount(); ++j) {
        result[j + headreader1->getColumnCount() + headreader2->getColumnCount()] =
                result[j - 1 + headreader1->getColumnCount() + headreader2->getColumnCount()] +
                headreader3->getColumn(j).getOffset() - headreader3->getColumn(j - 1).getOffset();
    }
    fp = fopen((path + "/fileout.dat").data(), "rb+");
    for (int m = 0; m < column_num; ++m) {
        fseek(fp, foffsets[m], SEEK_SET);
        fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
    }
    fflush(fp);
    fclose(fp);
}


void PPSLWriter(char *tblfile0 = "../res/tpch/part.tbl", char *tblfile1 = "../res/tpch/partsupp.tbl",
                char *tblfile2 = "../res/tpch/lineitem.tbl",
                char *result0 = "./part", char *result1 = "./partsupp", char *result2 = "./lineitem",
                char *result = "./ppsl",
                int blocksize = 1024) {
    SchemaReader sr("../res/schema/ppsl/nest.avsc", true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<recordReader> rrs;
    GenericRecord r(c.value<GenericRecord>());
    rs.push_back((r));
    rrs.push_back(recordReader());
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == AVRO_ARRAY) {
            c = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = c.value<GenericRecord>();
            rs.push_back((r));
            rrs.push_back(recordReader());
            j = 0;
        }
    }
    for (int k = 0; k < rs.size(); ++k) {
        rrs[k].record = &rs[k];
    }
    fstream lf(tblfile2, ios::in);
    string ll;
    vector<vector<vector<GenericDatum>>> vl;
    vector<vector<GenericDatum>> vs;
    vector<GenericDatum> vp;
    while (std::getline(lf, ll)) {
        rrs[2].readLine(ll);
        GenericRecord tmp = rrs[2].getRecord();
        int64_t partkey = tmp.fieldAt(1).value<int64_t>();
        int64_t supkey = tmp.fieldAt(2).value<int64_t>();
        if (partkey > vl.size()) {
            vl.resize(partkey, vs);
        }
        if (supkey > vl[partkey - 1].size()) {
            vl[partkey - 1].resize(supkey, vp);
        }
        vl[partkey - 1][supkey - 1].push_back(GenericDatum(tmp));
    }
    fstream psf(tblfile1, ios::in);
    string psl;
    while (std::getline(psf, psl)) {
        rrs[1].readLine(psl);
        GenericRecord tmp = rrs[1].getRecord();
        int partkey = tmp.fieldAt(0).value<int64_t>();
        int supkey = tmp.fieldAt(1).value<int64_t>();
        if (partkey > vs.size()) {
            vs.resize(partkey, vp);
        }if(vl[partkey - 1][supkey - 1].size()!=0){
            tmp.fieldAt(5).value<GenericArray>().value() = vl[partkey - 1][supkey - 1];
            tmp.schema()->setValid(5);
            vl[partkey - 1][supkey - 1].clear();
        }
        vs[partkey - 1].push_back(GenericDatum(tmp));
    }
    for (int i = 0; i < vl.size(); ++i) {
        for (int j = 0; j < vl[i].size(); ++j) {
            if (vl[i][j].size() != 0) {
                GenericRecord tmp = rs[1];
                tmp.fieldAt(5).value<GenericArray>().value() = vl[i][j];
                tmp.schema()->setValid(5);
                int64_t partkey = vl[i][j][0].value<GenericRecord>().fieldAt(1).value<int64_t>();
                vs[partkey - 1].push_back(GenericDatum(tmp));
            }
        }
    }
    fstream pf(tblfile0, ios::in);
    string pl;
    while (std::getline(pf, pl)) {
        rrs[0].readLine(pl);
        GenericRecord tmp = rrs[0].getRecord();
        int partkey = tmp.fieldAt(0).value<int64_t>();
        tmp.fieldAt(9).value<GenericArray>().value() = vs[partkey - 1];
//        cout<<partkey<<" "<<vs[partkey-1].size()<<endl;
        tmp.schema()->setValid(9);
        vs[partkey - 1].clear();
        vp.push_back(GenericDatum(tmp));
    }

    BatchFileWriter part(rs[0], result0, blocksize);
    BatchFileWriter partsupp(rs[1], result1, blocksize);
    BatchFileWriter lineitem(rs[2], result2, blocksize);

    for (auto iter0:vp) {
        part.writeRecord(iter0.value<GenericRecord>());
        for (auto iter1:iter0.value<GenericRecord>().fieldAt(9).value<GenericArray>().value()) {
            partsupp.writeRecord(iter1.value<GenericRecord>());
            for (auto iter2:iter1.value<GenericRecord>().fieldAt(5).value<GenericArray>().value()) {
                lineitem.writeRecord(iter2.value<GenericRecord>());
            }
        }
    }


    part.writeRest();
    part.mergeFiles();
    partsupp.writeRest();
    partsupp.mergeFiles();
    lineitem.writeRest();
    lineitem.mergeFiles();

    PPSLfilesMerge(result0, result1, result2, result);
}

void nextread(char *datafile, char *schemafile = "../res/schema/ppsl/nest.avsc") {
    SchemaReader sr(schemafile, true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(c.value<GenericRecord>());
    rs.push_back((r));
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == AVRO_ARRAY) {
            c = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = c.value<GenericRecord>();
            rs.push_back((r));
            bs.push_back(bs.back() + j + 1);
            cout << bs.back() << endl;
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount() + 1);
    cout << bs.back() << endl;
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    fileReader part(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile);
    fileReader partsupp(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile);
    fileReader lineitem(GenericDatum(rs[2]), headreader, bs[2], bs[3] - 2, datafile);

    vector<GenericDatum> &pss = part.getArr(bs[1] - 1);
    pss = vector<GenericDatum>();
    vector<GenericDatum> &ls = partsupp.getArr(bs[2] - bs[1] - 1);
    ls = vector<GenericDatum>();

    int ind=0,indp=0,indps=0;
    while (part.next()) {
        indp++;
//    part.next();
        int i = part.getArrsize();
        for (int j = 0; j < i; ++j) {
            partsupp.next();
            indps++;
            pss.push_back(GenericDatum(partsupp.getRecord()));
            int k = partsupp.getArrsize();
            for (int l = 0; l < k; ++l) {
                lineitem.next();
                ind++;
                ls.push_back(GenericDatum(lineitem.getRecord()));
            }
            if (ls.size() != 0)
                ls.clear();
        }
        if (pss.size() != 0)
            pss.clear();
    }

    cout<<"\n"<<indp;
    cout<<"\n"<<indps;
    cout<<"\n"<<ind;
//
//    int ind=0;
//    cout<<ind<<":"<<endl;
//    while (lineitem.next()){
//        ind++;
//        cout<<"\n"<<ind<<":";
//    };

}

void filetest(){
    SchemaReader sr("../res/schema/ppsl/nest.avsc", true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());

    ifstream file_in;
    file_in.open("./part/fileout.dat", ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << "./ppsl/fileout.dat" << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();/*
    FILE* f=fopen("./part/fileout.dat","rb");
    int i=2;
    cout<<headreader->getColumns()[0].getOffset()<<" "<<headreader->getColumns()[1].getOffset()<<" "<<headreader->getColumns()[2].getOffset()<<endl;
    fseek(f,headreader->getColumns()[i].getOffset(), SEEK_SET);
    Block* blockreader = new Block(f, 0L, 0, headreader->getBlockSize());
    int rmax=headreader->getColumns()[i].getBlock(0).getRowcount();
    blockreader->loadFromFile(rmax);
    int offsize=2;
    vector<int>* offarrs = blockreader->initString(offsize);
    int tmpi = (*offarrs)[0];
    char *tmp = blockreader->getoffstring(tmpi);
    cout<<tmpi<<" "<<tmp<<endl;*/

    FILE* f=fopen("./part/file8.data","rb");
    int i=8;
    Block* blockreader = new Block(f, 0L, 0, headreader->getBlockSize());
    int rsize=headreader->getColumns()[i].getBlock(0).getRowcount();
    blockreader->loadFromFile(rsize);
    vector<int>* offarrs = blockreader->initString(2);
    int tmpi = (*offarrs)[0];
    char *tmp = blockreader->getoffstring(tmpi);
    cout<<blockreader->isvalid(0)<<endl;
    cout<<rsize<<" "<<tmp<<endl;
    tmpi = (*offarrs)[1];
    tmp = blockreader->getoffstring(tmpi);
    cout<<blockreader->isvalid(0)<<endl;
    cout<<rsize<<" "<<tmp<<endl;
    rsize=headreader->getColumns()[i].getBlock(1).getRowcount();
    blockreader->loadFromFile(rsize);
    offarrs = blockreader->initString(2);
    tmpi = (*offarrs)[0];
    tmp = blockreader->getoffstring(tmpi);
    cout<<blockreader->isvalid(0)<<endl;
    cout<<rsize<<" "<<tmp<<endl;
}

int main() {
    system("mkdir part");
    system("mkdir partsupp");
    system("mkdir lineitem");
    system("mkdir ppsl");
//    PPSLWriter();
    nextread("./ppsl/fileout.dat");
//    filetest();
    return 0;
}


