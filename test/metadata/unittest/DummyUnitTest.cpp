//
// Created by iclab on 12/6/19.
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
#include <gtest/gtest.h>
#include "ColumnarData.h"


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
                case CORES_INT: {
                    int tmp = stoi(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = stof(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case CORES_LONG: {
                    int64_t tmp = stol(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case CORES_DOUBLE: {
                    double tmp = stod(v[i]);
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case CORES_BYTES: {
                    char tmp = ((char *) v[i].c_str())[0];
                    record->fieldAt(i) = tmp;
                    record->schema()->setValid(i);
                    break;
                }
                case CORES_STRING: {
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


void requiredfilesMerge(string file1, string file2, string path) {
    cout << "file1" << endl;
    ifstream file_1;
    file_1.open(file1 + "/fileout.dat", ios_base::in | ios_base::binary);
    HeadReader* headreader1(new HeadReader());
    headreader1->readHeader(file_1);
    file_1.close();

    cout << "file2" << endl;
    ifstream file_2;
    file_2.open(file2 + "/fileout.dat", ios_base::in | ios_base::binary);
    HeadReader* headreader2(new HeadReader());
    headreader2->readHeader(file_2);
    file_2.close();

    fstream fo;
    int column_num = headreader1->getColumnCount() + headreader2->getColumnCount();
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
    fp = fopen((path + "/fileout.dat").data(), "rb+");
    for (int m = 0; m < column_num; ++m) {
        fseek(fp, foffsets[m], SEEK_SET);
        fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
    }
    fflush(fp);
    fclose(fp);
}


void requiredWriter(char *tblfile1 = "../res/test/requiredtest/orders.tbl",
                    char *tblfile2 = "../res/test/requiredtest/lineitem.tbl",
                    char *result0 = "./layer0", char *result1 = "./layer1",
                    char *result = "./tmpresult", char *schema = "../res/test/requiredtest/nest.avsc",
                    int blocksize = 1024) {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<recordReader> rrs;
    GenericRecord r(c.value<GenericRecord>());
    rs.push_back((r));
    rrs.push_back(recordReader());
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
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
    vector<vector<GenericDatum>> vl;
    vector<GenericDatum> vo;
    cout<<"line reading"<<endl;
    while (std::getline(lf, ll)) {
        rrs[1].readLine(ll);
        GenericRecord tmp = rrs[1].getRecord();
        int64_t orderkey = tmp.fieldAt(0).value<int64_t>();
        if (orderkey > vl.size()) {
            vl.resize(orderkey, vo);
        }
        vl[orderkey - 1].push_back(GenericDatum(tmp));
    }
    fstream of(tblfile1, ios::in);
    string ol;
    int indpsl = 0;
    cout<<"orders reading"<<endl;
    while (std::getline(of, ol)) {
        rrs[0].readLine(ol);
        indpsl++;
        GenericRecord tmp = rrs[0].getRecord();
        int orderkey = tmp.fieldAt(0).value<int64_t>();

        tmp.fieldAt(9).value<GenericArray>().value() = vl[orderkey - 1];
//        cout<<partkey<<" "<<vs[partkey-1].size()<<endl;
        vl[orderkey - 1].clear();
        vo.push_back(GenericDatum(tmp));
    }
    BatchFileWriter file0(rs[0], result0, blocksize,true);
    BatchFileWriter file1(rs[1], result1, blocksize,true);

    cout<<"writing"<<endl;
    for (auto iter0:vo) {
        file0.write(iter0.value<GenericRecord>());
        for (auto iter1:iter0.value<GenericRecord>().fieldAt(9).value<GenericArray>().value()) {
            file1.write(iter1.value<GenericRecord>());
        }
    }


    file0.writeRest(true);
    file0.mergeFiles(true);
    file1.writeRest(true);
    file1.mergeFiles(true);

    requiredfilesMerge(result0, result1, result);
}

int requiredReader(bool flag, char *datafile = "./tmpresult/fileout.dat",
                   char *schema = "../res/test/requiredtest/nest.avsc") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(c.value<GenericRecord>());
    rs.push_back((r));
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
            c = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = c.value<GenericRecord>();
            rs.push_back((r));
            bs.push_back(bs.back() + j + 1);
            cout << bs.back() << endl;
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount());
    cout << bs.back() << endl;
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    fileReader fr0(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile);
    fileReader fr1(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile);

    vector<GenericDatum> &pss = fr0.getArr(bs[1] - 1);
    pss = vector<GenericDatum>();

    for (int k = 0; k <headreader->getColumn(0).getblockCount() ; ++k) {
//        cout<<headreader->getColumn(0).getBlock(k).getOffset()<<" "<<headreader->getColumn(0).getBlock(k).getRowcount()<<endl;

    }

    int ind = 0, indp = 0, indps = 0;
    while (fr0.next()) {
//        cout<<endl;
        indp++;
        int64_t key=fr0.getRecord().fieldAt(0).value<int64_t >();
        int i = fr0.getArrsize();
        for (int j = 0; j < i; ++j) {
            fr1.next();
//            cout<<endl;
            indps++;
            pss.push_back(GenericDatum(fr1.getRecord()));
        }
//        cout<<endl;
        if (pss.size() != 0)
            pss.clear();
    }

    cout << "\n" << ind;





    if (flag)
        return indp;
    else
        return indps;

}

void SETUP() {
    system("mkdir layer0");
    system("mkdir layer1");
    system("mkdir tmpresult");

    requiredWriter("/kolla/asterixdb/tpch_2_16_1/dbgen/orders.tbl","/kolla/asterixdb/tpch_2_16_1/dbgen/lineitem.tbl");
}

TEST(SchemaTest, DummyTest) {
    EXPECT_EQ(1500 , requiredReader(true));
    EXPECT_EQ(6005 , requiredReader(false));
}

void TEARDOWN() {
    system("rd /s/q layer0");
    system("rd /s/q layer1");
    system("rd /s/q tmpresult");
}

int main(int argc, char **argv) {
//    SETUP();
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    TEARDOWN();
    return ret;
}
