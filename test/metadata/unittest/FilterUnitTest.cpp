//
// Created by Vince Tu on 27/12/2019.
//

#include "Bitset.h"
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
#include "Filter.h"


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
            if (v[i].size() == 0) {
                record->fieldAt(i) = GenericDatum();
                continue;
            }
            switch (record->schema()->leafAt(i)->type()) {
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


void flatWriter(
        char *tblfile = "../res/test/optiontest/lineitem.tbl",
        char *result = "./tmpresult", char *schema = "../res/schema/single.avsc",
        int blocksize = 1024) {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    GenericRecord r(c.value<GenericRecord>());
    recordReader rr(&r);
    BatchFileWriter file0(r, result, blocksize, true);

    fstream lf(tblfile, ios::in);
    string ll;
    vector<vector<GenericDatum>> vl;
    vector<GenericDatum> vo;
    while (std::getline(lf, ll)) {
        rr.readLine(ll);
        file0.write(rr.getRecord());
    }


    file0.writeRest(true);
    file0.mergeFiles(true);

}

int optionReader(bool flag, char *datafile = "./tmpresult/fileout.dat",
                 char *schema = "../res/schema/single.avsc", char *fschema = "../res/test/filtertest/query.qsc") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum c = GenericDatum(vschema->root());
    GenericRecord r(c.value<GenericRecord>());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    fileReader fr0(GenericDatum(r), headreader, 0, r.fieldCount() - 1, datafile);
    int ind=0;

    SchemaReader fr(fschema, false);
    FetchTable ft;
    fr.read(&ft);
    for (FetchTable::const_iterator it= ft.begin();it!=ft.end();++it){
        cout<<it->first<<endl;
    }
    int rcount=headreader->getRowCount();
    Bitset bs(6005);

    while (fr0.next()) {
//        fr0.printRecord();
//        cout << "\n";
        bool flag=true;
        for (FetchTable::const_iterator it= ft.begin();it!=ft.end();++it){
            int ind=r.fieldIndex(it->first);
            switch (r.schema()->leafAt(ind)->type()){
                case CORES_FLOAT:
                    if(it->second.fn->filter((double)fr0.getRecord().fieldAt(ind).value<float>())){
                        continue;
                    }else{
                        flag= false;
                        break;
                    };
                case CORES_DOUBLE:
                    if(it->second.fn->filter((double)fr0.getRecord().fieldAt(ind).value<double>())){
                        continue;
                    }else{
                        flag= false;
                        break;
                    };
                case CORES_INT:
                    if(it->second.fn->filter((int64_t)fr0.getRecord().fieldAt(ind).value<int>())){
                        continue;
                    }else{
                        flag= false;
                        break;
                    };
                case CORES_LONG:
                    if(it->second.fn->filter((int64_t)fr0.getRecord().fieldAt(ind).value<int64_t>())){
                        continue;
                    }else{
                        flag= false;
                        break;
                    };
                case CORES_STRING:
                    if(it->second.fn->filter((string)fr0.getRecord().fieldAt(ind).value<const char*>())){
                        continue;
                    }else{
                        flag= false;
                        break;
                    };

            }
        }
        if (flag) {
            fr0.printRecord();
            cout<<"\n";
            bs.set(ind);
        }
        ind++;

    }

    return ind;

}

void SETUP() {
    system("mkdir layer0");
    system("mkdir layer1");
    system("mkdir tmpresult");

    flatWriter();
}

TEST(SchemaTest, DummyTest) {
    EXPECT_EQ(1500, optionReader(true));
}

void TEARDOWN() {
    system("rd /s/q layer0");
    system("rd /s/q layer1");
    system("rd /s/q tmpresult");
}

int main(int argc, char **argv) {
    SETUP();
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    TEARDOWN();
    return ret
}
