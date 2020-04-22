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
                 char *schema = "../res/test/requiredtest/nest.avsc",
                 char *fschema = "../res/test/filtertest/query.qsc") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum tmpGD = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(tmpGD.value<GenericRecord>());
    rs.push_back(tmpGD.value<GenericRecord>());
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
            tmpGD = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = tmpGD.value<GenericRecord>();
            rs.push_back(tmpGD.value<GenericRecord>());
            bs.push_back(bs.back() + j + 1);
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    fileReader fr1(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile);
    fileReader fr0(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile);
    int ind = 0;

    SchemaReader fr(fschema, false);
    FetchTable ft;
    fr.read(&ft);
    for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
        cout << it->first << endl;
    }
    int rcount = headreader->getColumn(bs[1]).getRowcount();
    Bitset bst(rcount);
    int res = 0;

    while (fr0.next()) {
//        fr0.printRecord();
//        cout << "\n";
        bool flag = true;
        for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
            int ind = r.fieldIndex(it->first);
            if (!flag) break;
            switch (r.schema()->leafAt(ind)->type()) {
                case CORES_FLOAT:
                    if (it->second.fn->filter((double) fr0.getRecord().fieldAt(ind).value<float>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_DOUBLE:
                    if (it->second.fn->filter((double) fr0.getRecord().fieldAt(ind).value<double>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_INT:
                    if (it->second.fn->filter((int64_t) fr0.getRecord().fieldAt(ind).value<int>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_LONG:
                    if (it->second.fn->filter((int64_t) fr0.getRecord().fieldAt(ind).value<int64_t>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_STRING:
                    if (it->second.fn->filter((string) fr0.getRecord().fieldAt(ind).value<const char *>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };

            }
        }
        if (flag) {
//            fr0.printRecord();
//            cout<<"\n";
            bst.set(ind);
            res++;
        }
        ind++;

    }

    return res;

}

//q1.txt
int bitsetReader(char *datafile = "./tmpresult/fileout.dat",
                 char *schema = "../res/test/requiredtest/nest.avsc",
                 char *fschema = "../res/test/filtertest/query.qsc") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum tmpGD = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(tmpGD.value<GenericRecord>());
    rs.push_back(tmpGD.value<GenericRecord>());
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
            tmpGD = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = tmpGD.value<GenericRecord>();
            rs.push_back(tmpGD.value<GenericRecord>());
            bs.push_back(bs.back() + j + 1);
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    fileReader fr1(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile);
    fileReader fr0(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile);

    SchemaReader fr(fschema, false);
    FetchTable ft;
    fr.read(&ft);
    for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
        cout << it->first << endl;
    }
    int rcount = headreader->getColumn(bs[1]).getRowcount();
    Bitset bst(rcount, -1);
    for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
        int ind = r.fieldIndex(it->first);
        int off = bst.get(0) ? 0 : bst.nextSetBit(0);
        auto curtype = r.schema()->leafAt(ind)->type();
        while (off != -1) {
            switch (curtype) {
                case CORES_FLOAT: {
                    if (!it->second.fn->filter((double) fr0.skipColRead<float>(off, ind)))
                        bst.clear(off);
                    break;
                }
                case CORES_DOUBLE: {
                    if (!it->second.fn->filter((double) fr0.skipColRead<double>(off, ind)))
                        bst.clear(off);
                    break;
                }
                case CORES_INT: {
                    if (!it->second.fn->filter((int64_t) fr0.skipColRead<int>(off, ind)))
                        bst.clear(off);
                    break;
                }
                case CORES_LONG: {
                    if (!it->second.fn->filter((int64_t) fr0.skipColRead<int64_t>(off, ind)))
                        bst.clear(off);
                    break;
                }
                case CORES_STRING: {
                    string tmp = fr0.skipColRead<const char *>(off, ind);
//                    if(ind==10) cout<<off<<" "<<tmp<<endl;
                    if (!it->second.fn->filter(tmp))
                        bst.clear(off);
                    break;
                }
            }
            off = bst.nextSetBit(off);
        }
    }

    fr0.fresh();
    int res = 0;

    int off = bst.get(0) ? 0 : bst.nextSetBit(0);
    while (off != -1) {
        res++;
        fr0.skipread(off);
        off = bst.nextSetBit(off);
//        fr0.printRecord();
//        cout<<"\n";
    }

    return res;
}

int queryTest(char *datafile = "./tmpresult/fileout.dat",
              char *schema = "../res/test/requiredtest/nest.avsc",
              char *fschema = "../res/schema/ol.txt") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum tmpGD = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(tmpGD.value<GenericRecord>());
    rs.push_back(tmpGD.value<GenericRecord>());
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
            tmpGD = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = tmpGD.value<GenericRecord>();
            rs.push_back(tmpGD.value<GenericRecord>());
            bs.push_back(bs.back() + j + 1);
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    vector<fileReader> frs({fileReader(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile),
                            fileReader(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile)});

    SchemaReader fr(fschema, false);
    FetchTable ft;
    fr.read(&ft);
    for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
        cout << it->first << endl;
    }
    int rcount = headreader->getColumn(bs[1]).getRowcount();
    vector<Bitset> bsts({Bitset(headreader->getColumn(bs[0]).getRowcount(), -1), Bitset(rcount, 0)});
//        cout<<headreader->getColumn(bs[0]).getRowcount()<<" "<<rcount<<endl;
    bool flag = true;
    for (FetchTable::reverse_iterator it = ft.rbegin(); it != ft.rend(); it++) {
        int ind = rs[0].fieldIndex(it->first);
        int i = 0;
        if (ind == -1) i = 1;
        if (i == 1 && flag) {
            ind = rs[1].fieldIndex(it->first);
            flag = false;
            frs[0].fresh();
            int idx[] = {0, 0};
            while (frs[0].next()) {
                int l = frs[0].getArrsize();
                if (bsts[0].get(idx[1])) {
                    bsts[1].setRange(idx[0], idx[0] + l);
                }
                idx[0] += l;
                idx[1]++;
            }
        }
        int off = bsts[i].get(0) ? 0 : bsts[i].nextSetBit(0);
        auto curtype = rs[i].schema()->leafAt(ind)->type();
        while (off != -1) {
//                cout<<off<<endl;
            switch (curtype) {
                case CORES_FLOAT: {
                    if (!it->second.fn->filter((double) frs[i].skipColRead<float>(off, ind)))
                        bsts[i].clear(off);
                    break;
                }
                case CORES_DOUBLE: {
                    if (!it->second.fn->filter((double) frs[i].skipColRead<double>(off, ind)))
                        bsts[i].clear(off);
                    break;
                }
                case CORES_INT: {
                    if (!it->second.fn->filter((int64_t) frs[i].skipColRead<int>(off, ind)))
                        bsts[i].clear(off);
                    break;
                }
                case CORES_LONG: {
                    if (!it->second.fn->filter((int64_t) frs[i].skipColRead<int64_t>(off, ind)))
                        bsts[i].clear(off);
                    break;
                }
                case CORES_STRING: {
                    string tmp = frs[i].skipColRead<const char *>(off, ind);
                    if (!it->second.fn->filter(tmp))
                        bsts[i].clear(off);
                    break;
                }
            }
            off = bsts[i].nextSetBit(off);
        }
    }

    frs[0].fresh();
    frs[1].fresh();
    int res = 0;

    int off = bsts[1].get(0) ? 0 : bsts[1].nextSetBit(0);
    while (off != -1) {
        res++;
//            frs[0].skipread(off);
        off = bsts[1].nextSetBit(off);
//        fr0.printRecord();
//        cout<<"\n";
    }
    cout << res << endl;

    return res;
}

int queryTest2(char *datafile = "./tmpresult/fileout.dat",
               char *schema = "../res/test/requiredtest/nest.avsc",
               char *fschema = "../res/schema/ol.txt") {
    SchemaReader sr(schema, true);
    ValidSchema *vschema = sr.read();
    GenericDatum tmpGD = GenericDatum(vschema->root());
    vector<GenericRecord> rs;
    vector<int> bs;
    GenericRecord r(tmpGD.value<GenericRecord>());
    rs.push_back(tmpGD.value<GenericRecord>());
    bs.push_back(0);
    for (int j = 0; j < r.fieldCount(); ++j) {
        if (r.schema()->leafAt(j)->type() == CORES_ARRAY) {
            tmpGD = GenericDatum(r.fieldAt(j).value<GenericArray>().schema()->leafAt(0));
            r = tmpGD.value<GenericRecord>();
            rs.push_back(tmpGD.value<GenericRecord>());
            bs.push_back(bs.back() + j + 1);
            j = 0;
        }
    }
    bs.push_back(bs.back() + r.fieldCount());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    shared_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    vector<fileReader> frs({fileReader(GenericDatum(rs[0]), headreader, bs[0], bs[1] - 1, datafile),
                            fileReader(GenericDatum(rs[1]), headreader, bs[1], bs[2] - 1, datafile)});

    SchemaReader fr(fschema, false);
    FetchTable ft;
    fr.read(&ft);
    for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
        cout << it->first << endl;
    }
    int rcount = headreader->getColumn(bs[1]).getRowcount();
    int idx = 0;
    while (frs[0].next()) {
//        fr0.printRecord();
//        cout << "\n";
        bool flag = true;
        for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
            int ind = rs[0].fieldIndex(it->first);
            if (ind == -1) continue;
            if (!flag) break;
            switch (rs[0].schema()->leafAt(ind)->type()) {
                case CORES_FLOAT:
                    if (it->second.fn->filter((double) frs[0].getRecord().fieldAt(ind).value<float>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_DOUBLE:
                    if (it->second.fn->filter((double) frs[0].getRecord().fieldAt(ind).value<double>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_INT:
                    if (it->second.fn->filter((int64_t) frs[0].getRecord().fieldAt(ind).value<int>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_LONG:
                    if (it->second.fn->filter((int64_t) frs[0].getRecord().fieldAt(ind).value<int64_t>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };
                case CORES_STRING:
                    if (it->second.fn->filter((string) frs[0].getRecord().fieldAt(ind).value<const char *>())) {
                        continue;
                    } else {
                        flag = false;
                        break;
                    };

            }
        }
        int arrsize = frs[0].getArrsize();
        for (int i = 0; i < arrsize; i++) {
            frs[1].next();
            if (!flag) continue;
            bool flag = true;
            for (FetchTable::const_iterator it = ft.begin(); it != ft.end(); ++it) {
                int ind = rs[1].fieldIndex(it->first);
                if (ind == -1) continue;
                if (!flag) break;
                switch (rs[1].schema()->leafAt(ind)->type()) {
                    case CORES_FLOAT:
                        if (it->second.fn->filter((double) frs[1].getRecord().fieldAt(ind).value<float>())) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        };
                    case CORES_DOUBLE:
                        if (it->second.fn->filter((double) frs[1].getRecord().fieldAt(ind).value<double>())) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        };
                    case CORES_INT:
                        if (it->second.fn->filter((int64_t) frs[1].getRecord().fieldAt(ind).value<int>())) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        };
                    case CORES_LONG:
                        if (it->second.fn->filter((int64_t) frs[1].getRecord().fieldAt(ind).value<int64_t>())) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        };
                    case CORES_STRING:
                        if (it->second.fn->filter((string) frs[1].getRecord().fieldAt(ind).value<const char *>())) {
                            continue;
                        } else {
                            flag = false;
                            break;
                        };

                }
            }
            if (flag) {
                idx++;
//            fr0.printRecord();
//            cout<<"\n";
            }
        }
    }
    cout << idx << endl;
    return idx;
}

void SETUP(char **argv) {
    system("mkdir layer0");
    system("mkdir layer1");
    system("mkdir tmpresult");

    flatWriter(argv[1]);
}

TEST(FilterTest, DummyTest) {
    system("echo 3 > /proc/sys/vm/drop_caches");
    Tracer tc;
    tc.startTime();
    int ans = optionReader(true);
//    queryTest();
    cout << tc.getRunTime() << endl;
    system("echo 3 > /proc/sys/vm/drop_caches");
    tc.getRunTime();
    EXPECT_EQ(ans, bitsetReader());
//    queryTest2();
    cout << tc.getRunTime();
}

void TEARDOWN() {
    system("rd /s/q layer0");
    system("rd /s/q layer1");
    system("rd /s/q tmpresult");
}

int main(int argc, char **argv) {
    if (argc == 2)
        SETUP(argv);
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    TEARDOWN();
    return ret;

//    queryTest();
//    queryTest2();
    return 0;
}
