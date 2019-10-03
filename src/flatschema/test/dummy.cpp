//
// Created by iclab on 9/27/19.
//

#include <algorithm>
#include <iostream>
#include "bitweaving/table.h"
#include "bitweaving/options.h"
#include "utils.h"

using namespace std;

using namespace bitweaving;

long total_count = (1 << 20);

int isRead = 0;

char *tablepath = "./test.tb";

void tableGenerate() {
    Options options = Options();
    options.delete_exist_files = true;
    Table *table = new Table(string(tablepath), options);
    table->Resize(total_count + 1024);
    Status status = table->Open();
    status = table->AddColumn("l_orderkey", kNaive, 32);
    Code *firstcol = new Code[total_count];
    for (long l = 0; l < total_count; l++) {
        firstcol[l] = l;
    }
    Column *fc = table->GetColumn("l_orderkey");
    unsigned long num_tuples = total_count;
    unsigned long offset = 0;
    /*while (num_tuples - offset > 0) {
        size_t num = std::min(num_tuples - offset, kNumCodesPerBlock / 4 + 1000);
        status = fc->Bulkload(firstcol + offset, num, offset);
        offset += num;
    }*/
    fc->Bulkload(firstcol, total_count);
    //status = table->AddColumn("l_linenumber", kNaive, 64);
    table->Save();
    table->Close();
    delete table;
}

void tableRead() {

}

int main(int argc, char **argv) {
    cout << argv << endl;
    if (argc > 3) {
        total_count = std::atol(argv[1]);
    }
    cout << "Init" << endl;
    Tracer tracer;
    tracer.startTime();
    if (isRead == 1) {

    } else {
        tableGenerate();
    }
    cout << "Table operation: " << tracer.getRunTime() << endl;
    return 0;
}