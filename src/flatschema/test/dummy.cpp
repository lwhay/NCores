//
// Created by iclab on 9/27/19.
//

#include <iostream>
#include "bitweaving/table.h"
#include "bitweaving/options.h"

using namespace std;

using namespace bitweaving;

void tableTest() {
    Options options = Options();
    options.in_memory = true;
    Table *table = new Table(string(), options);
    delete table;
}

int main(int argc, char **argv) {
    cout << "Init" << endl;
    tableTest();
    return 0;
}