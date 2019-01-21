//
// Created by Michael on 2018/11/30.
//

#include <iostream>

using namespace std;

void test() {

}

typedef int tint;

int main(int argc, char **argv) {
    long long llint = 2147483648;
    cout << (2147467264 < llint && llint >= 0) << endl;
    long long smaller = 2147467264, larger =2147483648;
    if (smaller <= larger && smaller >= 0) {
        cout << "right" << endl;
    } else {
        cout << "error" << endl;
    }
    cout << "tint: " << sizeof(tint) << endl;
    cout << "&func: " << sizeof(&test) << endl;
    void (*ptest)() = &test;
    cout << "short: " << sizeof(ptest) << endl;
    cout << "char: " << sizeof(char) << endl;
    char c = 'c';
    cout << "char: " << sizeof(c) << endl;
    cout << "short: " << sizeof(short) << endl;
    short vshort = -1;
    cout << "short: " << sizeof(vshort) << "\t" << vshort << endl;
    cout << "ushort: " << sizeof(unsigned short) << endl;
    unsigned short uvshort = -1;
    cout << "ushort: " << sizeof(uvshort) << "\t" << uvshort << endl;
    cout << "int: " << sizeof(int) << endl;
    int vint = -1;
    cout << "vint: " << sizeof(vint) << "\t" << vint << endl;
    cout << "uint: " << sizeof(unsigned int) << endl;
    unsigned int uvint = -1;
    cout << "uvint: " << sizeof(uvint) << "\t" << uvint << endl;
    cout << "long: " << sizeof(long) << endl;
    long vlong = -1;
    cout << "vlong: " << sizeof(vlong) << "\t" << vlong << endl;
    cout << "ulong: " << sizeof(unsigned long) << endl;
    unsigned long uvlong = -1;
    cout << "uvlong: " << sizeof(uvlong) << "\t" << uvlong << endl;
    cout << "llong: " << sizeof(long long) << endl;
    long long vllong = -1;
    cout << "vllong: " << sizeof(vllong) << "\t" << vllong << endl;
    cout << "ullong: " << sizeof(unsigned long long) << endl;
    unsigned long long uvllong = -1;
    cout << "uvllong: " << sizeof(uvllong) << "\t" << uvllong << endl;
    cout << "float: " << sizeof(float) << endl;
    float vfloat = -1.0;
    cout << "vfloat: " << sizeof(vfloat) << "\t" << vfloat << endl;
    cout << "double: " << sizeof(double) << endl;
    double vdouble = -1.0;
    cout << "vdouble: " << sizeof(vdouble) << "\t" << vdouble << endl;
}
