#include <iostream>
#include <fstream>
#include <stdio.h>
#include <EntityDescriptor.h>

using namespace std;

#define CACHE_SIZE 1024

int main(int argc, char **args) {
    std::cout << argc << " " << args[1] << std::endl;
    FILE *fp = fopen(args[1], "rb");
    char *str = new char[CACHE_SIZE];

    size_t size = fread(str, 1, CACHE_SIZE, fp);
    parser::EntityDescriptor ed(str);

    std::cout << ed.Export() << " " << size << std::endl;

    ed.Transform();

    fclose(fp);
    return 0;
}
