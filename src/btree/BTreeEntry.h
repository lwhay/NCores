#pragma once

#include "misc/global.h"

class BTreeLEntry {
public:
    KEY_TYPE key;
    int rid;
    RECORD_TYPE record;

    int get_size();

    void read_from_buffer(char *buffer);

    void write_to_buffer(char *buffer);

    virtual BTreeLEntry &operator=(BTreeLEntry &_d);
};