//
// Created by Michael on 2018/12/14.
//

#pragma once

#define FIX16_SLICE_LEN 2

#define FIX32_SLICE_LEN 4

#define FIX64_SLICE_LEN 8

struct slice {
    char *data;
    int len;
};

struct fix16slice {
    char *data[FIX16_SLICE_LEN];
};

struct fix32slice {
    char data[FIX32_SLICE_LEN];
};

struct fix64slice {
    char data[FIX64_SLICE_LEN];
};