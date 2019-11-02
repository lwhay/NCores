#pragma once

#include <unordered_map>
#include <string>
#include <vector>

using namespace std;

enum OrderType {
    ASCENDING = 0,
    DESCENDING,
    IGNORED
};

enum ValueType {
    EMPTY = 0,
    BOOL,
    INT,
    UINT,
    LONG,
    ULONG,
    FIXED32,
    FIXED64,
    FIXED128,
    FIXED256,
    FIXED512,
    FLOAT,
    DOUBLE,
    REAL,
    STRING,
    TEXT,
    BYTES,
    RECORD,
    ENUM,
    MAP,
    UNION,
    ARRAY,
    KEYGROUP,
    BLOB
};

enum FieldType {
    name_key = 0,
    type_key = 1,
    values_key = 2,
    parent_key = 4,
    array_key = 8,
    layer_key = 16,
    union_key = 32,
    codec_key = 36,
    union_array = 48
};

enum ClauseType {
    filter = 0,
    fetch = 1,
    aggregate = 2,
    group = 3
};

enum CompositeType {
    conjunctive,
    disjunctive,
    negative
};

enum OperatorType {
    eq = 0,
    ne = 1,
    lt = 2,
    le = 3,
    gt = 4,
    ge = 5,
    contains = 10,
    notcontains = 11,
    startwith = 12,
    endwith = 13
};

enum CodecType {
    null = 0,
    snappy,
    bzip,
    lz4,
    gz,
    deflate
};

typedef unsigned int FieldIndex;
