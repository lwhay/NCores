//
// Created by Michael on 2018/11/26.
//
#pragma once

#include "Types.h"

template<FieldType, CodecType>
class MetaData {
    unordered_map<FieldIndex, FieldType> namemap;
    const vector<string> codecTypes = {"null", "snappy", "bzip", "lz4", "gz", "deflate"};
    const vector<string> valueTypes = {"EMPTY", "BOOL", "INT", "UINT", "LONG", "ULONG", "FIXED32", "FIXED64",
                                       "FIXED128", "FIXED256", "FIXED512", "FLOAT", "DOUBLE", "REAL", "STRING", "TEXT",
                                       "BYTES", "RECORD", "ENUM", "MAP", "UNION", "ARRAY", "KEYGROUP", "BLOB"};
    unordered_map<string, CodecType> typesIndex;
    unordered_map<string, ValueType> valuesIndex;

    CodecType defaultCodecType;

public:
    MetaData() {
        for (int type = null; type <= deflate; type++) {
            CodecType ct = static_cast<CodecType>(type);
            typesIndex.insert(make_pair(codecTypes[type], ct));
        }
        for (int type = EMPTY; type <= BLOB; type++) {
            ValueType vt = static_cast<ValueType>(type);
            valuesIndex.insert(make_pair(valueTypes[type], vt));
        }
    }

    void setCodc(string codecname) {
        defaultCodecType = typesIndex.find(codecname)->second;
    }

    CodecType getCodec() {
        return defaultCodecType;
    }

    string getCodecName() {
        return codecTypes[defaultCodecType];
    }
};

class ColumnMetaData {
    string fieldname;
    FieldType fieldType;
    //MetaData<values_key, null>* metadata;
    bool isValues;
    bool isArray;

    unsigned int unionCount;
    unsigned int unionBits;

    ColumnMetaData *parent;
public:
    ColumnMetaData();
};
