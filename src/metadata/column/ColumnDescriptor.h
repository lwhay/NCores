#pragma once

#include "MetaData.h"

class ColumnDescriptor {
    ColumnMetaData metaData;

public:
    ColumnDescriptor(ColumnMetaData columnMetaData) : metaData(columnMetaData) {
    }
};
