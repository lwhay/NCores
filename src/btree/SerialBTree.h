#pragma once

#include "BTreeEntry.h"
#include "misc/global.h"
#include "cache/LSMCache.h"
#include "misc/querytype.h"

class SerialBTree : public Tree {
public:
    SerialBTree(char *_fname, Cache *_c);

    /*BTree(char *_fname, Cache *_c, bool existing);*/

    virtual ~SerialBTree();

    void insert(int oid, KEY_TYPE key, RECORD_TYPE *rd);

    bool _delete(int oid, KEY_TYPE key);

    int search(KEY_TYPE lowkey, KEY_TYPE highkey, BTreeLEntry *rs);

    virtual void clear();

    void traverse();

protected:
    virtual void insert(BTreeLEntry *e);

    virtual bool _delete(BTreeLEntry *e);

    void read_header();

    void write_header();
};
