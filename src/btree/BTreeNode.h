#pragma once

#include "BTreeEntry.h"
#include "misc/global.h"
#include "misc/querytype.h"

class BTreeONode : public Node {
public:
    BTreeLEntry *entries;
    bigint next;

    BTreeONode(Tree *_tree);

    BTreeONode(Tree *_tree, bigint block);

    ~BTreeONode();

    void insert(BTreeLEntry *e);

private:
    void read_from_buffer(char *buffer);

    void write_to_buffer(char *buffer);
};

class BTreeLNode : public Node {
public:
    BTreeLEntry *entries;
    bigint next;
    bigint over;
    int num_overflow;

    BTreeLNode(Tree *_tree);

    BTreeLNode(Tree *_tree, bigint block);

    ~BTreeLNode();

    bool insert(BTreeLEntry *e, BTreeLNode **sn);

    RET_TYPE _delete(BTreeLEntry *e);

    int search(KEY_TYPE lowkey, KEY_TYPE highkey, BTreeLEntry *rs);

private:
    void read_from_buffer(char *buffer);

    void write_to_buffer(char *buffer);

    void split(BTreeLEntry *e, BTreeLNode **sn);
};

class BTreeINode : public Node {
public:
    int level;
    KEY_TYPE *keys;
    bigint *children;

    BTreeINode(Tree *_tree);

    BTreeINode(Tree *_tree, bigint block);

    virtual ~BTreeINode();

    bool insert(BTreeLEntry *e, BTreeINode **newNode, KEY_TYPE *middlekey);    //insert into new node, will not split
    RET_TYPE _delete(BTreeLEntry *e);

    bigint search(KEY_TYPE key);

    void traverse(FILE *fp, int &datacount, int &leafcount, int &inodecount, int &onodecount);

    void traverseLeaf(FILE *fp, int &datacount, int &leafcount, int &onodecount);

protected:
    void read_from_buffer(char *buffer);

    void write_to_buffer(char *buffer);

    int selectChild(KEY_TYPE key);

    bool insert(bigint lchild, KEY_TYPE key, bigint rchild, BTreeINode **sn, KEY_TYPE *splitKey);

    bool redist(BTreeLNode *lchild, int pos, BTreeLNode *rchild);

    bool redist(BTreeINode *lchild, int pos, BTreeINode *rchild);

    bool merge(BTreeLNode *lchild, int pos, BTreeLNode *rchild);

    bool merge(BTreeINode *lchild, int pos, BTreeINode *rchild);

    bool split(bigint lchild, KEY_TYPE key, bigint rchild, BTreeINode **sn, KEY_TYPE *splitKey);
};