#include <string.h>
#include <stdlib.h>
#include "SerialBTree.h"
#include "BTreeNode.h"

SerialBTree::SerialBTree(char *_fname, Cache *_c) {
    printf("Recreate: %s\t%lld\n", _fname, root);
    num_of_data = 0;
    num_of_dnode = 0;
    num_of_inode = 0;
    num_of_onode = 0;

    strcpy(fname, _fname);
    fptr = fopen(fname, "wb+");
    if (fptr == NULL) {
        printf("------------------------B+-tree Error--------------------------------\n");
        printf("Error opening index file.\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }

    cache = _c;

    root_lvl = 0;
    BTreeLNode *root_ptr = new BTreeLNode(this);
    printf("capacity: %d\n", root_ptr->capacity);
    root = root_ptr->block;
    delete root_ptr;

    num_of_dnode++;
}

/*BTree::BTree(char *_fname, Cache *_c, bool existing) {
    if (!existing) {
        BTree(_fname, _c);
    } else {
        printf("Reopen: %s", _fname);
        strcpy(fname, _fname);
        fptr = fopen(fname, "rb+");
        if (fptr == NULL) {
            printf("------------------------B+-tree Error--------------------------------\n");
            printf("Error opening existing index file.\n");
            printf("---------------------------------------------------------------------\n");
            exit(-1);
        }

        cache = _c;
        FILE *conffile = fopen("index.conf", "rb+");
        fscanf(conffile, "%lld\n", root);
        fscanf(conffile, "%d\n", root_lvl);
        fscanf(conffile, "%d\n", num_of_data);
        fscanf(conffile, "%d\n", num_of_dnode);
        fscanf(conffile, "%d\n", num_of_inode);
        fscanf(conffile, "%d\n", num_of_onode);
        fclose(conffile);
        BTreeLNode *root_ptr = new BTreeLNode(this, root);
        delete root_ptr;
    }
}*/

SerialBTree::~SerialBTree() {
    if (cache)
        cache->flush();

    FILE *conffile = fopen("index.conf", "wb+");

    fprintf(conffile, "%lld\n", this->root);
    fprintf(conffile, "%d\n", this->root_lvl);
    fprintf(conffile, "%d\n", this->num_of_data);
    fprintf(conffile, "%d\n", this->num_of_dnode);
    fprintf(conffile, "%d\n", this->num_of_inode);
    fprintf(conffile, "%d\n", this->num_of_onode);

    fclose(conffile);

    if (fclose(fptr)) {
        printf("------------------------B+-tree Error--------------------------------\n");
        printf("Error closing index file.\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
}

int SerialBTree::search(KEY_TYPE lowkey, KEY_TYPE highkey, BTreeLEntry *rs) {
    int rsno = 0;
    if (root_lvl == 0) {
        BTreeLNode *lnode = new BTreeLNode(this, root);    //read node root node
        rsno = lnode->search(lowkey, highkey, rs);
        delete lnode;
    } else {
        BTreeINode *root_ptr = new BTreeINode(this, root);    //read node root node

        bigint leaf = root_ptr->search(lowkey);
        delete root_ptr;

        BTreeLNode *lnode = new BTreeLNode(this, leaf);
        rsno = lnode->search(lowkey, highkey, rs);
        delete lnode;
    }
    return rsno;
}

void SerialBTree::insert(int _rid, KEY_TYPE _key, RECORD_TYPE *_rd) {
    BTreeLEntry *le = new BTreeLEntry();
    le->rid = _rid;
    le->key = _key;
    le->record = *_rd;
    insert(le);
}

void SerialBTree::insert(BTreeLEntry *e) {
    bool SPLIT = false;

    bigint lchild, rchild;
    KEY_TYPE middlekey;  //the key value to be moved up when splitting occurs

    num_of_data++;

    if (root_lvl == 0)    //root is data
    {
        BTreeLNode *lnode, *newLnode;
        lnode = new BTreeLNode(this, root);    //read node root node
        SPLIT = lnode->insert(e, &newLnode);

        if (!SPLIT) {
            lnode->dirty = true;
            delete lnode;
        } else    //node split
        {
            middlekey = newLnode->entries[0].key;
            rchild = newLnode->block;
            delete newLnode;

            lchild = lnode->block;
            delete lnode;

            // spliting occurs at root, lock on root node is unreleased
            BTreeINode *newroot = new BTreeINode(this);
            root_lvl++;
            newroot->level = root_lvl;
            root = newroot->block;
            newroot->num_entries++;
            newroot->keys[0] = middlekey;
            newroot->children[0] = lchild;
            newroot->children[1] = rchild;
            newroot->dirty = true;
            delete newroot;

            num_of_inode++;
        }
    } else {
        BTreeINode *root_ptr, *newNode;

        root_ptr = new BTreeINode(this, root);    //read node root node

        SPLIT = root_ptr->insert(e, &newNode, &middlekey);

        if (!SPLIT)
            delete root_ptr;
        else {    //split at root
            lchild = root;
            delete root_ptr;
            rchild = newNode->block;
            delete newNode;

            BTreeINode *newroot = new BTreeINode(this);
            root_lvl++;
            newroot->level = root_lvl;
            root = newroot->block;
            newroot->num_entries++;
            newroot->keys[0] = middlekey;
            newroot->children[0] = lchild;
            newroot->children[1] = rchild;
            newroot->dirty = true;
            delete newroot;
            num_of_inode++;
        }
    }
}


bool SerialBTree::_delete(int _rid, KEY_TYPE _key) {
    BTreeLEntry *le = new BTreeLEntry();
    le->rid = _rid;
    le->key = _key;
    if (_delete(le)) {
        delete le;
        return true;
    }
    delete le;
    return false;
}

bool SerialBTree::_delete(BTreeLEntry *e) {
    RET_TYPE ret = NOTFOUND;

    if (root_lvl == 0)    //root is data
    {
        BTreeLNode *lnode;
        lnode = new BTreeLNode(this, root);    //read node root node
        ret = lnode->_delete(e);

        if (ret == NOTFOUND) {
            delete lnode;
            return false;
        } else {    //this is the only node, even underflow leave it
            num_of_data--;
            delete lnode;
            return true;
        }
    } else {
        BTreeINode *root_ptr;

        root_ptr = new BTreeINode(this, root);    //read node root node

        ret = root_ptr->_delete(e);

        if (ret == NOTFOUND) {
            delete root_ptr;
            return false;
        } else if (ret == NORMAL) {
            num_of_data--;
            delete root_ptr;
            return true;
        } else {
            num_of_data--;
            if (root_ptr->num_entries == 0) {
                root = root_ptr->children[0];
                root_lvl--;
                num_of_inode--;

                root_ptr->dirty = false;
                delete root_ptr;
            } else
                delete root_ptr;

            return true;
        }
    }
}

void SerialBTree::traverse() {
    FILE *fp = fopen("../test.log", "w");
    int datacount = 0;
    int lnodecount = 0;
    int inodecount = 0;
    int onodecount = 0;

    BTreeINode *root_ptr = new BTreeINode(this, root);
    inodecount++;
    root_ptr->traverse(fp, datacount, lnodecount, inodecount, onodecount);
    //root_ptr->traverseLeaf(fp, datacount, lnodecount, onodecount);
    delete root_ptr;
    fclose(fp);

    printf("Traverse Tree:\nNumber of Data: %d\nNumber of Leaf Node: %d\nNumber of Intermedia Node: %d\nNumber of Overflow Node: %d\n",
           datacount, lnodecount, inodecount, onodecount);
}

void SerialBTree::clear() {
    num_of_data = 0;
    num_of_dnode = 0;
    num_of_inode = 0;
    num_of_onode = 0;

    cache->clear(fptr);
    if (fptr)
        fclose(fptr);
    fptr = fopen(fname, "wb+");
    if (fptr == NULL) {
        printf("------------------------B+-tree Error--------------------------------\n");
        printf("Error opening index file (in clear phase).\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }

    root_lvl = 0;
    BTreeLNode *root_ptr = new BTreeLNode(this);
    root = root_ptr->block;
    delete root_ptr;

    num_of_dnode++;
}