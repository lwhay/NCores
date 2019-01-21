#include "BTreeNode.h"
#include <string.h>
#include <math.h>

BTreeONode::BTreeONode(Tree *_tree) {
    tree = _tree;
    num_entries = 0;
    next = -1;
    dirty = true;

    BTreeLEntry *d = new BTreeLEntry();
    int header_size = 2 * sizeof(int);  // level + num_entries
    capacity = (tree->cache->blocklength - header_size) / d->get_size();
    delete d;

    entries = new BTreeLEntry[capacity];

    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    block = tree->cache->write_new_block(tree->fptr, buffer);
    delete[] buffer;
}

BTreeONode::BTreeONode(Tree *_tree, bigint _block) {
    tree = _tree;
    dirty = false;
    block = _block;

    BTreeLEntry *d = new BTreeLEntry();
    int header_size = 2 * sizeof(int);  // level + num_entries
    capacity = (tree->cache->blocklength - header_size) / d->get_size();
    delete d;

    entries = new BTreeLEntry[capacity];

    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    tree->cache->read_block(block, tree->fptr, buffer);
    read_from_buffer(buffer);
    delete[] buffer;
}

BTreeONode::~BTreeONode() {
    if (dirty) {
        if (num_entries <= 0)
            num_entries += 0;
        char *buffer = new char[tree->cache->blocklength];
        write_to_buffer(buffer);
        tree->cache->write_block(block, tree->fptr, buffer);
        delete[] buffer;
    }
    delete[] entries;
}

void BTreeONode::read_from_buffer(char *buffer) {
    int i, j, s;

    // num_entries
    memcpy(&num_entries, buffer, sizeof(int));
    j = sizeof(int);

    // next overflow node
    memcpy(&next, &(buffer[j]), sizeof(bigint));
    j += sizeof(bigint);

    s = entries[0].get_size();
    for (i = 0; i < num_entries; i++) {
        entries[i].read_from_buffer(&buffer[j]);
        j += s;
    }
}

void BTreeONode::write_to_buffer(char *buffer) {
    int i, j, s;

    // num_entries
    memcpy(buffer, &num_entries, sizeof(int));
    j = sizeof(int);

    // next
    memcpy(&buffer[j], &next, sizeof(bigint));
    j += sizeof(bigint);

    s = entries[0].get_size();
    for (i = 0; i < num_entries; i++) {
        entries[i].write_to_buffer(&buffer[j]);
        j += s;
    }
}

void BTreeONode::insert(BTreeLEntry *e) {
    if (num_entries < capacity) {
        entries[num_entries] = *e;
        num_entries++;
        delete e;
        dirty = true;
    } else {
        BTreeONode *onode;
        if (next > 0)
            onode = new BTreeONode(tree, next);
        else {
            onode = new BTreeONode(tree);
            next = onode->block;
            tree->num_of_onode++;
            dirty = true;
        }
        onode->insert(e);
        delete onode;
    }
}

BTreeLNode::BTreeLNode(Tree *_tree) {
    tree = _tree;
    num_entries = 0;
    next = -1;
    over = -1;
    num_overflow = 0;
    dirty = true;

    BTreeLEntry *d = new BTreeLEntry();
    int header_size = 2 * sizeof(int) + 2 * sizeof(bigint);  // num_entries + next + over + num_overflow
    capacity = (tree->cache->blocklength - header_size) / d->get_size();
    delete d;

    entries = new BTreeLEntry[capacity];

    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    block = tree->cache->write_new_block(tree->fptr, buffer);
    delete[] buffer;
}

BTreeLNode::BTreeLNode(Tree *_tree, bigint _block) {
    /*printf("b: %lld\n", _block);*/
    tree = _tree;
    dirty = false;
    block = _block;

    BTreeLEntry *d = new BTreeLEntry();
    int header_size = 2 * sizeof(int) + 2 * sizeof(bigint);  // num_entries + next + over + num_overflow
    capacity = (tree->cache->blocklength - header_size) / d->get_size();
    delete d;

    entries = new BTreeLEntry[capacity];
    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    tree->cache->read_block(block, tree->fptr, buffer);
    read_from_buffer(buffer);
    delete[] buffer;
}

BTreeLNode::~BTreeLNode() {
    if (dirty) {
        char *buffer = new char[tree->cache->blocklength];
        write_to_buffer(buffer);
        tree->cache->write_block(block, tree->fptr, buffer);
        delete[] buffer;
    }
    delete[] entries;
}

void BTreeLNode::read_from_buffer(char *buffer) {
    int i, j, s;

    // num_entries
    memcpy(&num_entries, buffer, sizeof(int));
    j = sizeof(int);

    // next
    memcpy(&next, &(buffer[j]), sizeof(bigint));
    j += sizeof(bigint);

    //over
    memcpy(&over, &(buffer[j]), sizeof(bigint));
    j += sizeof(bigint);

    //num_overflow
    memcpy(&num_overflow, &(buffer[j]), sizeof(int));
    j += sizeof(int);

    s = entries[0].get_size();
    for (i = 0; i < num_entries; i++) {
        entries[i].read_from_buffer(&buffer[j]);
        j += s;
    }
}

void BTreeLNode::write_to_buffer(char *buffer) {
    int i, j, s;

    // num_entries
    memcpy(buffer, &num_entries, sizeof(int));
    j = sizeof(int);

    // next
    memcpy(&buffer[j], &next, sizeof(bigint));
    j += sizeof(bigint);

    // over
    memcpy(&buffer[j], &over, sizeof(bigint));
    j += sizeof(bigint);

    // num_overflow
    memcpy(&buffer[j], &num_overflow, sizeof(int));
    j += sizeof(int);

    s = entries[0].get_size();
    for (i = 0; i < num_entries; i++) {
        entries[i].write_to_buffer(&buffer[j]);
        j += s;
    }
}

int BTreeLNode::search(KEY_TYPE lowkey, KEY_TYPE highkey, BTreeLEntry *rs) {
    int rsno = 0;
    int i = 0;
    while (i < num_entries && entries[i].key < lowkey)
        i++;

    while (i < num_entries && entries[i].key <= highkey)
        rs[rsno++] = entries[i++];

    if (i == num_entries && over != -1) {
        bigint overblock = over;
        while (overblock >= 0) {
            BTreeONode *onode = new BTreeONode(tree, overblock);
            for (i = 0; i < onode->num_entries; i++)
                rs[rsno++] = onode->entries[i];
            overblock = onode->next;
            delete onode;
        }
    }

    if (next >= 0 && entries[num_entries - 1].key < highkey) {
        bigint nextblock = next;
        while (nextblock >= 0) {
            BTreeLNode *nextnode = new BTreeLNode(tree, nextblock);
            i = 0;
            while (i < nextnode->num_entries && nextnode->entries[i].key <= highkey)
                rs[rsno++] = nextnode->entries[i++];

            if (i == nextnode->num_entries && nextnode->over != -1) {
                bigint overblock = nextnode->over;
                while (overblock >= 0) {
                    BTreeONode *onode = new BTreeONode(tree, overblock);
                    for (i = 0; i < onode->num_entries; i++)
                        rs[rsno++] = onode->entries[i];
                    overblock = onode->next;
                    delete onode;
                }
            }

            if (nextnode->entries[nextnode->num_entries - 1].key >= highkey) {
                delete nextnode;
                break;
            }

            nextblock = nextnode->next;
            delete nextnode;
        }
    }

    return rsno;
}

bool BTreeLNode::insert(BTreeLEntry *e, BTreeLNode **sn) {
    bool found = false;

    if (num_entries == capacity && e->key <= entries[num_entries - 1].key) {
        if (e->key == entries[num_entries - 1].key) {
            //insert into overflow node
            BTreeONode *onode;
            if (over > 0)
                onode = new BTreeONode(tree, over);
            else {
                onode = new BTreeONode(tree);
                over = onode->block;
                tree->num_of_onode++;
            }
            onode->insert(e);
            num_overflow++;
            delete onode;
            dirty = true;
            return false;
        } else {
            //insert into leaf node and move overflow node
            if (entries[num_entries - 1].key == entries[num_entries - 2].key) {
                //move last entry into overflow node
                BTreeONode *onode;
                if (over > 0)
                    onode = new BTreeONode(tree, over);
                else {
                    onode = new BTreeONode(tree);
                    over = onode->block;
                    tree->num_of_onode++;
                }
                BTreeLEntry *le = new BTreeLEntry();
                *le = entries[num_entries - 1];
                onode->insert(le);
                num_overflow++;
                num_entries--;
                dirty = true;
                delete onode;
            }
        }
    }

    if (num_entries < capacity) {
        int i = 0;
        while (!found && i < num_entries) {
            if (e->key >= entries[i].key)
                i++;
            else
                found = true;
        }
        int position = i;
        for (i = num_entries; i > position; i--)
            entries[i] = entries[i - 1];
        entries[position] = *e;
        delete e;
        num_entries++;
        dirty = true;
        return false;
    } else {
        split(e, sn);
        tree->num_of_dnode++;
        return true;
    }
}

void BTreeLNode::split(BTreeLEntry *e, BTreeLNode **sn) {
    bool found = false;
    int i = 0, j, position;
    int total, sub_total;
    int splitP = -1;
    int overExist = 0;
    BTreeLEntry *allEntry;
    KEY_TYPE key;

    (*sn) = new BTreeLNode(tree);

    (*sn)->next = next;
    next = (*sn)->block;
    dirty = true;

    if (e->key > entries[num_entries - 1].key) {
        j = 1;
        while (entries[num_entries - 1].key == entries[num_entries - 1 - j].key)
            j++;

        total = num_entries + 1 + num_overflow;
        splitP = total / 2;

        if (1 + j + num_overflow > capacity || splitP > num_entries - j) {
            //new value single node
            (*sn)->entries[0] = *e;
            (*sn)->num_entries++;
            delete e;
            return;
        }

        i = 0;
        while (entries[splitP].key == entries[splitP - 1].key) {
            i++;
            if (splitP + i < num_entries && entries[splitP + i].key != entries[splitP + i - 1].key) {
                splitP = splitP + i;
                break;
            } else if (splitP - i > 0 && entries[splitP - i].key != entries[splitP - i - 1].key) {
                splitP = splitP - i;
                break;
            }
        }

        for (i = splitP; i < num_entries; i++)
            (*sn)->entries[i - splitP] = entries[i];

        num_entries = splitP;
        i = i - splitP;
        if (num_overflow > 0) {
            BTreeONode *onode = new BTreeONode(tree, over);
            for (j = 0; j < onode->num_entries; j++) {
                (*sn)->entries[i] = onode->entries[j];
                i++;
            }
            over = -1;
            num_overflow = 0;
            delete onode;
            tree->num_of_onode--;
        }
        (*sn)->num_entries = i;
        (*sn)->entries[i] = *e;
        (*sn)->num_entries++;
        return;
    }

    while (!found && i < num_entries) {
        if (e->key >= entries[i].key)
            i++;
        else
            found = true;
    }
    position = i;

    if (position == num_entries) {
        key = (entries[num_entries - 1]).key;
        for (i = 1; i < num_entries; i++) {
            if (entries[num_entries - 1 - i].key != key)
                break;
        }
        position += num_overflow;
        if ((i + num_overflow) >= capacity) {
            (*sn)->entries[0] = *e;
            (*sn)->num_entries++;
            delete e;
            return;
        } else if ((i + num_overflow) >= capacity / 2) {
            splitP = num_entries - i;
            for (i = splitP; i < num_entries; i++)
                (*sn)->entries[i - splitP] = entries[i];

            num_entries = splitP;
            i = i - splitP;
            if (num_overflow > 0) {
                BTreeONode *onode = new BTreeONode(tree, over);
                for (j = 0; j < onode->num_entries; j++) {
                    (*sn)->entries[i] = onode->entries[j];
                    i++;
                }
                over = -1;
                num_overflow = 0;
                delete onode;
                tree->num_of_onode--;
            }
            (*sn)->num_entries = i;
            (*sn)->entries[i] = *e;
            (*sn)->num_entries++;
            return;
        } else {}
    } else {}

    total = num_entries + 1 + num_overflow;
    if (total > capacity + 1)
        sub_total = capacity + 1;
    else
        sub_total = total;
    splitP = total / 2;
    if (splitP >= num_entries)
        splitP = num_entries - 1;
    i = 0;
    while (entries[splitP].key == entries[splitP - 1].key) {
        i++;
        if (splitP + i < num_entries && entries[splitP + i].key != entries[splitP + i - 1].key) {
            splitP = splitP + i;
            break;
        } else if (splitP - i > 0 && entries[splitP - i].key != entries[splitP - i - 1].key) {
            splitP = splitP - i;
            break;
        }
    }
    allEntry = new BTreeLEntry[capacity + 1];

    if (total > sub_total)
        overExist = 1;
    for (i = 0; i < sub_total; i++) {
        if (i < position && i < num_entries)
            allEntry[i] = entries[i];
        else if (i == position)
            allEntry[i] = *e;
        else
            allEntry[i] = entries[i - 1];
    }

    /*
    * Original version: Bug 2
    * Error in split position leading to key value's overlap between leaves
    *
    * Modified by: Chen Su
    * Date: Sept. 12, 2006
    */
    if (position <= splitP)
        splitP++;

    (*sn)->num_entries = 0;
    (*sn)->num_overflow = 0;
    for (i = 0; i < sub_total; i++) {
        if (i < splitP)
            entries[i] = allEntry[i];
        else {
            (*sn)->entries[i - splitP] = allEntry[i];
            (*sn)->num_entries++;
        }
    }
    i = i - splitP;
    bigint overblock = over;
    if (overExist == 1) {
        BTreeONode *newOnode = NULL;
        overExist = 0;
        while (overblock >= 0) {
            BTreeONode *onode = new BTreeONode(tree, overblock);
            for (j = 0; j < onode->num_entries; j++) {
                if ((*sn)->num_entries < capacity) {
                    (*sn)->entries[i] = onode->entries[j];
                    i++;
                    (*sn)->num_entries++;
                } else {
                    overExist = 1;
                    if (newOnode == NULL)
                        newOnode = new BTreeONode(tree);
                    BTreeLEntry *le = new BTreeLEntry();
                    *le = onode->entries[j];
                    newOnode->insert(le);
                    (*sn)->num_overflow++;
                }
            }
            overblock = onode->next;
            delete onode;
            tree->num_of_onode--;
        }
        over = -1;
        num_overflow = 0;
        num_entries = splitP;
        if (overExist == 1) {
            newOnode->dirty = true;
            (*sn)->over = newOnode->block;
            tree->num_of_onode++;
            delete newOnode;
        }
        delete[] allEntry;
        return;
    } else {
        num_entries = splitP;
        delete[] allEntry;
        return;
    }
}

RET_TYPE BTreeLNode::_delete(BTreeLEntry *e) {
    int i, j;

    for (i = 0; i < num_entries; i++) {
        //CASE 1: if find in leaf node, delete it
        if (entries[i].key == e->key && entries[i].rid == e->rid) {
            //CASE 1.0: normal delete, no merge occurs
            if (num_entries > capacity / 2) {
                for (j = i; j < num_entries - 1; j++)
                    entries[j] = entries[j + 1];

                if (over > 0)    //fill leaf with entry in overflow node
                {
                    BTreeONode *onode = new BTreeONode(tree, over);
                    entries[j] = onode->entries[onode->num_entries - 1];
                    onode->num_entries--;
                    if (onode->num_entries == 0) {
                        over = onode->next;
                        delete onode;
                        tree->num_of_onode--;
                    } else {
                        onode->dirty = true;
                        delete onode;
                    }
                    num_overflow--;
                } else
                    num_entries--;

                dirty = true;
                return NORMAL;
            }
                //CASE 1.1: merge occurs
            else {
                //if lchild->num_entries<L_FAN_OUT/2, no overflow nodes
                for (j = i; j < num_entries - 1; j++)
                    entries[j] = entries[j + 1];
                num_entries--;
                dirty = true;
                return UNDERFILL;
            }
        }
    }

    //can't find in leaf node
    if (entries[num_entries - 1].key == e->key && over >= 0) {
        bigint overblock = over;
        BTreeONode *preonode = NULL;
        while (overblock >= 0) {
            BTreeONode *onode = new BTreeONode(tree, overblock);
            for (int i = 0; i < onode->num_entries; i++) {
                if (onode->entries[i].rid == e->rid) {
                    if (onode->num_entries == 1) {
                        tree->num_of_onode--;

                        if (preonode != NULL) {
                            preonode->next = onode->next;
                            preonode->dirty = true;
                        } else
                            over = onode->next;
                    } else {
                        onode->entries[i] = onode->entries[onode->num_entries - 1];
                        onode->num_entries--;
                        onode->dirty = true;
                    }
                    if (preonode != NULL)
                        delete preonode;
                    delete onode;
                    num_overflow--;
                    dirty = true;
                    return NORMAL;
                }
            }
            if (preonode != NULL)
                delete preonode;
            preonode = onode;
            overblock = onode->next;
        }
        if (preonode != NULL)
            delete preonode;
        return NOTFOUND;
    }
    return NOTFOUND;
}

BTreeINode::BTreeINode(Tree *_tree) {
    tree = _tree;
    num_entries = 0;
    dirty = true;

    int header_size = 2 * sizeof(int);  // num_entries + level
    capacity = (tree->cache->blocklength - header_size - sizeof(int)) / (sizeof(int) + sizeof(KEY_TYPE));

    children = new bigint[capacity + 1];
    keys = new KEY_TYPE[capacity];

    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    block = tree->cache->write_new_block(tree->fptr, buffer);
    delete[] buffer;
}

BTreeINode::BTreeINode(Tree *_tree, bigint _block) {
    tree = _tree;
    dirty = false;
    block = _block;

    int header_size = 2 * sizeof(int);  // num_entries + level
    capacity = (tree->cache->blocklength - header_size - sizeof(bigint)) / (sizeof(bigint) + sizeof(KEY_TYPE));
    /*printf("Inode capacity: %d\n", this->capacity);*/

    children = new bigint[capacity + 1];
    keys = new KEY_TYPE[capacity];

    //assign a new block on the disk
    char *buffer = new char[tree->cache->blocklength];
    tree->cache->read_block(block, tree->fptr, buffer);
    read_from_buffer(buffer);
    delete[] buffer;
}

BTreeINode::~BTreeINode() {
    if (dirty) {
        char *buffer = new char[tree->cache->blocklength];
        write_to_buffer(buffer);
        tree->cache->write_block(block, tree->fptr, buffer);
        delete[] buffer;
    }
    delete[] children;
    delete[] keys;
}

void BTreeINode::read_from_buffer(char *buffer) {
    int i, j;

    // num_entries
    memcpy(&num_entries, buffer, sizeof(int));
    j = sizeof(int);

    //level
    memcpy(&level, &(buffer[j]), sizeof(int));
    j += sizeof(int);

    //num_overflow
    memcpy(&children[0], &(buffer[j]), sizeof(bigint));
    j += sizeof(bigint);

    for (i = 0; i < num_entries; i++) {
        memcpy(&keys[i], &(buffer[j]), sizeof(KEY_TYPE));
        j += sizeof(KEY_TYPE);
        memcpy(&children[i + 1], &(buffer[j]), sizeof(bigint));
        j += sizeof(bigint);
    }
}

void BTreeINode::write_to_buffer(char *buffer) {
    int i, j;

    // num_entries
    memcpy(buffer, &num_entries, sizeof(int));
    j = sizeof(int);

    //level
    memcpy(&(buffer[j]), &level, sizeof(int));
    j += sizeof(int);

    //num_overflow
    memcpy(&(buffer[j]), &children[0], sizeof(bigint));
    j += sizeof(bigint);

    for (i = 0; i < num_entries; i++) {
        memcpy(&(buffer[j]), &keys[i], sizeof(KEY_TYPE));
        j += sizeof(KEY_TYPE);
        memcpy(&(buffer[j]), &children[i + 1], sizeof(bigint));
        j += sizeof(bigint);
    }
}

int BTreeINode::selectChild(KEY_TYPE key) {
    int i;

    if (num_entries == 0)
        return -1;

    if (key < keys[0])
        return 0;

    for (i = 0; i < num_entries - 1; i++) {
        if ((key >= keys[i] && key < keys[i + 1]) || (key == keys[i] && key <= keys[i + 1])) {
            return i + 1;
        }
    }
    return num_entries;
}

bigint BTreeINode::search(KEY_TYPE key) {
    int childpos = selectChild(key);
    if (level == 1)
        return children[childpos];
    BTreeINode *inode = new BTreeINode(tree, children[childpos]);
    bigint leaf = inode->search(key);
    delete inode;
    return leaf;
}

bool BTreeINode::insert(BTreeLEntry *e, BTreeINode **newNode, KEY_TYPE *middlekey) {
    bigint child, newchild;
    KEY_TYPE splitkey;

    child = children[selectChild(e->key)];
    if (level == 1) {
        BTreeLNode *newlnode;
        BTreeLNode *lnode = new BTreeLNode(tree, child);
        bool SPLIT = lnode->insert(e, &newlnode);
        if (!SPLIT)
            delete lnode;
        else {
            splitkey = newlnode->entries[0].key;
            newchild = newlnode->block;
            delete lnode;
            delete newlnode;
            return insert(child, splitkey, newchild, newNode, middlekey);
        }
    } else {
        BTreeINode *inode = new BTreeINode(tree, child);
        BTreeINode *newInode;
        bool SPLIT = inode->insert(e, &newInode, &splitkey);
        if (!SPLIT)
            delete inode;
        else {
            // split occurs
            newchild = newInode->block;
            delete inode;
            delete newInode;
            return insert(child, splitkey, newchild, newNode, middlekey);
        }
    }
    return false;
}

bool BTreeINode::insert(bigint lchild, KEY_TYPE key, bigint rchild, BTreeINode **sn, KEY_TYPE *splitKey) {
    int position = -1;
    int i;
    int found = false;

    if (num_entries < capacity - 1) {
        // no split
        for (i = 0; i <= num_entries; i++) {
            if ((children[i] == lchild) && !found) {
                position = i;
                found = true;
                break;
            }
        }
        for (i = num_entries; i > position; i--)
            keys[i] = keys[i - 1];
        keys[position] = key;
        for (i = num_entries + 1; i > position + 1; i--)
            children[i] = children[i - 1];
        children[position + 1] = rchild;
        num_entries += 1;
        dirty = true;
        return false;
    } else {
        // split needed
        split(lchild, key, rchild, sn, splitKey);
        tree->num_of_inode++;
        return true;
    }
}

bool BTreeINode::split(bigint lchild, KEY_TYPE key, bigint rchild, BTreeINode **sn, KEY_TYPE *splitKey) {
    int i;
    int totalKey;
    int totalChildren;
    int position = -1;
    int splitP;
    int found = false;
    KEY_TYPE *allKey = new KEY_TYPE[capacity];
    bigint *allChildren = new bigint[capacity + 1];

    dirty = true;
    (*sn) = new BTreeINode(tree);
    (*sn)->level = level;

    for (i = 0; i <= num_entries; i++) {
        if ((children[i] == lchild) && !found) {
            position = i;
            found = true;
        }
    }
    totalKey = capacity;
    totalChildren = totalKey + 1;
    splitP = totalKey / 2;

    for (i = 0; i < totalKey; i++) {
        if (i < position)
            allKey[i] = keys[i];
        else if (i == position)
            allKey[i] = key;
        else
            allKey[i] = keys[i - 1];
    }

    for (i = 0; i < totalChildren; i++) {
        if (i < position + 1)
            allChildren[i] = children[i];
        else if (i == position + 1)
            allChildren[i] = rchild;
        else
            allChildren[i] = children[i - 1];
    }

    for (i = 0; i < totalKey; i++) {
        if (i < splitP)
            keys[i] = allKey[i];
        else if (i > splitP)
            (*sn)->keys[i - splitP - 1] = allKey[i];
    }

    for (i = 0; i < totalChildren; i++) {
        if (i <= splitP)
            children[i] = allChildren[i];
        else
            (*sn)->children[i - splitP - 1] = allChildren[i];
    }
    num_entries = splitP;
    (*sn)->num_entries = totalKey - splitP - 1;
    (*sn)->level = level;
    *splitKey = allKey[splitP];
    delete[] allKey;
    delete[] allChildren;
    return true;
}

RET_TYPE BTreeINode::_delete(BTreeLEntry *e) {
    int childpos = selectChild(e->key);
    bigint child = children[childpos];
    if (level == 1) {
        BTreeLNode *lnode = new BTreeLNode(tree, child);
        RET_TYPE ret = lnode->_delete(e);

        if (ret != UNDERFILL) {
            delete lnode;
            return ret;
        }

        BTreeLNode *sibling;
        if (childpos != 0)    //has left sibling
        {
            sibling = new BTreeLNode(tree, children[childpos - 1]);
            if (redist(sibling, childpos - 1, lnode)) {
                delete sibling;
                delete lnode;
                return NORMAL;
            } else if (merge(sibling, childpos - 1, lnode)) {
                delete sibling;
                delete lnode;
                if (num_entries < capacity / 2)
                    return UNDERFILL;
                return NORMAL;
            }
            delete sibling;
        }

        if (childpos != num_entries)    //has right sibling
        {
            sibling = new BTreeLNode(tree, children[childpos + 1]);
            if (redist(lnode, childpos, sibling)) {
                delete sibling;
                delete lnode;
                return NORMAL;
            } else if (merge(lnode, childpos, sibling)) {
                delete sibling;
                delete lnode;
                if (num_entries < capacity / 2)
                    return UNDERFILL;
                return NORMAL;
            }
            delete sibling;
        }

        if (lnode->num_entries == 0) {
            lnode->dirty = false;
            delete lnode;

            for (int i = childpos; i < num_entries - 2; i++) {
                keys[i] = keys[i + 1];
                children[i] = children[i + 1];
            }
            children[num_entries - 1] = children[num_entries];
            dirty = true;
            tree->num_of_onode--;
        } else
            delete lnode;

        if (num_entries < capacity / 2)
            return UNDERFILL;
        return NORMAL;
    } else {
        BTreeINode *inode = new BTreeINode(tree, child);
        RET_TYPE ret = inode->_delete(e);
        if (ret != UNDERFILL) {
            delete inode;
            return ret;
        }

        BTreeINode *sibling = NULL;
        if (childpos != 0)    //has left sibling
        {
            sibling = new BTreeINode(tree, children[childpos - 1]);
            if (redist(sibling, childpos - 1, inode)) {
                delete sibling;
                delete inode;
                return NORMAL;
            } else if (merge(sibling, childpos - 1, inode)) {
                delete sibling;
                delete inode;
                if (num_entries < capacity / 2)
                    return UNDERFILL;
                else
                    return NORMAL;
            }
            delete sibling;
        }

        if (childpos != num_entries)    //has right sibling
        {
            sibling = new BTreeINode(tree, children[childpos + 1]);
            if (redist(inode, childpos, sibling)) {
                delete sibling;
                delete inode;
                return NORMAL;
            } else if (merge(inode, childpos, sibling)) {
                delete sibling;
                delete inode;
                if (num_entries < capacity / 2)
                    return UNDERFILL;
                else
                    return NORMAL;
            }
            delete sibling;
        }
        delete inode;
        return NORMAL;
    }
}

bool BTreeINode::redist(BTreeLNode *lchild, int pos, BTreeLNode *rchild) {
    bool result = false;
    int i = 0, j;
    int total, splitP;

    if (lchild->num_entries < lchild->capacity / 2)    //left node underflow
    {
        //find number of entries having key equal to the first key in rnode
        int firstKeyNum = 1;
        while (firstKeyNum < rchild->num_entries &&
               rchild->entries[0].key == rchild->entries[firstKeyNum].key)
            firstKeyNum++;

        int lastKeyPos = rchild->num_entries - 1;
        while (lastKeyPos > 0 &&
               rchild->entries[rchild->num_entries - 1].key == rchild->entries[lastKeyPos].key)
            lastKeyPos--;

        total = lchild->num_entries + rchild->num_entries + rchild->num_overflow;
        splitP = total / 2;

        while (lchild->num_entries + firstKeyNum < lchild->capacity    //lnode not overflow
               &&
               rchild->num_entries + rchild->num_overflow - firstKeyNum >= rchild->capacity / 2    //rnode not underflow
                ) {
            //Can redistribute
            i = firstKeyNum;
            if (firstKeyNum >= lastKeyPos) {
                result = true;
                break;
            }
            if (rchild->num_entries - firstKeyNum < splitP) {
                result = true;
                break;
            }

            while (rchild->entries[i].key == rchild->entries[firstKeyNum].key
                   && firstKeyNum < rchild->num_entries)
                firstKeyNum++;
            result = true;
        }

        if (result) {
            firstKeyNum = i;
            BTreeONode *onode = NULL, *nonode = NULL;

            if (rchild->over >= 0)
                onode = new BTreeONode(tree, rchild->over);

            for (i = lchild->num_entries, j = 0; j < firstKeyNum; i++, j++) {    //copy from rnode
                if (i < lchild->capacity) { //leaf node not full
                    lchild->entries[i] = rchild->entries[j];
                    lchild->num_entries++;
                } else {    //leaf node full, copy into new overflow node
                    if (nonode == NULL)
                        nonode = new BTreeONode(tree);
                    nonode->entries[i - lchild->capacity] = rchild->entries[j];
                    nonode->num_entries++;
                }
            }
            for (j = 0; j < rchild->num_entries - firstKeyNum; j++)
                rchild->entries[j] = rchild->entries[j + firstKeyNum];

            if (onode != NULL) {
                while (j < lchild->capacity && onode->num_entries > 0) {
                    rchild->entries[j] = onode->entries[onode->num_entries - 1];
                    onode->num_entries--;
                    rchild->num_overflow--;
                    j++;
                }
            }
            rchild->num_entries = j;

            if (nonode != NULL) {
                lchild->over = nonode->block;
                lchild->num_overflow = nonode->num_entries;
                nonode->dirty = true;
                delete nonode;
                tree->num_of_onode++;
            }

            if (onode != NULL) {
                if (onode->num_entries != 0) {
                    rchild->over = onode->block;
                    onode->dirty = true;
                } else {
                    rchild->over = onode->next;
                    tree->num_of_onode--;
                }
                delete onode;
            }

            lchild->dirty = true;
            rchild->dirty = true;

            keys[pos] = rchild->entries[0].key;
            dirty = true;
            return true;
        }
        return false;
    } else    //right node underflow
    {
        //get number of entries having the same key with last entry in lnode
        int lastKeyNum = 1;
        while (lastKeyNum < lchild->num_entries &&
               lchild->entries[lchild->num_entries - 1].key ==
               lchild->entries[lchild->num_entries - 1 - lastKeyNum].key)
            lastKeyNum++;

        //get number of entries having the same key with last entry in rnode
        int lastKeyNum2 = 1;
        while (lastKeyNum2 < rchild->num_entries &&
               rchild->entries[rchild->num_entries - 1].key ==
               rchild->entries[rchild->num_entries - 1 - lastKeyNum2].key)
            lastKeyNum2++;

        total = lchild->num_entries + rchild->num_entries + lchild->num_overflow;    //total number of entries
        splitP = total / 2;    //split position

        while (lchild->num_entries - lastKeyNum >= lchild->capacity / 2    //lnode not underflow
               && rchild->num_entries - lastKeyNum2 + lchild->num_overflow + lastKeyNum <
                  lchild->capacity//rnode not overflow
                ) {
            //can redistribute, find most suitable split position
            i = lastKeyNum;
            if (lchild->num_entries - lastKeyNum < splitP) {
                result = true;
                break;
            }
            while (lastKeyNum < lchild->num_entries &&
                   lchild->entries[lchild->num_entries - 1 - i].key ==
                   lchild->entries[lchild->num_entries - 1 - lastKeyNum].key
                    )
                lastKeyNum++;
            result = true;
        }

        if (result) {    //can redistribute
            lastKeyNum = i;    //number of entries to be moved

            rchild->num_overflow = lastKeyNum + lchild->num_overflow + rchild->num_entries - rchild->capacity;
            if (rchild->num_overflow < 0)
                rchild->num_overflow = 0;

            if (rchild->num_overflow > 0) {
                BTreeONode *nonode = new BTreeONode(tree);
                for (i = 0; i < rchild->num_overflow; i++)
                    nonode->entries[i] = rchild->entries[rchild->num_entries - 1 - i];
                nonode->dirty = true;
                nonode->num_entries = rchild->num_overflow;
                rchild->over = nonode->block;
                delete nonode;
                tree->num_of_onode++;
            }

            rchild->num_entries += lastKeyNum + lchild->num_overflow - rchild->num_overflow;

            for (i = rchild->num_entries - 1; i >= lastKeyNum + lchild->num_overflow; i--)
                rchild->entries[i] = rchild->entries[i - lastKeyNum - lchild->num_overflow];


            for (i = 0; i < lastKeyNum; i++)    //copy from lnode
                rchild->entries[i] = lchild->entries[lchild->num_entries - lastKeyNum + i];

            if (lchild->over >= 0) {    //copy from overflow nodes
                int overblock = lchild->over;
                while (overblock >= 0) {
                    BTreeONode *onode = new BTreeONode(tree, overblock);
                    for (j = 0; j < onode->num_entries; i++, j++)
                        rchild->entries[i] = onode->entries[j];
                    tree->num_of_onode--;
                    overblock = onode->next;
                    delete onode;
                }
                lchild->over = -1;
                lchild->num_overflow = 0;
            }

            lchild->num_entries -= lastKeyNum;
            lchild->dirty = true;
            rchild->dirty = true;

            keys[pos] = rchild->entries[0].key;
            dirty = true;

            return true;
        }
        //can not redistribute
        return false;
    }
}

bool BTreeINode::redist(BTreeINode *lchild, int pos, BTreeINode *rchild) {
    if ((lchild->num_entries + rchild->num_entries) < capacity)
        return false;    //can't redist

    int splitP = (lchild->num_entries + rchild->num_entries) / 2;
    int i, j;
    if (splitP > lchild->num_entries) {
        lchild->keys[lchild->num_entries++] = keys[pos];
        lchild->children[lchild->num_entries] = rchild->children[0];

        for (i = 0; lchild->num_entries < splitP; i++) {
            lchild->keys[lchild->num_entries++] = rchild->keys[i];
            lchild->children[lchild->num_entries] = rchild->children[i + 1];
        }

        keys[pos] = rchild->keys[i++];

        for (j = 0; i < rchild->num_entries; j++, i++) {
            rchild->children[j] = rchild->children[i];
            rchild->keys[j] = rchild->keys[i];
        }
        rchild->children[j] = rchild->children[i];
        rchild->num_entries = j;
    } else {
        int movNo = lchild->num_entries - splitP;
        rchild->children[rchild->num_entries + movNo] = rchild->children[rchild->num_entries];
        for (i = rchild->num_entries - 1; i >= 0; i--) {
            rchild->keys[i + movNo] = rchild->keys[i];
            rchild->children[i + movNo] = rchild->children[i];
        }
        rchild->keys[movNo - 1] = keys[pos];
        rchild->children[movNo - 1] = lchild->children[lchild->num_entries--];
        for (i = movNo - 2; i >= 0; i--) {
            rchild->keys[i] = lchild->keys[lchild->num_entries];
            rchild->children[i] = lchild->children[lchild->num_entries--];
        }
        keys[pos] = lchild->keys[lchild->num_entries];
        rchild->num_entries += movNo;
    }

    lchild->dirty = true;
    rchild->dirty = true;
    dirty = true;
    return true;
}

bool BTreeINode::merge(BTreeLNode *lchild, int pos, BTreeLNode *rchild) {
    int lastKeyNum;
    int i, j;

    lastKeyNum = 1;
    while (lastKeyNum < rchild->num_entries &&
           rchild->entries[rchild->num_entries - 1].key == rchild->entries[rchild->num_entries - 1 - lastKeyNum].key)
        lastKeyNum++;

    if (lchild->num_entries + lchild->num_overflow + rchild->num_entries - lastKeyNum >= lchild->capacity)
        return false;    //can't merge into one node;

    BTreeONode *onode = NULL;
    if (lchild->over >= 0)    //copy from overflow node of left leaf node (at most one onode)
    {
        onode = new BTreeONode(tree, lchild->over);
        for (j = 0; j < onode->num_entries; j++)
            lchild->entries[lchild->num_entries++] = onode->entries[j];
        delete onode;
        onode = NULL;
        tree->num_of_onode--;
    }

    if (rchild->over >= 0) {
        onode = new BTreeONode(tree, rchild->over);
        tree->num_of_onode--;
    }

    BTreeONode *nonode = NULL;
    for (j = 0; j < rchild->num_entries; j++) {    //copy right leaf node
        if (lchild->num_entries < lchild->capacity)    //new node not full
            lchild->entries[lchild->num_entries++] = rchild->entries[j];
        else if (onode == NULL ||
                 onode->num_entries < onode->capacity)    //new node full, copy into first overflow node
        {
            if (onode == NULL)
                onode = new BTreeONode(tree);
            onode->entries[onode->num_entries++] = rchild->entries[j];
            rchild->num_overflow++;
        } else//copy into new overflow node
        {
            if (nonode == NULL)
                nonode = new BTreeONode(tree);

            nonode->entries[nonode->num_entries++] = rchild->entries[j];
            rchild->num_overflow++;
        }
    }

    if (onode != NULL) {    //has overflow node
        if (nonode != NULL) {    //has more than one overflow node
            nonode->next = onode->next;    //link following overflow nodes
            onode->next = nonode->block;
            nonode->dirty = true;
            delete nonode;
            tree->num_of_onode++;
        }
        lchild->over = onode->block;
        tree->num_of_onode++;
        onode->dirty = true;
        delete onode;

        lchild->num_overflow = rchild->num_overflow;
    }

    rchild->dirty = false;
    lchild->dirty = true;
    lchild->next = rchild->next;

    //move second entry, release space
    for (i = pos; i < num_entries - 1; i++) {
        keys[i] = keys[i + 1];
        children[i + 1] = children[i + 2];
    }
    num_entries--;
    dirty = true;

    tree->num_of_dnode--;

    return true;
}

bool BTreeINode::merge(BTreeINode *lchild, int pos, BTreeINode *rchild) {
    if (lchild->num_entries + rchild->num_entries > capacity)
        return false;    //can't merge

    lchild->keys[lchild->num_entries++] = keys[pos];
    int i;
    for (i = 0; i < rchild->num_entries; i++) {
        lchild->children[lchild->num_entries] = rchild->children[i];
        lchild->keys[lchild->num_entries++] = rchild->keys[i];
    }
    lchild->children[lchild->num_entries] = rchild->children[i];

    lchild->dirty = true;
    rchild->dirty = false;

    for (i = pos; i < num_entries - 1; i++) {
        keys[i] = keys[i + 1];
        children[i + 1] = children[i + 2];
    }
    num_entries--;
    dirty = true;
    tree->num_of_inode--;
    return true;
}

void BTreeINode::traverse(FILE *fp, int &datacount, int &lnodecount, int &inodecount, int &onodecount) {
    if (level == 1) {
        for (int i = 0; i <= num_entries; i++) {
            BTreeLNode *lnode = new BTreeLNode(tree, children[i]);
            lnodecount++;

            fprintf(fp, "NO_ENTRIES: %d\tOVER:%d\tOVERNUM:%d\n", lnode->num_entries, lnode->over, lnode->num_overflow);
            for (int j = 0; j < lnode->num_entries; j++) {
                fprintf(fp, "RID: %dKEY: %d\n", lnode->entries[j].rid, lnode->entries[j].key);
                datacount++;
            }
            bigint overblock = lnode->over;
            while (overblock >= 0) {
                fprintf(fp, "# ONODE\n");
                BTreeONode *onode = new BTreeONode(tree, overblock);
                onodecount++;
                for (int k = 0; k < onode->num_entries; k++) {
                    fprintf(fp, "RID: %d\n", onode->entries[k].rid);
                    datacount++;
                }
                overblock = onode->next;
                delete onode;
                fprintf(fp, "# \n");
            }
            delete lnode;
        }
    } else {
        for (int i = 0; i < num_entries; i++) {
            BTreeINode *inode = new BTreeINode(tree, children[i]);
            inodecount++;
            inode->traverse(fp, datacount, lnodecount, inodecount, onodecount);
            delete inode;
        }
        BTreeINode *inode = new BTreeINode(tree, children[num_entries]);
        inodecount++;
        inode->traverse(fp, datacount, lnodecount, inodecount, onodecount);
        delete inode;
    }
}

void BTreeINode::traverseLeaf(FILE *fp, int &datacount, int &lnodecount, int &onodecount) {
    if (level == 1) {
        bigint addr = children[0];
        while (addr != -1) {
            BTreeLNode *lnode = new BTreeLNode(tree, addr);
            lnodecount++;
            fprintf(fp, "NO_ENTRIES: %d\tOVER:%d\tOVERNUM:%d\n", lnode->num_entries, lnode->over, lnode->num_overflow);
            for (int j = 0; j < lnode->num_entries; j++) {
                fprintf(fp, "RID: %dKEY: %d\n", lnode->entries[j].rid, lnode->entries[j].key);
                datacount++;
            }
            bigint overblock = lnode->over;
            while (overblock >= 0) {
                fprintf(fp, "# ONODE\n");
                BTreeONode *onode = new BTreeONode(tree, overblock);
                onodecount++;
                for (int k = 0; k < onode->num_entries; k++) {
                    fprintf(fp, "RID: %d\n", onode->entries[k].rid);
                    datacount++;
                }
                overblock = onode->next;
                delete onode;
                fprintf(fp, "# \n");
            }
            addr = lnode->next;
            delete lnode;
        }
    } else {
        BTreeINode *inode = new BTreeINode(tree, children[0]);
        inode->traverseLeaf(fp, datacount, lnodecount, onodecount);
        delete inode;
    }
}