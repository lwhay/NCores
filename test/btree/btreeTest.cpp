#include "SerialBTree.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include "misc/querytype.h"

extern int DIM;

//bplus test
int main() {
    DIM = 4;
    SerialBTree *bplus = new SerialBTree("index", new Cache(0, 4096));
    KEY_TYPE keys[10000];
    int maxN = 1000;
    for (int i = 0; i < maxN; i++) {
        printf("%d\n", i);
        BTreeLEntry *e = new BTreeLEntry();
        keys[i] = e->key = rand() % maxN;
        e->rid = i;
        e->record.coord[0] = rand();
        e->record.coord[1] = rand();
        e->record.coord[2] = rand();
        e->record.coord[3] = rand();
        bplus->insert(e->rid, e->key, &e->record);
        delete e;
        if (i % (maxN / 100) == 0) {
            printf("#%d\n", i);
#ifdef __DEBUG
            printf("%d %d %d %d\n", bplus->num_of_data, bplus->num_of_dnode, bplus->num_of_onode, bplus->num_of_inode);
            bplus->Traverse();
            getchar();
#endif
        }
    }
    printf("b+:%d %d %d %d\n", bplus->num_of_data, bplus->num_of_dnode, bplus->num_of_onode, bplus->num_of_inode);
    bplus->traverse();
    printf("%d", bplus->cache->num_io);
    getchar();
    bplus->cache->flush();
    bplus->cache->num_io = 0;

    for (int i = 0; i < maxN; i++) {
        BTreeLEntry *e = new BTreeLEntry();
        e->key = keys[i];
        e->rid = i;
        e->record.coord[0] = rand();
        e->record.coord[1] = rand();
        e->record.coord[2] = rand();
        e->record.coord[3] = rand();
        if (!bplus->_delete(e->rid, e->key)) {
            printf("can't find %d\n", i);
            getchar();
        }
        delete e;
        e = new BTreeLEntry();
        e->key = keys[i];
        e->rid = i;
        e->record.coord[0] = rand();
        e->record.coord[1] = rand();
        e->record.coord[2] = rand();
        e->record.coord[3] = rand();
        bplus->insert(e->rid, e->key, &(e->record));
        delete e;
        if (i % (maxN / 10) == 0) {
            printf("#%d\n", i);
#ifdef    __DEBUG
            printf("%d %d %d %d\n", bplus->num_of_data, bplus->num_of_dnode, bplus->num_of_onode, bplus->num_of_inode);
            bplus->Traverse();
            getchar();
#endif
        }
    }
    printf("%d", bplus->cache->num_io);
    getchar();
    printf("%d %d %d %d\n", bplus->num_of_data, bplus->num_of_dnode, bplus->num_of_onode, bplus->num_of_inode);

    KEY_TYPE lowkey = 100;
    KEY_TYPE highkey = 200;
    BTreeLEntry *rs = new BTreeLEntry[10000];

    int rsno = bplus->search(lowkey, highkey, rs);
    for (int i = 0; i < rsno; i++)
        printf("OID: %d\n", rs[i].rid);
    delete[] rs;

    bplus->cache->flush();
    printf("%d", bplus->cache->num_io);
    bplus->cache->num_io = 0;
    delete bplus;
    return 0;
}
