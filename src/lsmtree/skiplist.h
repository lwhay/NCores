#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#include "util.h"
#include "platform.h"

#define MAXLEVEL (15)
#define SKIP_KSIZE (256)

typedef enum {
    ADD, DEL
} OPT;

struct skipnode {
    char key[SKIP_KSIZE];
    UINT val;
    OPT opt;
    struct skipnode *forward[1];
    struct skipnode *next;
};

struct skiplist {
    struct skipnode *hdr;
    size_t count;
    size_t size;
    int level;
    char pool_embedded[1024];
    struct pool *pool;
};

struct skiplist *skiplist_new(size_t size);

void skiplist_init(struct skiplist *list);

int skiplist_insert(struct skiplist *list, struct slice *sk, UINT val, OPT opt);

int skiplist_notfull(struct skiplist *list);

void skiplist_dump(struct skiplist *list);

void skiplist_free(struct skiplist *list);


#endif
