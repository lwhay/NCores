#ifndef _UTIL_H
#define _UTIL_H

#ifndef linux
#define UINT unsigned int
#endif

struct slice {
    char *data;
    int len;
};

#endif
