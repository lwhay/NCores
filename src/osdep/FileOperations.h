//
// Created by Michael on 2018/12/1.
//
#pragma
#if linux //|| __CYGWIN__
#define bigseek fseeko64
#define bigtell ftello64
#else
#ifdef __MINGW64__
#include <stdlib.h>
#define bigseek _fseeki64
#define bigtell _ftelli64
#else
#define bigseek fseek
#define bigtell ftell
#endif
#endif

typedef unsigned long long bigint;