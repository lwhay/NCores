#ifndef PMwCAS_H
#define PMwCAS_H

#include <libpmemobj.h>
#include <libpmem.h>
#include "bzconfig.h"
#include "rel_ptr.h"
#include "gc.h"
#include "utils.h"

#ifdef IS_PMEM
#define persist        pmem_persist
#else
#define persist		pmem_msync
#endif // IS_PMEM

#define RDCSS_BIT        0x8000000000000000
#define MwCAS_BIT        0x4000000000000000
#define DIRTY_BIT        0x2000000000000000
#define ADDR_MASK        0xffffffffffff

#define ST_UNDECIDED    0
#define ST_SUCCESS        1
#define ST_FAILED        2
#define ST_FREE            3

#define RELEASE_NEW_ON_FAILED            1
#define RELEASE_EXP_ON_SUCCESS            2
#define RELEASE_SWAP_PTR                3 //release new on failed and release expect on success
#define NOCAS_RELEASE_ADDR_ON_SUCCESS    4 //no CAS performed, only used to reclaim memories pointed to by field addr
#define NOCAS_EXECUTE_ON_FAILED            5 //no CAS performed, only used to execute CAS after failure, e.g. unfreeze node when power failure
#define NOCAS_RELEASE_NEW_ON_FAILED        6 //no CAS performed, only used to reclaim memoried pointed to by field new_val

#define EXCHANGE        InterlockedExchange
#define CAS                InterlockedCompareExchange

struct word_entry;
struct pmwcas_entry;
struct pmwcas_pool;

typedef void(*recycle_func_t)(void *);

typedef rel_ptr<word_entry> wdesc_t;
typedef rel_ptr<pmwcas_entry> mdesc_t;
typedef pmwcas_pool *mdesc_pool_t;

struct word_entry {
    rel_ptr<uint64_t> addr;
    uint64_t expect;
    uint64_t new_val;
    mdesc_t mdesc;
    off_t recycle_func; /* 0���ޣ�1��ʧ��ʱ�ͷ�new��2���ɹ�ʱ�ͷ�expect */
};

struct pmwcas_entry {
    uint64_t status;
    gc_entry_t gc_entry;
    mdesc_pool_t mdesc_pool;
    size_t count;
    off_t callback;
    word_entry wdescs[WORD_DESCRIPTOR_SIZE];
};

struct pmwcas_pool {
    //recycle_func_t		callbacks[CALLBACK_SIZE];
    gc_t *gc;
    pmwcas_entry mdescs[DESCRIPTOR_POOL_SIZE];
    bz_memory_pool mem_;
    uint64_t magic[WORD_DESCRIPTOR_SIZE];
};

/* 
* ��һ��ʹ��pmwcasʱ����
* ��ʼ��������״̬
*/
void pmwcas_first_use(mdesc_pool_t pool, PMEMobjpool *pop, PMEMoid oid);

/*
* ÿ������ʱ���ȵ���
* �������ָ��Ļ���ַ
* ����G/C�ͻ����߳�
*/
int pmwcas_init(mdesc_pool_t pool, PMEMoid oid, PMEMobjpool *pop);

/*
* ����ʱ����
* GC
* ����G/C
*/
void pmwcas_finish(mdesc_pool_t pool);

/*
* ÿ������ʱ����
* �޸�������״̬
* ��ɻ�ع��ϴ�δ��ɵ�PMwCAS
* ���տ���й©���ڴ�����
*/
void pmwcas_recovery(mdesc_pool_t pool);

/*
* ÿ��ִ��PMwCAS֮ǰ����
* ȷ����������Ŀ���ַ������ͬ
* ����PMwCAS�����������ָ��
* ʧ��ʱ���ؿյ����ָ��
*/
mdesc_t pmwcas_alloc(mdesc_pool_t pool, off_t recycle_policy = 0, off_t search_pos = 0);

/*
* ����ִ��PMwCAS, ���̵߳���
* ������ʱ���ܷ�����ڴ棬�Ա���ϵͳ�ϵ�����
*/
bool pmwcas_abort(mdesc_t mdesc);

rel_ptr<uint64_t> get_magic(mdesc_pool_t pool, int magic);

/*
* ÿ��ִ����PMwCAS֮�����
* ���ն�Ӧ��PMwCAS������
*/
void pmwcas_free(mdesc_t mdesc);

/* ִ���ͷź��� */
void pmwcas_word_recycle(mdesc_pool_t pool, rel_ptr<rel_ptr<uint64_t>> ptr_leak);

/*
* ��PMwCAS���������һ��CAS�ֶ�
* ���سɹ�/ʧ��
*/
bool pmwcas_add(mdesc_t mdesc, rel_ptr<uint64_t> addr, uint64_t expect, uint64_t new_val, off_t recycle = 0);

/* ִ��PMwCAS */
bool pmwcas_commit(mdesc_t mdesc);

/* ��ȡ���ܱ�PMwCAS�������ֵ�ֵ */
uint64_t pmwcas_read(uint64_t *addr);

/*
* ��PMwCAS���������һ��CAS�ֶ�
* ����new_val�����գ�������д�����������ڴ�ĵ�ַ
* recycle�ֶ�����ָ��ĳЩ�����µ�NVM�ڴ����
* ����0���ޣ�1��ʧ��ʱ����new_val��2���ɹ�ʱ����expect
*/
/*
����ͨ�ڴ滷����ʵ��
parent->child[0] = new node();
��NVM�У���Ҫ
1) ����洢�ռ�
2) ��parent->child[0]ָ��ô洢�ռ���׵�ַ
3) �־û�parent->child[0]
����:
���Ϲ�����ʱ���ܷ����ϵ磬���
1) �����Դ洢й¶��1����2û����ɣ�
2) ָ��������Ⱦ��3û����ɣ�
�������:
1) ʹ��pmdk�ṩ��ԭ���ڴ������
2) ʹ��pmwcas�־û�ָ������
*/
template<typename T>
rel_ptr<rel_ptr<T>> pmwcas_reserve(mdesc_t mdesc, rel_ptr<rel_ptr<T>> addr, rel_ptr<T> expect, off_t recycle = 0) {
    /* check if PMwCAS is full */
    if (mdesc->count == WORD_DESCRIPTOR_SIZE) {
        assert(0);
        return rel_ptr<rel_ptr<T>>::null();
    }
    /*
    * check if the target address exists
    * otherwise, find the insert point
    */
    for (off_t i = 0; i < mdesc->count; ++i) {
        wdesc_t wdesc = mdesc->wdescs + i;
        if (wdesc->addr == addr) {
            assert(0);
            return rel_ptr<rel_ptr<T>>::null();
        }
    }
    wdesc_t wdesc = mdesc->wdescs + mdesc->count;

    wdesc->addr = addr;
    wdesc->expect = expect.rel();
    wdesc->new_val = 0;
    wdesc->mdesc = mdesc;
    wdesc->recycle_func = !recycle ? mdesc->callback : recycle;
    persist(wdesc.abs(), sizeof(*wdesc));

    ++mdesc->count;
    persist(&mdesc->count, sizeof(uint64_t));
    return rel_ptr<rel_ptr<T>>((rel_ptr<T> *) &mdesc->wdescs[mdesc->count - 1].new_val);
}

#endif // !PMwCAS_H