#ifndef BZTREE_H
#define BZTREE_H

#include <thread>
#include <vector>
#include <tuple>
#include <algorithm>
#include <queue>

#include "bzerrno.h"
#include "PMwCAS.h"
#include "utils.h"

#include <mutex>
#include <fstream>

//Consolidation types
#define BZ_SPLIT            1
#define BZ_MERGE            2
#define BZ_CONSOLIDATE        3
#define BZ_FROZEN            4

//Action types
#define BZ_ACTION_INSERT    1
#define BZ_ACTION_DELETE    2
#define BZ_ACTION_UPDATE    3
#define BZ_ACTION_UPSERT    4
#define BZ_ACTION_READ        5

//Node types
const uint64_t BZ_KEY_MAX = 0xdeadbeafdeadbeaf;
#define BZ_TYPE_LEAF        1
#define BZ_TYPE_NON_LEAF    2

template<typename Key, typename Val>
struct bz_tree;

//Print
#include <iomanip>

std::mutex mylock;
std::fstream fs("log.txt", std::ios::app);

/* Head node of BzTree */
/* For leaf, Val represents the actual type; Otherwise, for the related pointer of a child node */
template<typename Key, typename Val>
struct bz_node {
    /* length 32: node size, 31: sorted count, 1: is_leaf */
    uint64_t length_;
    /* status 3: PMwCAS control, 1: frozen, 16: record count, 22: block size, 22: delete size */
    uint64_t status_;

    /* record meta entry 3: PMwCAS control, 1: visiable, 28: offset, 16: key length, 16: total length */
    uint64_t *rec_meta_arr();

    /* K-V getter and setter */
    Key *get_key(uint64_t meta);

    void set_key(uint32_t offset, const Key *key);

    void copy_key(Key *dst, const Key *src);

    Val *get_value(uint64_t meta);

    void set_value(uint32_t offset, const Val *val);

    void copy_value(Val *dst, const Val *src);

    /* Comparators */
    int key_cmp(uint64_t meta_1, const Key *key);

    bool key_cmp_meta(uint64_t meta_1, uint64_t meta_2);

    /* Basic operations */
    uint32_t binary_search(const Key *key, int size = 0, uint64_t *meta_arr = nullptr);

    bool find_key_sorted(const Key *key, uint32_t &pos);

    bool find_key_unsorted(const Key *key, uint64_t status_rd, uint32_t alloc_epoch, uint32_t &pos, bool &recheck);

    uint64_t status_add_rec_blk(uint64_t status_rd, uint32_t total_size);

    uint64_t status_del(uint64_t status_rd, uint32_t total_size);

    template<typename TreeVal>
    bool add_dele_sz(bz_tree<Key, TreeVal> *tree, uint32_t total_size);

    uint64_t status_frozen(uint64_t status_rd);

    uint64_t meta_vis_off(uint64_t meta_rd, bool set_vis, uint32_t new_offset);

    uint64_t
    meta_vis_off_klen_tlen(uint64_t meta_rd, bool set_vis, uint32_t new_offset, uint32_t key_size, uint32_t total_size);

    void copy_data(uint32_t new_offset, const Key *key, const Val *val, uint32_t key_size, uint32_t total_size);

    void copy_data(uint64_t meta_rd, std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> &res);

    template<typename TreeVal>
    int rescan_unsorted(bz_tree<Key, TreeVal> *tree, uint32_t beg_pos, uint32_t rec_cnt, const Key *key,
                        uint32_t total_size, uint32_t alloc_epoch);

    /* SMO functions */
    int triger_consolidate();

    uint32_t copy_node_to(rel_ptr<bz_node<Key, Val>> dst, uint64_t status_rd = 0);

    void fr_sort_meta();

    void fr_remove_meta(int pos);

    int fr_insert_meta(const Key *key, uint64_t left, uint32_t key_sz, uint64_t right);

    int fr_root_init(const Key *key, uint64_t left, uint32_t key_sz, uint64_t right);

    uint32_t copy_sort_meta_to(rel_ptr<bz_node<Key, Val>> dst);

    uint32_t copy_payload_to(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt);

    void init_header(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t blk_sz, int leaf_opt = 0,
                     uint32_t dele_sz = 0);

    uint32_t valid_block_size(uint64_t status_rd = 0);

    uint32_t valid_node_size(uint64_t status_rd = 0);

    uint32_t valid_record_count(uint64_t status_rd = 0);

    uint32_t fr_get_balanced_count(rel_ptr<bz_node<Key, Val>> dst);

    rel_ptr<uint64_t> nth_child(int n);

    Key *nth_key(int n);

    Val *nth_val(int n);

    template<typename TreeVal>
    mdesc_t try_freeze(bz_tree<Key, TreeVal> *tree);

    template<typename TreeVal>
    bool unfreeze(bz_tree<Key, TreeVal> *tree);

    template<typename TreeVal>
    int consolidate(bz_tree<Key, TreeVal> *tree, rel_ptr<uint64_t> parent_status, rel_ptr<uint64_t> parent_ptr);

    template<typename TreeVal>
    int split(bz_tree<Key, TreeVal> *tree, rel_ptr<bz_node<Key, uint64_t>> parent, rel_ptr<uint64_t> grandpa_status,
              rel_ptr<uint64_t> grandpa_ptr);

    template<typename TreeVal>
    int merge(bz_tree<Key, TreeVal> *tree, int child_id, rel_ptr<bz_node<Key, uint64_t>> parent,
              rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr);

    /* Operations on leaf nodes */
    template<typename TreeVal>
    int insert(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size, uint32_t total_size,
               uint32_t alloc_epoch);

    template<typename TreeVal>
    int remove(bz_tree<Key, TreeVal> *tree, const Key *key);

    template<typename TreeVal>
    int update(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size, uint32_t total_size,
               uint32_t alloc_epoch);

    template<typename TreeVal>
    int read(bz_tree<Key, TreeVal> *tree, const Key *key, Val *buffer, uint32_t max_val_size);

    template<typename TreeVal>
    int upsert(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size, uint32_t total_size,
               uint32_t alloc_epoch);

    std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>>
    range_scan(const Key *beg_key, const Key *end_key);

    void print_log(const char *action, const Key *k = nullptr, uint64_t ret = -1, bool pr =
#ifdef BZ_DEBUG
    true
#else
    false
#endif // BZ_DEBUG
    );

};

/* BzTree */
template<typename Key, typename Val>
struct bz_tree {
    PMEMobjpool *pop_;
    pmwcas_pool pool_;
    uint64_t root_;
    uint32_t epoch_;

    void first_use(PMEMobjpool *pop, PMEMoid base_oid);

    int init(PMEMobjpool *pop, PMEMoid base_oid);

    void recovery();

    void finish();

    int insert(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size);

    int remove(const Key *key);

    int update(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size);

    int upsert(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size);

    int read(const Key *key, Val *buffer, uint32_t max_val_size);


    /* Inner operations */
    void register_this();

    bool acquire_wr(bz_path_stack *path_stack);

    void acquire_rd();

    void release();

    template<typename NType>
    bool smo(bz_path_stack *path_stack, int &ret);

    int new_root();

    int traverse(int action, bool wr, const Key *key, const Val *val = nullptr, uint32_t key_size = 0,
                 uint32_t total_size = 0, Val *buffer = nullptr, uint32_t max_val_size = 0);

    template<typename NType>
    rel_ptr<rel_ptr<bz_node<Key, NType>>> alloc_node(mdesc_t mdesc, int magic = 0);

    mdesc_t alloc_mdesc(int recycle = 0);

    void recycle_node(rel_ptr<rel_ptr<uint64_t>> ptr);

    int pack_pmwcas(std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn);

    void print_node(uint64_t ptr, int extra);

    void print_tree(bool pr =
#ifdef BZ_DEBUG
    true
#else
    false
#endif // BZ_DEBUG
    );

    void print_dfs(uint64_t ptr, int level);

};

template<typename Key, typename Val>
int
bz_tree<Key, Val>::traverse(int action, bool wr, const Key *key, const Val *val, uint32_t key_size, uint32_t total_size,
                            Val *buffer, uint32_t max_val_size) {
    register_this();
    if (!pmwcas_read(&root_)) {
        new_root();
    }
    bz_path_stack path_stack;
    int retry = 0;
    while (true) {
        path_stack.reset();
        uint64_t root = pmwcas_read(&root_);
        path_stack.push(root, -1);
        while (true) {
            if (wr && acquire_wr(&path_stack)) {
                break;
            } else if (!wr) {
                acquire_rd();
            }

            uint64_t ptr = path_stack.get_node();
            if (is_leaf_node(ptr)) {
                rel_ptr<bz_node<Key, Val>> node(ptr);
                int ret;
                if (action == BZ_ACTION_INSERT)
                    ret = node->insert(this, key, val, key_size, total_size, epoch_);
                else if (action == BZ_ACTION_DELETE)
                    ret = node->remove(this, key);
                else if (action == BZ_ACTION_UPDATE)
                    ret = node->update(this, key, val, key_size, total_size, epoch_);
                else if (action == BZ_ACTION_UPSERT)
                    ret = node->upsert(this, key, val, key_size, total_size, epoch_);
                else
                    ret = node->read(this, key, buffer, max_val_size);
                release();
                if (ret == EPMWCASALLOC || ret == EFROZEN) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(++retry));
                    break;
                }
                return ret;
            } else {
                rel_ptr<bz_node<Key, uint64_t>> node(ptr);
                int child_id = (int) node->binary_search(key);
                uint64_t next = *node->nth_val(child_id);
                while (next & MwCAS_BIT || next & RDCSS_BIT || next & DIRTY_BIT) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    next = *node->nth_val(child_id);
                    //assert(0);
                    //node->print_log("READ_BUG");
                }
                path_stack.push(next, child_id);
                release();
            }
        }
    }
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::register_this() {
    gc_register(pool_.gc);
}

/* Check whether we need to adjust nodes and enter GC CR */
/* @param path_stack <relative positions of node> from parent to child */
template<typename Key, typename Val>
bool bz_tree<Key, Val>::acquire_wr(bz_path_stack *path_stack) {
    // Access nodes
    gc_crit_enter(pool_.gc);

    // Check whether we will adjust nodes
    bool smo_type;
    int ret;
    uint64_t ptr = path_stack->get_node();
    if (is_leaf_node(ptr)) {
        smo_type = smo<Val>(path_stack, ret);
    } else {
        smo_type = smo<uint64_t>(path_stack, ret);
    }
    if (smo_type) {
        gc_crit_exit(pool_.gc);
    }
    if (ret == EFROZEN || ret == EPMWCASALLOC) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return smo_type;
}

/* Enter CR */
template<typename Key, typename Val>
inline void bz_tree<Key, Val>::acquire_rd() {
    // Enter node
    gc_crit_enter(pool_.gc);
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::release() {
    // Exit node
    gc_crit_exit(pool_.gc);
}

template<typename Key, typename Val>
template<typename NType>
bool bz_tree<Key, Val>::smo(bz_path_stack *path_stack, int &ret) {
    rel_ptr<bz_node<Key, NType>> node(path_stack->get_node());
    int child_id = path_stack->get_child_id();
    int smo_type = node->triger_consolidate();
    if (smo_type == BZ_CONSOLIDATE) {
        CONSOLIDATE_TAG:
        if (child_id < 0) {
            ret = node->consolidate<Val>(this, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
        } else {
            path_stack->pop();
            rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
            ret = node->consolidate<Val>(this, &parent->status_, parent->nth_val(child_id));
            path_stack->push();
        }
    } else if (smo_type == BZ_SPLIT) {
        if (child_id < 0) {
            ret = node->split<Val>(this, rel_ptr<bz_node<Key, uint64_t>>::null(),
                                   rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
        } else {
            path_stack->pop();
            rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
            int grand_id = path_stack->get_child_id();
            if (grand_id < 0) {
                ret = node->split<Val>(this, parent, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
            } else {
                path_stack->pop();
                rel_ptr<bz_node<Key, uint64_t>> grandpa(path_stack->get_node());
                ret = node->split<Val>(this, parent, &grandpa->status_, grandpa->nth_val(grand_id));
                path_stack->push();
            }
            path_stack->push();
        }
    } else if (smo_type == BZ_MERGE) {
        if (child_id < 0)
            goto CONSOLIDATE_TAG;
        path_stack->pop();
        rel_ptr<bz_node<Key, uint64_t>> parent(path_stack->get_node());
        int grand_id = path_stack->get_child_id();
        if (grand_id < 0) {
            ret = node->merge<Val>(this, child_id, parent, rel_ptr<uint64_t>::null(), rel_ptr<uint64_t>::null());
        } else {
            path_stack->pop();
            rel_ptr<bz_node<Key, uint64_t>> grandpa(path_stack->get_node());
            ret = node->merge<Val>(this, child_id, parent, &grandpa->status_, grandpa->nth_val(grand_id));
            path_stack->push();
        }
        path_stack->push();
    }
    return smo_type && (ret == EFROZEN || !ret || ret == EPMWCASALLOC);
}

template<typename Key, typename Val>
template<typename NType>
rel_ptr<rel_ptr<bz_node<Key, NType>>> bz_tree<Key, Val>::alloc_node(mdesc_t mdesc, int magic) {
    rel_ptr<rel_ptr<bz_node<Key, NType>>> new_node_ptr =
            pmwcas_reserve<bz_node<Key, NType>>(mdesc,
                                                get_magic(&pool_, magic), 0, NOCAS_RELEASE_NEW_ON_FAILED);

    pool_.mem_.acquire(new_node_ptr);

    uint32_t node_sz = NODE_ALLOC_SIZE;
    rel_ptr<bz_node<Key, NType>> node = *new_node_ptr;
    memset(node.abs(), 0, sizeof(bz_node<Key, NType>));
    set_node_size(node->length_, node_sz);
    persist(node.abs(), sizeof(bz_node<uint64_t, uint64_t>));
    return new_node_ptr;
    /*
    // Atomic allocation
    TX_BEGIN(pop_) {
        pmemobj_tx_add_range_direct(new_node_ptr.abs(), sizeof(uint64_t));
        *new_node_ptr = pmemobj_tx_alloc(NODE_ALLOC_SIZE, TOID_TYPE_NUM(struct bz_node_fake));
    } TX_END;
    assert(!new_node_ptr->is_null());

    rel_ptr<bz_node<Key, Val>> new_node = *new_node_ptr;
    memset(new_node.abs(), 0, NODE_ALLOC_SIZE);
    set_node_size(new_node->length_, NODE_ALLOC_SIZE);
    return new_node;
    */
}

template<typename Key, typename Val>
int bz_node<Key, Val>::triger_consolidate() {
    uint64_t status_rd = pmwcas_read(&status_);
    if (is_frozen(status_rd))
        return BZ_FROZEN;
    uint32_t rec_cnt = get_record_count(status_rd);
    uint32_t blk_sz = get_block_size(status_rd);
    uint32_t dele_sz = get_delete_size(status_rd);
    uint32_t node_sz = get_node_size(length_);
    uint32_t free_sz = node_sz - blk_sz - sizeof(*this) - rec_cnt * sizeof(uint64_t);
    if (free_sz <= NODE_MIN_FREE_SIZE || dele_sz >= NODE_MAX_DELETE_SIZE) {
        uint32_t new_node_sz = valid_node_size(status_rd);
        if (new_node_sz >= NODE_SPLIT_SIZE)
            return BZ_SPLIT;
        else if (new_node_sz <= NODE_MERGE_SIZE)
            return BZ_MERGE;
        else
            return BZ_CONSOLIDATE;
    }
    return 0;
}

/*
merge N
1. choose N's left/right sibling if
1.1 shares the same parent
1.2 small enough to absorb N's records
2. if neither is ok, return false
3. freeze N and L status
4. allocate 2 new nodes:
4.1 new node N' contains N and L's records
4.2 N and L parent P' that swaps the child ptr to L to N'
5. 3-word PMwCAS
5.1 G's child ptr to P
5.2 G's status
5.3 freeze P
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::merge(bz_tree<Key, TreeVal> *tree, int child_id,
                             rel_ptr<bz_node<Key, uint64_t>> parent,
                             rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr) {

    /* Global Variables */
    uint64_t status_parent;
    uint64_t status_cur;
    uint64_t status_sibling;
    int sibling_type = 0; //-1: left 1: right
    rel_ptr<bz_node<Key, Val>> sibling;
    rel_ptr<rel_ptr<bz_node<Key, Val>>> new_node_ptr;
    rel_ptr<rel_ptr<bz_node<Key, uint64_t>>> new_parent_ptr;
    uint32_t child_max;
    int ret = 0;
    bool forbids[2] = {false, false};

    mdesc_t mdesc = try_freeze<TreeVal>(tree);
    if (mdesc.is_null())
        return EFROZEN;

    // Search siblings
    while (true) {
        sibling_type = 0;

        // Read the current node
        status_cur = pmwcas_read(&status_);

        // Delete if can not find
        uint32_t valid_rec_cnt = valid_record_count(status_cur);
        if (!valid_rec_cnt) {
            if (!parent.is_null()) {
                // Read parent for the following steps
                status_parent = pmwcas_read(&parent->status_);
                if (is_frozen(status_parent)) {
                    if (this->unfreeze(tree))
                        pmwcas_abort(mdesc);
                    return EFROZEN;
                }
                child_max = get_record_count(status_parent);
            }
            break;
        }

        //Can not merge root
        if (parent.is_null()) {
            if (this->unfreeze(tree))
                pmwcas_abort(mdesc);
            else
                return EPMWCASALLOC;
            return ENONEED;
        }

        //Read parent
        status_parent = pmwcas_read(&parent->status_);
        if (is_frozen(status_parent)) {
            if (this->unfreeze(tree))
                pmwcas_abort(mdesc);
            return EFROZEN;
        }
        child_max = get_record_count(status_parent);

        uint32_t cur_sz = valid_node_size(status_cur);

        //Read siblings
        if (!forbids[0] && child_id > 0) {
            //Check left sibling
            sibling = parent->nth_child(child_id - 1);
            status_sibling = pmwcas_read(&sibling->status_);
            if (!is_frozen(status_sibling)) {
                uint32_t left_sz = sibling->valid_node_size(status_sibling);
                if (cur_sz + left_sz - sizeof(*this) < NODE_SPLIT_SIZE) {
                    sibling_type = -1;
                }
            } else {
                forbids[0] = true;
            }
        }
        if (!forbids[1] && !sibling_type && (uint32_t) child_id + 1 < child_max) {
            //Check right sibling
            sibling = parent->nth_child(child_id + 1);
            status_sibling = pmwcas_read(&sibling->status_);
            if (!is_frozen(status_sibling)) {
                uint32_t right_sz = sibling->valid_node_size(status_sibling);
                if (cur_sz + right_sz - sizeof(*this) < NODE_SPLIT_SIZE) {
                    sibling_type = 1;
                }
            } else {
                forbids[1] = true;
            }
        }
        if (!sibling_type) {
            //Can not find
            if (this->unfreeze(tree))
                pmwcas_abort(mdesc);
            else
                return EPMWCASALLOC;
            return ENONEED;
        }

        //Try to freeze
        uint64_t status_sibling_new = status_frozen(status_sibling);
        pmwcas_add(mdesc, &sibling->status_, status_sibling_new, status_sibling, NOCAS_EXECUTE_ON_FAILED);
        int cas_res = tree->pack_pmwcas({{&sibling->status_, status_sibling, status_sibling_new}});
        if (!cas_res)
            break;
        print_log("MERGE-SIBLING_ERACE");
        if (this->unfreeze(tree))
            pmwcas_abort(mdesc);
        else
            return EPMWCASALLOC;
        return ERACE;
    }

    print_log("MERGE_BEGIN", NULL, sibling.rel());

    //Delete root
    if (parent.is_null()) {
        pmwcas_add(mdesc, &tree->root_, rel_ptr<bz_node<Key, Val>>(this).rel(), 0, RELEASE_EXP_ON_SUCCESS);
        if (!pmwcas_commit(mdesc))
            return ERACE;
        return 0;
    }

    //freeze old parent
    uint64_t status_parent_new = status_frozen(status_parent);
    pmwcas_add(mdesc, &parent->status_, status_parent, status_parent_new, 0);

    if (sibling_type) {
        /* Init N` */
        new_node_ptr = tree->alloc_node<Val>(mdesc, 0);
        rel_ptr<bz_node<Key, Val>> new_node = *new_node_ptr;

        uint32_t new_blk_sz = 0;
        uint32_t new_rec_cnt = 0;

        //Copy the metadata and KV of N and sibling
        this->copy_node_to(new_node);
        uint32_t tot_rec_cnt = sibling->copy_node_to(new_node);
        new_node->fr_sort_meta();

        //Init status and length
        persist(new_node.abs(), NODE_ALLOC_SIZE);
    }

    rel_ptr<uint64_t> this_node((uint64_t *) this);
    rel_ptr<uint64_t> sibling_addr(sibling);

    if (child_max > 2
        || child_max == 2 && BZ_KEY_MAX != *(uint64_t *) parent->nth_key(1)
        || new_node_ptr.is_null()) {
        //Keep parent

        /* Init P' BEGIN */
        new_parent_ptr = tree->alloc_node<uint64_t>(mdesc, 1);
        rel_ptr<bz_node<Key, uint64_t>> new_parent = *new_parent_ptr;

        uint32_t new_parent_rec_cnt = parent->copy_node_to(new_parent) - 1;
        int pos = sibling_type < 0 ? child_id - 1 : child_id;

        new_parent->fr_remove_meta(pos);
        if (sibling_type)
            *new_parent->nth_val(pos) = new_node_ptr->rel();
        set_sorted_count(new_parent->length_, new_parent_rec_cnt);
        persist(new_parent.abs(), NODE_ALLOC_SIZE);

        //pmwcas
        if (grandpa_ptr.is_null()) {
            pmwcas_add(mdesc, &tree->root_, parent.rel(), new_parent.rel(), RELEASE_EXP_ON_SUCCESS);
        } else {
            pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_parent.rel(), RELEASE_EXP_ON_SUCCESS);
            uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
            if (is_frozen(status_grandpa_rd)) {
                ret = EFROZEN;
                goto IMMEDIATE_ABORT;
            }
            pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
        }
    } else {
        //Delete parent

        if (grandpa_ptr.is_null()) {
            pmwcas_add(mdesc, &tree->root_, parent.rel(), new_node_ptr->rel(), RELEASE_EXP_ON_SUCCESS);
        } else {
            pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_node_ptr->rel(), RELEASE_EXP_ON_SUCCESS);
            uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
            if (is_frozen(status_grandpa_rd)) {
                ret = EFROZEN;
                goto IMMEDIATE_ABORT;
            }
            pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
        }
    }
    //Delete the current node and siblings
    pmwcas_add(mdesc, this_node, 0, 0, NOCAS_RELEASE_ADDR_ON_SUCCESS);
    if (sibling_type) {
        pmwcas_add(mdesc, sibling_addr, 0, 0, NOCAS_RELEASE_ADDR_ON_SUCCESS);
    }

    //Tigger pmwcas
    if (!pmwcas_commit(mdesc)) {
        ret = ERACE;
    }

    IMMEDIATE_ABORT:
    pmwcas_free(mdesc);

    print_log("MERGE_END", NULL, ret);
    if (!ret) {
        if (!new_node_ptr.is_null())
            (*new_node_ptr)->print_log("MERGE_NEW");
        if (!new_parent_ptr.is_null())
            (*new_parent_ptr)->print_log("MERGE_NEW_PARENT");
    }

    return ret;
}

/*
split
1. freeze the node (k1, k2]
2. scan all valid keys and find the seperator key K
3. allocate 3 new nodes:
3.1 new N' (K, k2]
3.2 N' sibling O (k1, K]
3.3 N' new parent P' (add new key record K and ptr to O)
4. 3-word PMwCAS
4.1 freeze P status
4.2 swap G's ptr to P to P'
4.3 G's status to detect conflicts
NOTES:
Of new nodes, N' and O are not taken care of.
So we need an extra PMwCAS mdesc to record those memories:
in case failure before memory transmition, pmwcas will help reclaim;
in case success, abort the pmwcas since it has been safe.
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::split(
        bz_tree<Key, TreeVal> *tree, rel_ptr<bz_node<Key, uint64_t>> parent,
        rel_ptr<uint64_t> grandpa_status, rel_ptr<uint64_t> grandpa_ptr) {
    /* Global Variables */
    uint64_t status_parent_rd;
    int ret = 0;

    mdesc_t mdesc = try_freeze<TreeVal>(tree);
    if (mdesc.is_null())
        return EFROZEN;

    print_log("SPLIT_BEGIN");

    /* Allocate N O P */
    rel_ptr<rel_ptr<bz_node<Key, Val>>> new_left_ptr = tree->alloc_node<Val>(mdesc, 0);
    rel_ptr<bz_node<Key, Val>> new_left = *new_left_ptr;

    /* Init N' and O Begin */
    //Copy meta to new_left and sort by keys
    uint32_t new_rec_cnt = this->copy_sort_meta_to(new_left);
    if (new_rec_cnt < 2) {
        pmwcas_free(mdesc);
        print_log("SPLIT-NEW_REC_CNT");
        return ENONEED;
    }

    rel_ptr<uint64_t> this_node_addr((uint64_t *) this);

    //Allocate to O and P'
    rel_ptr<rel_ptr<bz_node<Key, Val>>> new_right_ptr = tree->alloc_node<Val>(mdesc, 1);
    rel_ptr<bz_node<Key, Val>> new_right = *new_right_ptr;

    rel_ptr<rel_ptr<bz_node<Key, uint64_t>>> new_parent_ptr = tree->alloc_node<uint64_t>(mdesc, 2);
    rel_ptr<bz_node<Key, uint64_t>> new_parent = *new_parent_ptr;

    //Ensure balance based on count
    uint32_t left_rec_cnt = this->fr_get_balanced_count(new_left);
    //Ensure at least one to right node
    if (left_rec_cnt == new_rec_cnt) {
        --left_rec_cnt;
    }
    uint32_t right_rec_cnt = new_rec_cnt - left_rec_cnt;
    //Copy meta to new_right
    memcpy(new_right->rec_meta_arr(), new_left->rec_meta_arr() + left_rec_cnt,
           right_rec_cnt * sizeof(uint64_t));
    //Copy KV payload
    uint32_t left_blk_sz = this->copy_payload_to(new_left, left_rec_cnt);
    uint32_t right_blk_sz = this->copy_payload_to(new_right, right_rec_cnt);
    //Init status and length
    this->init_header(new_left, left_rec_cnt, left_blk_sz);
    this->init_header(new_right, right_rec_cnt, right_blk_sz);
    //Persistence
    persist(new_left.abs(), NODE_ALLOC_SIZE);
    persist(new_right.abs(), NODE_ALLOC_SIZE);
    /* Init N'and O END */

    /* Init P' BEGIN */
    //Generate split point K
    uint64_t meta_key = new_left->rec_meta_arr()[left_rec_cnt - 1];
    const Key *K = new_left->get_key(meta_key);
    uint32_t key_sz = get_key_length(meta_key);
    uint32_t tot_sz = key_sz + sizeof(uint64_t);
    uint64_t V = new_left.rel();
    uint64_t *parent_meta_arr = new_parent->rec_meta_arr();

    if (!parent.is_null()) {
        /* If the current node is not root */
        status_parent_rd = pmwcas_read(&parent->status_);
        if (is_frozen(status_parent_rd)) {
            ret = EFROZEN;
            goto IMMEDIATE_ABORT;
        }

        uint32_t new_parent_rec_cnt = parent->copy_node_to(new_parent, status_parent_rd);
        if (ret = new_parent->fr_insert_meta(K, V, key_sz, new_right.rel()))
            goto IMMEDIATE_ABORT;
        //Persistence
        persist(new_parent.abs(), NODE_ALLOC_SIZE);
    } else {
        /* For root */
        if (ret = new_parent->fr_root_init(K, V, key_sz, new_right.rel()))
            goto IMMEDIATE_ABORT;
        persist(new_parent.abs(), NODE_ALLOC_SIZE);
    }
    /* Init P' END */

    /* 3-word pmwcas */
    if (!grandpa_ptr.is_null()) {
        /* Exist an ancestor */
        //3.1 G's ptr to P -> P'
        pmwcas_add(mdesc, grandpa_ptr, parent.rel(), new_parent.rel(), RELEASE_EXP_ON_SUCCESS);
        //3.2 freeze P's status
        uint64_t status_parent_new = status_frozen(status_parent_rd);

        pmwcas_add(mdesc, &parent->status_, status_parent_rd, status_parent_new);
        //3.3 make sure G's status is not frozen
        uint64_t status_grandpa_rd = pmwcas_read(grandpa_status.abs());
        if (is_frozen(status_grandpa_rd)) {
            ret = EFROZEN;
            goto IMMEDIATE_ABORT;
        }
        pmwcas_add(mdesc, grandpa_status, status_grandpa_rd, status_grandpa_rd);
        pmwcas_add(mdesc, this_node_addr, 0, 0, NOCAS_RELEASE_ADDR_ON_SUCCESS);
    } else if (!parent.is_null()) {
        /* Parent is root */
        //3.1 root's ptr to P -> P'
        pmwcas_add(mdesc, &tree->root_, parent.rel(), new_parent.rel(), RELEASE_EXP_ON_SUCCESS);
        //3.2 freeze P's status
        uint64_t status_parent_new = status_frozen(status_parent_rd);
        pmwcas_add(mdesc, &parent->status_, status_parent_rd, status_parent_new);
        pmwcas_add(mdesc, this_node_addr, 0, 0, NOCAS_RELEASE_ADDR_ON_SUCCESS);
    } else {
        /* Current node is root */
        rel_ptr<bz_node<Key, Val>> cur_ptr = this;
        pmwcas_add(mdesc, &tree->root_, cur_ptr.rel(), new_parent.rel(), RELEASE_EXP_ON_SUCCESS);
    }

    //Trigger pmwcas
    if (!pmwcas_commit(mdesc)) {
        ret = ERACE;
    }

    IMMEDIATE_ABORT:
    pmwcas_free(mdesc);

    print_log("SPLIT_END", NULL, ret);
    if (!ret) {
        string msg = "SPLIT";
        if (!grandpa_ptr.is_null())
            msg += "_3_";
        else if (!parent.is_null())
            msg += "_2_";
        else
            msg += "_1_";
        new_left->print_log(string(msg + "NEW_LEFT").c_str(), nullptr, left_rec_cnt);
        new_right->print_log(string(msg + "NEW_RIGHT").c_str(), nullptr, right_rec_cnt);
        new_parent->print_log(string(msg + "NEW_PARENT").c_str());
    }

    return ret;
}

/*
consolidate:
trigger by
either free space too small or deleted space too large
1) single-word PMwCAS on status => frozen bit
2) scan all valid records(visiable and not deleted)
calculate node size = record size + free space
2.1) if node size too large, do a split
2.2) otherwise, allocate a new node N', copy records sorted, init header
3) use path stack to find Node's parent P
3.1) if P's frozen, retraverse to locate P
4) 2-word PMwCAS
r in P that points to N => N'
P's status => detect concurrent freeze
5) N is ready for gc
6)
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::consolidate(bz_tree<Key, TreeVal> *tree,
                                   rel_ptr<uint64_t> parent_status, rel_ptr<uint64_t> parent_ptr) {
    int ret = 0;

    mdesc_t mdesc = try_freeze<TreeVal>(tree);
    if (mdesc.is_null())
        return EFROZEN;

    print_log("CONSOLIDATE_BEGIN");

    //Init node to be 0
    rel_ptr<rel_ptr<bz_node<Key, Val>>> node_ptr = tree->alloc_node<Val>(mdesc);
    rel_ptr<bz_node<Key, Val>> node = *node_ptr;
    this->copy_node_to(node);
    node->fr_sort_meta();
    //Persistence
    persist(node.abs(), NODE_ALLOC_SIZE);

    rel_ptr<bz_node<Key, Val>> this_node(this);

    //Ensure Frozen != 0 before changing parent
    if (!parent_ptr.is_null()) {
        uint64_t status_rd = pmwcas_read(parent_status.abs());
        if (is_frozen(status_rd)) {
            ret = EFROZEN;
            goto IMMEDIATE_ABORT;
        }
        pmwcas_add(mdesc, parent_status, status_rd, status_rd, 0);
        pmwcas_add(mdesc, parent_ptr, this_node.rel(), node.rel(), RELEASE_EXP_ON_SUCCESS);
    } else {
        pmwcas_add(mdesc, &tree->root_, this_node.rel(), node.rel(), RELEASE_EXP_ON_SUCCESS);
    }

    //Trigger pmwcas
    if (!pmwcas_commit(mdesc))
        ret = ERACE;

    IMMEDIATE_ABORT:
    pmwcas_free(mdesc);

    print_log("CONSOLIDATE_END", NULL, ret);
    if (!ret) {
        node->print_log("CONSOLIDATE_NEW");
    }

    return ret;
}

/* Count the nodes when deciding split points using SINGLE THREAD */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::fr_get_balanced_count(rel_ptr<bz_node<Key, Val>> dst) {
    uint32_t old_blk_sz = valid_block_size();
    uint64_t *meta_arr = dst->rec_meta_arr();
    uint32_t left_rec_cnt = 0;
    for (uint32_t i = 0, acc_sz = 0; acc_sz < old_blk_sz / 2; ++i) {
        if (is_visiable(meta_arr[i])) {
            ++left_rec_cnt;
            acc_sz += get_total_length(meta_arr[i]);
        }
    }
    return left_rec_cnt;
}


template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_node_to(rel_ptr<bz_node<Key, Val>> dst, uint64_t status_rd) {
    if (!status_rd)
        status_rd = pmwcas_read(&status_);
    uint64_t *meta_arr = rec_meta_arr();
    uint64_t *new_meta_arr = dst->rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_rd);
    uint32_t new_rec_cnt = get_record_count(dst->status_);
    uint32_t new_blk_sz = get_block_size(dst->status_);
    uint32_t new_node_sz = get_node_size(dst->length_);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd)) {
            const Key *key = get_key(meta_rd);
            const Val *val = get_value(meta_rd);

            //assert(*(uint64_t*)key < 65 || *(uint64_t*)key == BZ_KEY_MAX);
            uint64_t tmp = *(uint64_t *) val;
            if ((tmp & MwCAS_BIT || tmp & RDCSS_BIT || tmp & DIRTY_BIT)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                uint64_t tt = *(uint64_t *) val;
                assert(0);
            }

            uint32_t key_sz = get_key_length(meta_rd);
            uint32_t tot_sz = get_total_length(meta_rd);
            uint32_t offset = new_node_sz - new_blk_sz - tot_sz - 1;
            new_meta_arr[new_rec_cnt] = meta_vis_off_klen_tlen(0, true, offset, key_sz, tot_sz);
            dst->copy_data(offset, key, val, key_sz, tot_sz);
            new_blk_sz += tot_sz;
            ++new_rec_cnt;
        }
    }
    set_record_count(dst->status_, new_rec_cnt);
    set_block_size(dst->status_, new_blk_sz);
    if (is_leaf(length_)) {
        set_leaf(dst->length_);
    } else {
        set_non_leaf(dst->length_);
    }
    return new_rec_cnt;
}

template<typename Key, typename Val>
void bz_node<Key, Val>::fr_sort_meta() {
    uint64_t tot_rec_cnt = get_record_count(status_);
    uint64_t *new_meta_arr = rec_meta_arr();
    std::sort(new_meta_arr, new_meta_arr + tot_rec_cnt,
              std::bind(&bz_node<Key, Val>::key_cmp_meta, this, std::placeholders::_1, std::placeholders::_2));
    //Compute the effective nodes
    uint32_t new_rec_cnt = binary_search(nullptr, (int) tot_rec_cnt);
    set_sorted_count(length_, new_rec_cnt);
    set_record_count(status_, new_rec_cnt);
}

template<typename Key, typename Val>
inline void bz_node<Key, Val>::fr_remove_meta(int pos) {
    uint64_t *parent_meta_arr = rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_);
    uint32_t dele_sz = get_delete_size(status_);
    memmove(parent_meta_arr + pos, parent_meta_arr + pos + 1, (rec_cnt - 1 - pos) * sizeof(uint64_t));
    //Increase delete size, decrease rec_cnt
    set_record_count(status_, rec_cnt - 1);
    set_delete_size(status_, dele_sz + get_total_length(parent_meta_arr[pos]));
}

template<typename Key, typename Val>
inline int bz_node<Key, Val>::fr_insert_meta(const Key *K, uint64_t left, uint32_t key_sz, uint64_t right) {
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t tot_sz = key_sz + sizeof(uint64_t);
    uint32_t rec_cnt = get_record_count(status_);
    uint32_t blk_sz = get_block_size(status_);
    uint32_t node_sz = get_node_size(length_);
    uint32_t pos = binary_search(K, rec_cnt);
    if ((rec_cnt + 2) * sizeof(uint64_t) + key_sz + blk_sz + sizeof(*this) > node_sz) {
        return EALLOCSIZE;
    }
    memmove(meta_arr + pos + 1, meta_arr + pos, sizeof(uint64_t) * (rec_cnt - pos));
    uint32_t key_offset = node_sz - blk_sz - tot_sz - 1;
    meta_arr[pos] = meta_vis_off_klen_tlen(0, true, key_offset, key_sz, tot_sz);
    this->copy_data(key_offset, K, &left, key_sz, tot_sz);
    //Modify original node to N, and redirect it to new_right
    *get_value(meta_arr[pos + 1]) = right;
    assert(!(right & MwCAS_BIT || right & RDCSS_BIT || right & DIRTY_BIT));
    set_record_count(status_, rec_cnt + 1);
    set_sorted_count(length_, rec_cnt + 1);
    set_block_size(status_, blk_sz + tot_sz);
    return 0;
}

template<typename Key, typename Val>
int bz_node<Key, Val>::fr_root_init(const Key *K, uint64_t left, uint32_t key_sz, uint64_t right) {
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t node_sz = get_node_size(length_);
    uint32_t tot_sz = key_sz + sizeof(uint64_t);
    if (node_sz < sizeof(*this) + sizeof(uint64_t) * 2 - tot_sz * 2)
        return EALLOCSIZE;

    uint32_t left_key_offset = node_sz - tot_sz - 1;
    meta_arr[0] = meta_vis_off_klen_tlen(0, true, left_key_offset, key_sz, tot_sz);
    copy_data(left_key_offset, K, &left, key_sz, tot_sz);

    //Insert <BZ_KEY_MAX, new_right> into P'
    uint32_t right_key_offset = left_key_offset - sizeof(uint64_t) * 2;
    meta_arr[1] = meta_vis_off_klen_tlen(0, true, right_key_offset, sizeof(uint64_t), sizeof(uint64_t) * 2);
    *(uint64_t *) get_key(meta_arr[1]) = BZ_KEY_MAX;
    *(uint64_t *) get_value(meta_arr[1]) = right;

    //Init status and length
    set_record_count(status_, 2);
    set_block_size(status_, tot_sz + sizeof(uint64_t) * 2);
    set_sorted_count(length_, 2);
    set_non_leaf(length_);
    return 0;
}


/* Copy meta from the current node to dst node, and sort by keys and visibility, return the current count */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_sort_meta_to(rel_ptr<bz_node<Key, Val>> dst) {
    //Copy meta and sort by keys
    uint64_t *new_meta_arr = dst->rec_meta_arr();
    uint64_t *old_meta_arr = rec_meta_arr();
    uint64_t status_rd = pmwcas_read(&status_);
    uint64_t rec_cnt = get_record_count(status_rd);
    uint32_t new_rec_cnt = get_record_count(dst->status_);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&old_meta_arr[i]);
        if (is_visiable(meta_rd)) {
            new_meta_arr[new_rec_cnt] = meta_rd;
            ++new_rec_cnt;
        }
    }
    std::sort(new_meta_arr, new_meta_arr + new_rec_cnt,
              std::bind(&bz_node<Key, Val>::key_cmp_meta, this, std::placeholders::_1, std::placeholders::_2));
    //Compute effective meta number
    return this->binary_search(nullptr, new_rec_cnt, new_meta_arr);
    //Empty the resting part
    //memset(new_meta_arr + new_rec_cnt, 0, (rec_cnt - new_rec_cnt) * sizeof(uint64_t));
    //return new_rec_cnt;
}

/* Copy KV, and return the new block size */
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::copy_payload_to(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt) {
    uint32_t blk_sz = 0;
    uint32_t node_sz = NODE_ALLOC_SIZE;
    uint64_t *new_meta_arr = dst->rec_meta_arr();
    for (uint32_t i = 0; i < new_rec_cnt; ++i) {
        const Key *key = get_key(new_meta_arr[i]);
        const Val *val = get_value(new_meta_arr[i]);

        //assert(*(uint64_t*)key < 65 || *(uint64_t*)key == BZ_KEY_MAX);
        uint64_t tmp = *(uint64_t *) val;
        assert(!(tmp & MwCAS_BIT || tmp & DIRTY_BIT || tmp & RDCSS_BIT));

        uint32_t key_sz = get_key_length(new_meta_arr[i]);
        uint32_t tot_sz = get_total_length(new_meta_arr[i]);
        uint32_t new_offset = node_sz - blk_sz - tot_sz - 1;
        new_meta_arr[i] = meta_vis_off_klen_tlen(0, true, new_offset, key_sz, tot_sz);
        dst->copy_data(new_offset, key, val, key_sz, tot_sz);
        blk_sz += tot_sz;
    }
    return blk_sz;
}

/* Init status and length based on parameters, using SINGLE THREAD */
template<typename Key, typename Val>
void bz_node<Key, Val>::init_header(rel_ptr<bz_node<Key, Val>> dst, uint32_t new_rec_cnt, uint32_t blk_sz, int leaf_opt,
                                    uint32_t dele_sz) {
    dst->status_ = 0ULL;
    set_record_count(dst->status_, new_rec_cnt);
    set_block_size(dst->status_, blk_sz);
    set_delete_size(dst->status_, dele_sz);
    if (!leaf_opt) {
        if (is_leaf(length_))
            set_leaf(dst->length_);
        else
            set_non_leaf(dst->length_);
    } else if (leaf_opt == BZ_TYPE_LEAF)
        set_leaf(dst->length_);
    else
        set_non_leaf(dst->length_);
    set_node_size(dst->length_, NODE_ALLOC_SIZE);
    set_sorted_count(dst->length_, new_rec_cnt);
}

template<typename Key, typename Val>
inline uint32_t bz_node<Key, Val>::valid_block_size(uint64_t status_rd) {
    if (!status_rd) {
        status_rd = pmwcas_read(&status_);
    }
    uint32_t tot_sz = 0;
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_rd);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd))
            tot_sz += get_total_length(meta_rd);
    }
    return tot_sz;
}

template<typename Key, typename Val>
inline uint32_t bz_node<Key, Val>::valid_node_size(uint64_t status_rd) {
    if (!status_rd) {
        status_rd = pmwcas_read(&status_);
    }
    uint32_t tot_sz = 0;
    uint32_t tot_rec_cnt = 0;
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_rd);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd)) {
            tot_sz += get_total_length(meta_rd);
            ++tot_rec_cnt;
        }
    }
    return sizeof(*this) + sizeof(uint64_t) * tot_rec_cnt + tot_sz;
}

template<typename Key, typename Val>
inline uint32_t bz_node<Key, Val>::valid_record_count(uint64_t status_rd) {
    if (!status_rd) {
        status_rd = pmwcas_read(&status_);
    }
    uint32_t tot_rec_cnt = 0;
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_rd);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd)) {
            ++tot_rec_cnt;
        }
    }
    return tot_rec_cnt;
}

template<typename Key, typename Val>
template<typename TreeVal>
mdesc_t bz_node<Key, Val>::try_freeze(bz_tree<Key, TreeVal> *tree) {
    while (true) {
        uint64_t status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return mdesc_t::null();
        uint64_t status_new = status_frozen(status_rd);
        mdesc_t mdesc = tree->alloc_mdesc();
        if (mdesc.is_null())
            return mdesc_t::null();
        pmwcas_add(mdesc, &status_, status_new, status_rd, NOCAS_EXECUTE_ON_FAILED);
        int ret = tree->pack_pmwcas({{&status_, status_rd, status_new}});
        if (!ret)
            return mdesc;
        pmwcas_abort(mdesc);
    }
}

template<typename Key, typename Val>
template<typename TreeVal>
inline bool bz_node<Key, Val>::unfreeze(bz_tree<Key, TreeVal> *tree) {
    uint64_t status_rd = pmwcas_read(&status_);
    uint64_t status_unfrozen = status_rd;
    unset_frozen(status_unfrozen);
    return !tree->pack_pmwcas({{&status_, status_rd, status_unfrozen}});
}

template<typename Key, typename Val>
inline rel_ptr<uint64_t> bz_node<Key, Val>::nth_child(int n) {
    if (n < 0)
        return rel_ptr<uint64_t>::null();
    uint64_t addr = *nth_val(n);
    return rel_ptr<uint64_t>(*nth_val(n));
}

template<typename Key, typename Val>
inline Key *bz_node<Key, Val>::nth_key(int n) {
    if (n < 0)
        return nullptr;
    return get_key(pmwcas_read(rec_meta_arr() + n));
}

template<typename Key, typename Val>
inline Val *bz_node<Key, Val>::nth_val(int n) {
    if (n < 0)
        return nullptr;
    return get_value(pmwcas_read(rec_meta_arr() + n));
}

template<typename Key, typename Val>
void bz_node<Key, Val>::print_log(const char *action, const Key *k, uint64_t ret, bool pr) {
    if (!pr)
        return;
    mylock.lock();
    fs << "<" << hex << rel_ptr<bz_node<Key, Val>>(this).rel() << "> ";
    if (typeid(Key) == typeid(char)) {
        fs << "[" << this_thread::get_id() << "] " << " ";
        fs << action;
        if (k)
            fs << " key " << k;
        if (ret != -1)
            fs << " : " << ret;
        fs << endl;
    } else {
        fs << "[" << this_thread::get_id() << "] ";
        fs << action;
        if (k)
            fs << " key " << *k;
        if (ret != -1)
            fs << " : " << ret;
        fs << endl;
    }
    mylock.unlock();
}

template<typename Key, typename Val>
void bz_tree<Key, Val>::print_dfs(uint64_t ptr, int level) {
    if (!ptr)
        return;
    bool isLeaf = is_leaf_node(ptr);
    rel_ptr<bz_node<Key, Val>> node(ptr);
    uint64_t *meta_arr = node->rec_meta_arr();
    uint32_t rec_cnt = get_record_count(node->status_);
    fs << hex << ptr;
    for (int i = 0; i < level; ++i)
        fs << "-";
    fs << std::setfill('0') << std::hex << std::setw(16) << node->status_ << " ";
    fs << std::setfill('0') << std::hex << std::setw(16) << node->length_ << " : ";
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        if (!is_visiable(meta_arr[i]))
            continue;
        if (isLeaf) {
            fs << "(";
            if (typeid(Key) == typeid(char))
                fs << (char *) node->get_key(meta_arr[i]);
            else
                fs << *node->get_key(meta_arr[i]);
            fs << ",";
            if (typeid(Val) == typeid(rel_ptr<char>))
                fs << (char *) (*node->get_value(meta_arr[i])).abs();
            else {
                rel_ptr<uint64_t> *p = node->get_value(meta_arr[i]);
                rel_ptr<uint64_t> pp = *p;
                if (pp.rel() & MwCAS_BIT) {
                    assert(0);
                }
                uint64_t val = *pp;
                fs << val;
            }
            fs << ") ";
        } else if (*(uint64_t *) node->get_key(meta_arr[i]) != BZ_KEY_MAX) {
            if (typeid(Key) == typeid(char))
                fs << (char *) node->get_key(meta_arr[i]) << " ";
            else
                fs << *node->get_key(meta_arr[i]) << " ";
        } else {
            fs << "KEY_MAX";
        }
    }
    fs << "\n";
    if (!isLeaf) {
        for (uint32_t i = 0; i < rec_cnt; ++i) {
            print_dfs(*(uint64_t *) node->get_value(meta_arr[i]), level + 1);
        }
    }
}

template<typename Key, typename Val>
inline mdesc_t bz_tree<Key, Val>::alloc_mdesc(int recycle) {
    int max_retry = 100;
    int retry = 0;
    mdesc_t mdesc = pmwcas_alloc(&pool_, recycle);
    while (mdesc.is_null() && retry < max_retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(++retry));
        mdesc = pmwcas_alloc(&pool_, recycle);
    }
    return mdesc;
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::recycle_node(rel_ptr<rel_ptr<uint64_t>> ptr) {
    pmwcas_word_recycle(&pool_, ptr);
}

template<typename Key, typename Val>
inline void bz_tree<Key, Val>::print_node(uint64_t ptr, int extra) {
    mylock.lock();
    fs << "extra " << ptr << " [" << this_thread::get_id() << "] ";
    rel_ptr<bz_node<Key, Val>> node(ptr);
    uint64_t status_rd = pmwcas_read(&node->status_);
    uint64_t length = node->length_;
    fs << std::setfill('0') << std::hex << std::setw(16) << status_rd << " ";
    fs << std::setfill('0') << std::hex << std::setw(16) << length << "\n";
    uint64_t *meta_arr = node->rec_meta_arr();
    uint32_t rec_cnt = get_record_count(status_rd);
    for (uint32_t i = 0; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        fs << meta_rd << " ";
        if (is_visiable(meta_rd)) {
            const Key *key = node->get_key(meta_rd);
            if (*(uint64_t *) key != BZ_KEY_MAX) {
                if (typeid(Key) == typeid(char))
                    fs << (char *) key << " ";
                else
                    fs << *key << " ";
            } else {
                fs << "KEY_MAX";
            }
        }
        fs << "\n";
    }
    fs << "\n";
    mylock.unlock();
}

/* Print tree using SINGLE THREAD */
template<typename Key, typename Val>
void bz_tree<Key, Val>::print_tree(bool pr) {
    if (!pr)
        return;
    mylock.lock();
    print_dfs(root_, 1);
    fs << "\n\n";
    mylock.unlock();
}

/*
insert:
1) writer tests Frozen Bit, retraverse if failed
to avoid duplicate keys:
2) scan the sorted keys, fail if found a duplicate key
3) scan the unsorted keys
3.2) fail if meet a duplicate key
3.1) if meet a meta entry whose visiable unset and epoch = current global epoch,
set Retry bit and continue
4) reserve space in meta entry and block
by adding one to record count and adding data length to node block size(both in node status)
and flipping meta entry offset's high bit and setting the rest bits to current index global epoch
4.1) in case fail => concurrent thread may be trying to insert duplicate key,
so need to set Recheck flag
5) unset visiable bit and copy data to block
6) persist
6.5) re-scan prior positions if Recheck is set
6.5.1) if find a duplicate key, set offset and block = 0
return fail
7) 2-word PMwCAS:
set status back => Frozen Bit isn't set
set meta entry to visiable and correct offset
7.1) if fails and Frozen bit is not set, read status and retry
7.2) if Frozen bit is set, abort and retry the insert
*/
/* Trigger inserting leaf nodes */
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::insert(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size,
                              uint32_t total_size, uint32_t alloc_epoch) {
    /* Global variables */
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t node_sz = get_node_size(length_);
    uint32_t sorted_cnt = get_sorted_count(length_);
    bool recheck = false;
    uint64_t meta_new;
    uint32_t rec_cnt, blk_sz;

    /* UNIKEY Search duplicated keys in sorted part */
    uint32_t pos;
    if (find_key_sorted(key, pos))
        return EUNIKEY;

    uint64_t status_rd = pmwcas_read(&status_);
    if (is_frozen(status_rd))
        return EFROZEN;
    /* UNIKEY Search duplicated keys in unsorted part */
    if (find_key_unsorted(key, status_rd, alloc_epoch, pos, recheck))
        return EUNIKEY;

    while (true) {
        rec_cnt = get_record_count(status_rd);
        blk_sz = get_block_size(status_rd);

        /* Enough space */
        if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
            return EALLOCSIZE;

        /* Increase record count and block size */
        uint64_t status_new = status_add_rec_blk(status_rd, total_size);

        /* Set offset to be alloc_epoch, unset visiable bit */
        uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
        meta_new = meta_vis_off(0, false, alloc_epoch);

        /* 2-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,           status_rd, status_new},
                                                {&meta_arr[rec_cnt], meta_rd,   meta_new}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            print_log("IS-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        recheck = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return EFROZEN;
    }

    print_log("IS-pos", key, rec_cnt);

    /* copy key and val */
    uint32_t new_offset = node_sz - blk_sz - total_size - 1;
    copy_data(new_offset, key, val, key_size, total_size);

    if (recheck) {
        int ret = rescan_unsorted(tree, sorted_cnt, rec_cnt, key, total_size, alloc_epoch);
        if (ret)
            return ret;

        print_log("IS-recheck", key, rec_cnt);
    }

    /* set visiable; real offset; key_len and tot_len */
    uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

    while (true) {
        uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
        assert(meta_new_rd == meta_new);

        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd)) {
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            return EFROZEN;
        }

        /* 2-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,           status_rd, status_rd},
                                                {&meta_arr[rec_cnt], meta_new,  meta_new_plus}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            //Increase dele_sz
            if (!add_dele_sz(tree, total_size))
                return EFROZEN;
            print_log("IS-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    print_log("IS-finish", key, rec_cnt);
    return 0;
}
/*
delete:
1) 2-word PMwCAS on
meta entry: visiable = 0, offset = 0
node status: delete size += data length
1.1) if fails due to Frozen bit set, abort and retraverse
1.2) otherwise, read and retry
*/
/* Delete leaf */
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::remove(bz_tree<Key, TreeVal> *tree, const Key *key) {
    /* Global variables */
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t pos;

    /* check Frozen */
    uint64_t status_rd = pmwcas_read(&status_);
    uint32_t rec_cnt = get_record_count(status_rd);
    if (is_frozen(status_rd))
        return EFROZEN;

    /* Search target key */
    bool useless;
    if (!find_key_sorted(key, pos) && !find_key_unsorted(key, status_rd, 0, pos, useless))
        return ENOTFOUND;

    print_log("RM-pos", key, pos);

    while (true) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
        if (!is_visiable(meta_rd)) {
            /* Delete in case of contention */
            return ENOTFOUND;
        }

        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return EFROZEN;

        /* Increase delete size */
        uint64_t status_new = status_del(status_rd, get_total_length(meta_rd));

        /* unset visible' offset=0*/
        uint64_t meta_new = meta_vis_off(meta_rd, false, 0);

        /* 2-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,       status_rd, status_new},
                                                {&meta_arr[pos], meta_rd,   meta_new}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            print_log("RM-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    print_log("RM-finish", key, 0);

    return 0;
}
/*
update
1) writer tests Frozen Bit, retraverse if failed
to find the target key:
2) scan the sorted keys, store the positon if find the target key
2.1) if not found, scan the unsorted keys, fail if cannot find, otherwise store the pos
2.2) if meet a meta entry whose visiable unset and epoch = current global epoch,
set Retry bit and continue
3.1) reserve space in meta entry and block
by adding one to record count and adding data length to node block size(both in node status)
and setting the offset to current index global epoch and unset visual
3.2) in case fail => concurrent thread may be trying to insert duplicate key,
so need to set Recheck flag. and keep reading status, and perform pmwcas

4) copy data to block
5) persist
6) re-scan the area (key_pos, rec_cnt) if Recheck is set
6.1) if find a duplicate key, set offset and block = 0
return fail
7) 3-word PMwCAS:
set status back => Frozen Bit isn't set
set updated meta entry to visiable and correct offset
set origin meta entry to unvisualable and offset = 0
in case fail:
7.1) Frozen set => return fail
7.2) origin meta entry not visual => return fail
otherwise, read origin meta and status, retry pmwcas
*/
/* Update leaf */
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::update(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size,
                              uint32_t total_size, uint32_t alloc_epoch) {
    /* Global variables */
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t node_sz = get_node_size(length_);
    uint32_t sorted_cnt = get_sorted_count(length_);
    bool recheck = false;
    uint64_t meta_new;
    uint32_t rec_cnt, blk_sz;

    /* not Frozen */
    uint64_t status_rd = pmwcas_read(&status_);
    if (is_frozen(status_rd))
        return EFROZEN;

    /* Search target key */
    uint32_t del_pos;
    if (!find_key_sorted(key, del_pos)
        && !find_key_unsorted(key, status_rd, alloc_epoch, del_pos, recheck))
        return ENOTFOUND;

    print_log("UP-find", key, del_pos);

    while (true) {
        rec_cnt = get_record_count(status_rd);
        blk_sz = get_block_size(status_rd);

        /* Enough space */
        if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
            return EALLOCSIZE;

        /* Increase record count and block size */
        uint64_t status_new = status_add_rec_blk(status_rd, total_size);

        /* Set offset to be alloc_epoch, unset visiable bit */
        uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
        meta_new = meta_vis_off(0, false, alloc_epoch);

        /* 2-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,           status_rd, status_new},
                                                {&meta_arr[rec_cnt], meta_rd,   meta_new}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            print_log("UP-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        recheck = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return EFROZEN;
    }

    print_log("UP-pos", key, rec_cnt);

    /* copy key and val */
    uint32_t new_offset = node_sz - blk_sz - total_size - 1;
    copy_data(new_offset, key, val, key_size, total_size);

    if (recheck) {
        int ret = rescan_unsorted(tree, del_pos + 1, rec_cnt, key, total_size, alloc_epoch);
        if (ret)
            return ret;

        print_log("UP-recheck", key, rec_cnt);
    }

    /* set visiable; real offset; key_len and tot_len */
    uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

    while (true) {
        uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
        assert(meta_new_rd == meta_new);

        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd)) {
            /* frozen set */
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            return EFROZEN;
        }
        uint64_t meta_del = pmwcas_read(&meta_arr[del_pos]);
        if (!is_visiable(meta_del)) {
            /* Delete or update in case of contention, and free space */
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            if (!add_dele_sz(tree, total_size))
                return EFROZEN;
            return ERACE;
        }
        uint64_t status_new = status_del(status_rd, get_total_length(meta_del));
        uint64_t meta_del_new = meta_vis_off(meta_del, false, 0);
        /* 3-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,           status_rd, status_new},
                                                {&meta_arr[rec_cnt], meta_new,  meta_new_plus},
                                                {&meta_arr[del_pos], meta_del,  meta_del_new}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            if (!add_dele_sz(tree, total_size))
                return EFROZEN;
            print_log("UP-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    print_log("UP-finish", key, rec_cnt);
    return 0;
}

/*
read
1) binary search on sorted keys
2) linear scan on unsorted keys
3) return the record found
*/
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::read(bz_tree<Key, TreeVal> *tree, const Key *key, Val *val, uint32_t max_val_size) {
    /* Global variables */
    uint32_t pos;

    /* Search target key */
    uint64_t status_rd = pmwcas_read(&status_);
    bool useless;
    if (!find_key_sorted(key, pos) && !find_key_unsorted(key, status_rd, 0, pos, useless))
        return ENOTFOUND;

    uint64_t *meta_arr = rec_meta_arr();
    uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
    if (get_total_length(meta_rd) - get_key_length(meta_rd) > max_val_size)
        return ENOSPACE;

    copy_value(val, get_value(meta_rd));
    return 0;
}

/* If exists, then update; otherwise, insert */
/* Upsert */
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::upsert(bz_tree<Key, TreeVal> *tree, const Key *key, const Val *val, uint32_t key_size,
                              uint32_t total_size, uint32_t alloc_epoch) {
    /* Global variables */
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t node_sz = get_node_size(length_);
    uint32_t sorted_cnt = get_sorted_count(length_);
    bool recheck = false, found = false;
    uint64_t meta_new;
    uint32_t rec_cnt, blk_sz;

    /* not Frozen */
    uint64_t status_rd = pmwcas_read(&status_);
    if (is_frozen(status_rd))
        return EFROZEN;

    /* Search target key */
    uint32_t del_pos;
    if (find_key_sorted(key, del_pos)
        || find_key_unsorted(key, status_rd, alloc_epoch, del_pos, recheck))
        found = true;

    print_log("US-find", key, del_pos);

    while (true) {
        rec_cnt = get_record_count(status_rd);
        blk_sz = get_block_size(status_rd);

        /* Enough space */
        if (blk_sz + total_size + sizeof(uint64_t) * (1 + rec_cnt) + sizeof(*this) > node_sz)
            return EALLOCSIZE;

        /* Increase record count and block size */
        uint64_t status_new = status_add_rec_blk(status_rd, total_size);

        /* Set offset to be alloc_epoch, unset visiable bit */
        uint64_t meta_rd = pmwcas_read(&meta_arr[rec_cnt]);
        meta_new = meta_vis_off(0, false, alloc_epoch);

        /* 2-word PMwCAS */
        int cas_res = tree->pack_pmwcas({
                                                {&status_,           status_rd, status_new},
                                                {&meta_arr[rec_cnt], meta_rd,   meta_new}
                                        });
        if (!cas_res)
            break;
        if (EPMWCASALLOC == cas_res) {
            print_log("US-EPMWCASALLOC");
            return EPMWCASALLOC;
        }

        recheck = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return EFROZEN;
    }

    print_log("US-pos", key, rec_cnt);

    /* copy key and val */
    uint32_t new_offset = node_sz - blk_sz - total_size - 1;
    copy_data(new_offset, key, val, key_size, total_size);

    if (recheck) {
        uint32_t beg_pos = found ? del_pos + 1 : sorted_cnt;
        int ret = rescan_unsorted(tree, beg_pos, rec_cnt, key, total_size, alloc_epoch);
        if (ret)
            return ret;

        print_log("US-recheck", key, rec_cnt);

    }

    /* set visiable; real offset; key_len and tot_len */
    uint64_t meta_new_plus = meta_vis_off_klen_tlen(0, true, new_offset, key_size, total_size);

    while (true) {
        uint64_t meta_new_rd = pmwcas_read(&meta_arr[rec_cnt]);
        assert(meta_new_rd == meta_new);

        status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd)) {
            /* frozen set */
            set_offset(meta_arr[rec_cnt], 0);
            persist(&meta_arr[rec_cnt], sizeof(uint64_t));
            return EFROZEN;
        }

        if (found) {
            /* Trigger update */
            uint64_t meta_del = pmwcas_read(&meta_arr[del_pos]);
            if (!is_visiable(meta_del)) {

                print_log("US-RACE", key, del_pos);

                /* Delete or update in case of contention, and free space*/
                set_offset(meta_arr[rec_cnt], 0);
                persist(&meta_arr[rec_cnt], sizeof(uint64_t));
                if (!add_dele_sz(tree, total_size))
                    return EFROZEN;
                return ERACE;
            }
            uint64_t status_new = status_del(status_rd, get_total_length(meta_del));
            uint64_t meta_del_new = meta_vis_off(meta_del, false, 0);
            /* 3-word PMwCAS */
            int cas_res = tree->pack_pmwcas({
                                                    {&status_,           status_rd, status_new},
                                                    {&meta_arr[rec_cnt], meta_new,  meta_new_plus},
                                                    {&meta_arr[del_pos], meta_del,  meta_del_new}
                                            });
            if (!cas_res)
                break;
            if (EPMWCASALLOC == cas_res) {
                set_offset(meta_arr[rec_cnt], 0);
                persist(&meta_arr[rec_cnt], sizeof(uint64_t));
                if (!add_dele_sz(tree, total_size))
                    return EFROZEN;
                print_log("US-EPMWCASALLOC");
                return EPMWCASALLOC;
            }

        } else {
            /* Trigger insert */
            /* 2-word PMwCAS */
            int cas_res = tree->pack_pmwcas({
                                                    {&status_,           status_rd, status_rd},
                                                    {&meta_arr[rec_cnt], meta_new,  meta_new_plus}
                                            });
            if (!cas_res)
                break;
            if (EPMWCASALLOC == cas_res) {
                set_offset(meta_arr[rec_cnt], 0);
                persist(&meta_arr[rec_cnt], sizeof(uint64_t));
                if (!add_dele_sz(tree, total_size))
                    return EFROZEN;
                print_log("US-EPMWCASALLOC");
                return EPMWCASALLOC;
            }

        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    print_log("US-finish", key, rec_cnt);

    return 0;
}

/*
range scan: [beg_key, end_key)
one leaf node at a time
1) enter the epoch
2) construct a response_array(visiable and not deleted)
3) exit epoch
4) one-record-at-a-time
5) enter new epoch
6) greater than search the largest key in response array
*/

template<typename Key, typename Val>
std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>>
bz_node<Key, Val>::range_scan(const Key *beg_key, const Key *end_key) {
    std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> res;
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t sorted_cnt = get_sorted_count(length_);

    uint32_t bin_beg_pos = binary_search(beg_key, sorted_cnt);
    uint32_t bin_end_pos = end_key ? binary_search(end_key, sorted_cnt) : sorted_cnt;
    for (uint32_t i = bin_beg_pos; i < bin_end_pos; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd) && key_cmp(meta_rd, beg_key) >= 0
            && (!end_key || key_cmp(meta_rd, end_key) < 0))
            copy_data(meta_rd, res);
    }

    uint64_t status_rd = pmwcas_read(&status_);
    uint32_t rec_cnt = get_record_count(status_rd);
    for (uint32_t i = sorted_cnt; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd) && key_cmp(meta_rd, beg_key) >= 0
            && (!end_key || key_cmp(meta_rd, end_key) < 0)) {
            copy_data(meta_rd, res);
        }
    }

    return res;
}

/* First use bztree */
template<typename Key, typename Val>
void bz_tree<Key, Val>::first_use(PMEMobjpool *pop, PMEMoid base_oid) {
    pmwcas_first_use(&pool_, pop, base_oid);
    root_ = 0;
    persist(&root_, sizeof(uint64_t));
    epoch_ = 1;
    persist(&epoch_, sizeof(uint32_t));
}

/* Init BzTree */
template<typename Key, typename Val>
int bz_tree<Key, Val>::init(PMEMobjpool *pop, PMEMoid base_oid) {
    rel_ptr<bz_node<Key, Val>>::set_base(base_oid);
    rel_ptr<rel_ptr<bz_node<Key, Val>>>::set_base(base_oid);
    rel_ptr<bz_node<Key, uint64_t>>::set_base(base_oid);
    rel_ptr<rel_ptr<bz_node<Key, uint64_t>>>::set_base(base_oid);
    rel_ptr<bz_node<uint64_t, uint64_t>>::set_base(base_oid);
    rel_ptr<Key>::set_base(base_oid);
    rel_ptr<Val>::set_base(base_oid);
    pop_ = pop;
    int ret = pmwcas_init(&pool_, base_oid, pop);
    if (ret)
        return ret;
    gc_register(pool_.gc);
    return 0;
}

/* Recover BzTree */
template<typename Key, typename Val>
void bz_tree<Key, Val>::recovery() {
    pmwcas_recovery(&pool_);
    ++epoch_;
    persist(&epoch_, sizeof(epoch_));
}

/* Finish */
template<typename Key, typename Val>
void bz_tree<Key, Val>::finish() {
    pmwcas_finish(&pool_);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::insert(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size) {
    return traverse(BZ_ACTION_INSERT, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::remove(const Key *key) {
    return traverse(BZ_ACTION_DELETE, true, key);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::update(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size) {
    return traverse(BZ_ACTION_UPDATE, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::upsert(const Key *key, const Val *val, uint32_t key_size, uint32_t total_size) {
    return traverse(BZ_ACTION_UPSERT, true, key, val, key_size, total_size);
}

template<typename Key, typename Val>
inline int bz_tree<Key, Val>::read(const Key *key, Val *buffer, uint32_t max_val_size) {
    return traverse(BZ_ACTION_READ, false, key, nullptr, 0, 0, buffer, max_val_size);
}

template<typename Key, typename Val>
int bz_tree<Key, Val>::new_root() {
    mdesc_t mdesc = alloc_mdesc();
    if (mdesc.is_null())
        return EPMWCASALLOC;
    rel_ptr<rel_ptr<bz_node<Key, Val>>> new_node_ptr = alloc_node<Val>(mdesc);
    rel_ptr<bz_node<Key, Val>> new_node = *new_node_ptr;

    pmwcas_add(mdesc, &root_, 0, new_node.rel(), RELEASE_NEW_ON_FAILED);

    int ret = 0;
    if (!pmwcas_commit(mdesc))
        ret = ERACE;

    pmwcas_free(mdesc);

    if (!ret) {
        new_node->print_log("NEW_ROOT");
    }
    return ret;
}

/*
Search @param key within sorted part
return bool, position and @param pos
*/
template<typename Key, typename Val>
bool bz_node<Key, Val>::find_key_sorted(const Key *key, uint32_t &pos) {
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t sorted_cnt = get_sorted_count(length_);
    pos = binary_search(key, sorted_cnt);
    if (pos != sorted_cnt) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[pos]);
        if (is_visiable(meta_rd) && !key_cmp(meta_rd, key))
            return true;
    }
    return false;
}

/*
Search @param key within unsorted part
 @param rec_cnt: Maximal boundary position
 @param alloc_epoch: recheck condition
 return bool
 return position @param pos
 return retry mark @param recheck
*/
template<typename Key, typename Val>
bool bz_node<Key, Val>::find_key_unsorted(const Key *key, uint64_t status_rd, uint32_t alloc_epoch, uint32_t &pos,
                                          bool &retry) {
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t sorted_cnt = get_sorted_count(length_);
    uint32_t rec_cnt = get_record_count(status_rd);
    bool ret = false;
    for (uint32_t i = sorted_cnt; i < rec_cnt; ++i) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd)) {
            if (!key_cmp(meta_rd, key)) {
                pos = i;
                ret = true;
            }
        } else if (!retry && get_offset(meta_rd) == alloc_epoch)
            retry = true;
    }
    return ret;
}

/*
Trigger record_cnt++ for given @param status_rd
 Add @param total_size to block_size
 return new status uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::status_add_rec_blk(uint64_t status_rd, uint32_t total_size) {
    uint32_t rec_cnt = get_record_count(status_rd);
    uint32_t blk_sz = get_block_size(status_rd);
    set_record_count(status_rd, rec_cnt + 1);
    set_block_size(status_rd, blk_sz + total_size);
    return status_rd;
}

/*
* Ϊ����meta_entry @param meta_rd ��delete-size�����������С
* ������meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::status_del(uint64_t status_rd, uint32_t total_size) {
    uint32_t dele_sz = get_delete_size(status_rd);
    set_delete_size(status_rd, dele_sz + total_size);
    return status_rd;
}

template<typename Key, typename Val>
template<typename TreeVal>
inline bool bz_node<Key, Val>::add_dele_sz(bz_tree<Key, TreeVal> *tree, uint32_t total_size) {
    while (true) {
        uint64_t status_rd = pmwcas_read(&status_);
        if (is_frozen(status_rd))
            return false;
        uint64_t status_new = status_del(status_rd, total_size);
        int ret = tree->pack_pmwcas({{&status_, status_rd, status_new}});
        if (!ret)
            return true;
        if (ret == EPMWCASALLOC)
            return false;
    }
}

template<typename Key, typename Val>
inline uint64_t bz_node<Key, Val>::status_frozen(uint64_t status_rd) {
    set_frozen(status_rd);
    return status_rd;
}

/*
* ����meta_entry @param meta_rd
* @param set_vis �Ƿ�ɼ�
* @param new_offset �µ�offset
* ������meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t bz_node<Key, Val>::meta_vis_off(uint64_t meta_rd, bool set_vis, uint32_t new_offset) {
    if (set_vis)
        set_visiable(meta_rd);
    else
        unset_visiable(meta_rd);
    set_offset(meta_rd, new_offset);
    return meta_rd;
}

/*
* ����meta_entry @param meta_rd
* @param set_vis �Ƿ�ɼ�
* @param new_offset �µ�offset
* @param key_size ��ֵ����
* @param total_size �ܳ���
* ������meta_entry uint64_t
*/
template<typename Key, typename Val>
uint64_t
bz_node<Key, Val>::meta_vis_off_klen_tlen(uint64_t meta_rd, bool set_vis, uint32_t new_offset, uint32_t key_size,
                                          uint32_t total_size) {
    if (set_vis)
        set_visiable(meta_rd);
    else
        unset_visiable(meta_rd);
    set_offset(meta_rd, new_offset);
    set_key_length(meta_rd, key_size);
    set_total_length(meta_rd, total_size);
    return meta_rd;
}

/*
* �����������key value��block storage
* @param new_offset: ��������ڵ�ͷ��ƫ��
*/
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_data(uint32_t new_offset, const Key *key, const Val *val, uint32_t key_size,
                                  uint32_t total_size) {
    set_key(new_offset, key);
    set_value(new_offset + key_size, val);
    persist((char *) this + new_offset, total_size);
}

/* �������ṹ�������ݵ��û��ռ� */
template<typename Key, typename Val>
void bz_node<Key, Val>::copy_data(uint64_t meta_rd,
                                  std::vector<std::pair<std::shared_ptr<Key>, std::shared_ptr<Val>>> &res) {
    shared_ptr <Key> sp_key;
    if (typeid(Key) == typeid(char)) {
        sp_key = shared_ptr<Key>(new Key[get_key_length(meta_rd)], [](Key *p) { delete[] p; });
        copy_key(sp_key.get(), get_key(meta_rd));
    } else {
        sp_key = make_shared<Key>(*get_key(meta_rd));
    }
    shared_ptr <Val> sp_val;
    if (typeid(Val) == typeid(char)) {
        uint32_t val_sz = get_total_length(meta_rd) - get_key_length(meta_rd);
        sp_val = shared_ptr<Val>(new Val[val_sz], [](Val *p) { delete[] p; });
        copy_value(sp_val.get(), get_value(meta_rd));
    } else {
        sp_val = make_shared<Val>(*get_value(meta_rd));
    }
    res.emplace_back(make_pair(sp_key, sp_val));
}

/*
* ����ɨ�������ֵ���� */
template<typename Key, typename Val>
template<typename TreeVal>
int bz_node<Key, Val>::rescan_unsorted(bz_tree<Key, TreeVal> *tree, uint32_t beg_pos, uint32_t rec_cnt, const Key *key,
                                       uint32_t total_size, uint32_t alloc_epoch) {
    uint64_t *meta_arr = rec_meta_arr();
    uint32_t i = beg_pos;
    while (i < rec_cnt) {
        uint64_t meta_rd = pmwcas_read(&meta_arr[i]);
        if (is_visiable(meta_rd)) {
            if (!key_cmp(meta_rd, key)) {
                set_offset(meta_arr[rec_cnt], 0);
                persist(&meta_arr[rec_cnt], sizeof(uint64_t));
                if (!add_dele_sz(tree, total_size))
                    return EFROZEN;
                return EUNIKEY;
            }
        } else if (get_offset(meta_rd) == alloc_epoch) {
            // Ǳ�ڵ�UNIKEY����������ȴ������
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        ++i;
    }
    return 0;
}

/*
* ����key�Ĳ���λ�ã����ʱȡ����λ�ã�
* �ն�����£�����̽���ҵ��ǿն�Ԫ��
* ���keyΪ�գ������˺������ڲ���������meta�����еķǿ�meta�ĸ���
*/
template<typename Key, typename Val>
uint32_t bz_node<Key, Val>::binary_search(const Key *key, int size, uint64_t *meta_arr) {
    if (!size) {
        size = (int) get_sorted_count(length_);
    }
    if (!meta_arr)
        meta_arr = rec_meta_arr();
    int left = -1, right = size;
    while (left + 1 < right) {
        int mid = left + (right - left) / 2;
        uint64_t meta_rd = pmwcas_read(&meta_arr[mid]);
        /* �ն����� BEGIN */
        if (!is_visiable(meta_rd)) {
            if (!key) {
                //���ڲ��ҷǿ�meta
                right = mid;
                continue;
            }
            //���ڲ��Ҽ�ֵλ��
            bool go_left = true;
            int left_mid = mid - 1, right_mid = mid + 1;
            while (left_mid > left || right_mid < right) {
                if (go_left && left_mid > left || right_mid == right) {
                    meta_rd = pmwcas_read(&meta_arr[left_mid]);
                    if (is_visiable(meta_rd)) {
                        mid = left_mid;
                        break;
                    }
                    go_left = false;
                    --left_mid;
                }
                if (!go_left && right_mid < right || left_mid == left) {
                    meta_rd = pmwcas_read(&meta_arr[right_mid]);
                    if (is_visiable(meta_rd)) {
                        mid = right_mid;
                        break;
                    }
                    go_left = true;
                    ++right_mid;
                }
            }
            if (left_mid == left && right_mid == right) {
                return right;
            }
        }
        /* �ն����� END */

        if (!key) {
            //���ڲ��ҷǿ�meta
            left = mid;
            continue;
        }
        //���ڲ��Ҽ�ֵλ��
        if (key_cmp(meta_rd, key) < 0)
            left = mid;
        else
            right = mid;
    }
    return right;
}

/* ��װpmwcas��ʹ�� */
template<typename Key, typename Val>
int bz_tree<Key, Val>::pack_pmwcas(std::vector<std::tuple<rel_ptr<uint64_t>, uint64_t, uint64_t>> casn) {
    mdesc_t mdesc = alloc_mdesc();
    if (mdesc.is_null())
        return EPMWCASALLOC;
    for (auto cas : casn)
        pmwcas_add(mdesc, std::get<0>(cas), std::get<1>(cas), std::get<2>(cas));
    bool done = pmwcas_commit(mdesc);
    pmwcas_free(mdesc);
    return done ? 0 : EPMWCASFAIL;
}

template<typename Key, typename Val>
uint64_t *bz_node<Key, Val>::rec_meta_arr() {
    return (uint64_t *) ((char *) this + sizeof(*this));
}

/* K-V getter and setter */
template<typename Key, typename Val>
Key *bz_node<Key, Val>::get_key(uint64_t meta) {
    uint32_t off = get_offset(meta);
    if (!is_visiable(meta) || !off)
        return nullptr;
    return (Key *) ((char *) this + off);
}

template<typename Key, typename Val>
void bz_node<Key, Val>::set_key(uint32_t offset, const Key *key) {
    Key *addr = (Key *) ((char *) this + offset);
    copy_key(addr, key);
}

template<typename Key, typename Val>
void bz_node<Key, Val>::copy_key(Key *dst, const Key *src) {
    if (*(uint64_t *) src == BZ_KEY_MAX)
        *(uint64_t *) dst = BZ_KEY_MAX;
    else if (typeid(Key) == typeid(char))
        strcpy_s((char *) dst, strlen((char *) src) + 1, (char *) src);
    else
        *dst = *src;
}

template<typename Key, typename Val>
Val *bz_node<Key, Val>::get_value(uint64_t meta) {
    return (Val *) ((char *) this + get_offset(meta) + get_key_length(meta));
}

template<typename Key, typename Val>
void bz_node<Key, Val>::set_value(uint32_t offset, const Val *val) {
    Val *addr = (Val *) ((char *) this + offset);
    copy_value(addr, val);
}

template<typename Key, typename Val>
void bz_node<Key, Val>::copy_value(Val *dst, const Val *src) {
    if (typeid(Val) == typeid(char))
        strcpy((char *) dst, (const char *) src);
    else
        *dst = *src;
}

/* ��ֵ�ȽϺ��� */
template<typename Key, typename Val>
int bz_node<Key, Val>::key_cmp(uint64_t meta_entry, const Key *key) {
    const Key *k1 = get_key(meta_entry);
    if (*(uint64_t *) k1 == BZ_KEY_MAX)
        return 1;
    if (*(uint64_t *) key == BZ_KEY_MAX)
        return -1;
    if (typeid(Key) == typeid(char))
        return strcmp((char *) k1, (char *) key);
    if (*k1 == *key)
        return 0;
    return *k1 < *key ? -1 : 1;
}

template<typename Key, typename Val>
bool bz_node<Key, Val>::key_cmp_meta(uint64_t meta_1, uint64_t meta_2) {
    if (!is_visiable(meta_1))
        return false;
    if (!is_visiable(meta_2))
        return true;
    const Key *k2 = get_key(meta_2);
    return key_cmp(meta_1, k2) < 0;
}

/* λ���������� BEGIN */
inline bool is_frozen(uint64_t status) {
    return 1 & (status >> 60);
}

inline void set_frozen(uint64_t &s) {
    s |= (1ULL << 60);
}

inline void unset_frozen(uint64_t &s) {
    s &= ~(1ULL << 60);
}

inline uint32_t get_record_count(uint64_t status) {
    return 0xffff & (status >> 44);
}

inline void set_record_count(uint64_t &s, uint64_t new_record_count) {
    s = (s & 0xf0000fffffffffff) | (new_record_count << 44);
}

inline uint32_t get_block_size(uint64_t status) {
    return 0x3fffff & (status >> 22);
}

inline void set_block_size(uint64_t &s, uint64_t new_block_size) {
    s = (s & 0xfffff000003fffff) | (new_block_size << 22);
}

inline uint32_t get_delete_size(uint64_t status) {
    return 0x3fffff & status;
}

inline void set_delete_size(uint64_t &s, uint32_t new_delete_size) {
    s = (s & 0xffffffffffc0000) | new_delete_size;
}

inline bool is_leaf(uint64_t length) {
    return !(1 & length);
}

inline void set_non_leaf(uint64_t &length) {
    length |= 1;
}

inline void set_leaf(uint64_t &length) {
    length &= (~1ULL);
}

inline uint32_t get_node_size(uint64_t length) {
    return length >> 32;
}

inline void set_node_size(uint64_t &s, uint64_t new_node_size) {
    s = (s & 0xffffffff) | (new_node_size << 32);
}

inline uint32_t get_sorted_count(uint64_t length) {
    return (length >> 1) & 0x7fffffff;
}

inline void set_sorted_count(uint64_t &s, uint32_t new_sorted_count) {
    s = (s & 0xffffffff00000001) | (new_sorted_count << 1);
}

inline bool is_visiable(uint64_t meta) {
    return !(1 & (meta >> 60));
}

inline void set_visiable(uint64_t &meta) {
    meta &= 0xefffffffffffffff;
}

inline void unset_visiable(uint64_t &meta) {
    meta |= ((uint64_t) 1 << 60);
}

inline uint32_t get_offset(uint64_t meta) {
    return 0xfffffff & (meta >> 32);
}

inline void set_offset(uint64_t &meta, uint64_t new_offset) {
    meta = (meta & 0xf0000000ffffffff) | (new_offset << 32);
}

inline uint16_t get_key_length(uint64_t meta) {
    return 0xffff & (meta >> 16);
}

inline void set_key_length(uint64_t &meta, uint32_t new_key_length) {
    meta = (meta & 0xffffffff0000ffff) | (new_key_length << 16);
}

inline uint32_t get_total_length(uint64_t meta) {
    return 0xffff & meta;
}

inline void set_total_length(uint64_t &meta, uint16_t new_total_length) {
    meta = (meta & 0xffffffffffff0000) | new_total_length;
}

/* λ������������ END */

inline bool is_leaf_node(uint64_t node) {
    rel_ptr<bz_node<uint64_t, uint64_t>> ptr(node);
    return is_leaf(ptr->length_);
}

#endif // !BZTREE_H
