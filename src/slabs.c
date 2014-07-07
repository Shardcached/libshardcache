/*
 * Copyright (c) 2003, Danga Interactive, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 * 
 *     * Neither the name of the Danga Interactive nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include "shardcache.h"

#include "slabs.h"

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)
#define DEFAULT_ITEM_SIZE_MAX (1024 * 1024) /* The famous 1MB upper limit. */

#define ITEM_LINKED 1
#define ITEM_SLABBED 4
#define ITEM_FETCHED 8


/* powers-of-N allocation structures */

#pragma pack(push, 1)
typedef struct __item_s {
    struct __item_s *next;
    struct __item_s *prev;
    unsigned short  used;
    uint8_t         it_flags;   /* ITEM_* above */
    uint8_t         clsid;/* which slab class we're in */
    pthread_mutex_t lock;
} item_t;

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    item_t *slots;    /* list of item ptrs */
    unsigned int sl_curr;   /* total free items in list */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;  /* index+1 of dying slab, or zero if none */
    size_t requested; /* The number of requested bytes */
} slabclass_t;

struct slab_rebalance {
    void *slab_start;
    void *slab_end;
    void *slab_pos;
    int s_clsid;
    int d_clsid;
    int busy_items;
    uint8_t done;
};

#define DEFAULT_SLAB_BULK_CHECK 1
struct __slabs_s {
    slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
    size_t mem_limit;
    size_t mem_malloced;
    int power_largest;

    int automove;

    int rebalance_signal;
    pthread_cond_t maintenance_cond;
    pthread_cond_t rebalance_cond;
    pthread_t maintenance_tid;
    pthread_t rebalance_tid;

    int do_run_maintenance_thread;
    int do_run_rebalance_thread;

    void *mem_base;
    void *mem_current;
    size_t mem_avail;
    size_t item_size_max;
    pthread_mutex_t lock;
    pthread_mutex_t rebalance_lock;
    struct slab_rebalance rebal;

    int slab_bulk_check;

};
#pragma pack(pop)

/**
 * Access to the slab allocator is protected by this lock
 */

/*
 * Forward Declarations
 */
static int do_slabs_newslab(slabs_t *sl, const unsigned int id);
static void *memory_allocate(slabs_t *sl, size_t size);
static void do_slabs_free(slabs_t *sl, void *ptr, const size_t size, unsigned int id);

/* Preallocate as many slab pages as possible (called from slabs_create)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (slabs_t *sl, const unsigned int maxslabs);

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(slabs_t *sl, const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > sl->slabclass[res].size)
        if (res++ == sl->power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
slabs_t *slabs_create(const size_t limit, const double factor, const int prealloc, const size_t max_size)
{
    int i = POWER_SMALLEST - 1;
    unsigned int size = sizeof(item_t);

    slabs_t *sl = calloc(1, sizeof(slabs_t));
    sl->item_size_max = DEFAULT_ITEM_SIZE_MAX;
    sl->mem_limit = limit;
    sl->slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

    if (max_size)
        sl->item_size_max = max_size;

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        sl->mem_base = malloc(sl->mem_limit);
        if (sl->mem_base != NULL) {
            sl->mem_current = sl->mem_base;
            sl->mem_avail = sl->mem_limit;
        } else {
            fprintf(stderr, "Warning: Failed to allocate requested memory in"
                    " one large chunk.\nWill allocate in smaller chunks\n");
        }
    }

    memset(sl->slabclass, 0, sizeof(slabclass_t));

    while (++i < POWER_LARGEST && size <= sl->item_size_max / factor) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        sl->slabclass[i].size = size;
        sl->slabclass[i].perslab = sl->item_size_max / sl->slabclass[i].size;
        size *= factor;
        SHC_DEBUG3("slab class %3d: chunk size %9u perslab %7u",
                i, sl->slabclass[i].size, sl->slabclass[i].perslab);
    }

    sl->power_largest = i;
    sl->slabclass[sl->power_largest].size = sl->item_size_max;
    sl->slabclass[sl->power_largest].perslab = 1;
    SHC_DEBUG3("slab class %3d: chunk size %9u perslab %7u",
            i, sl->slabclass[i].size, sl->slabclass[i].perslab);

    /* for the test suite:  faking of how much we've already malloc'd */
    /*
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            mem_malloced = (size_t)atol(t_initial_malloc);
        }

    }
    */

    pthread_mutex_init(&sl->lock, NULL);
    pthread_mutex_init(&sl->rebalance_lock, NULL);

    pthread_cond_init(&sl->maintenance_cond, NULL);
    pthread_cond_init(&sl->rebalance_cond, NULL);
    sl->do_run_maintenance_thread = 1;
    sl->do_run_rebalance_thread = 1;
    if (prealloc) {
        slabs_preallocate(sl, sl->power_largest);
    }

    return sl;
}

void slabs_destroy(slabs_t *sl)
{
    if (sl->maintenance_tid > 0)
        stop_slab_maintenance_thread(sl);

    if (sl->rebalance_tid > 0)
        stop_slab_rebalance_thread(sl);

}

static void slabs_preallocate (slabs_t *sl, const unsigned int maxslabs) {
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
        if (++prealloc > maxslabs)
            return;
        if (do_slabs_newslab(sl, i) == 0) {
            fprintf(stderr, "Error while preallocating slab memory!\n"
                "If using -L or other prealloc options, max memory must be "
                "at least %d megabytes.\n", sl->power_largest);
            exit(1);
        }
    }

}

static int grow_slab_list (slabs_t *sl, const unsigned int id) {
    slabclass_t *p = &sl->slabclass[id];
    if (p->slabs == p->list_size) {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static void split_slab_page_into_freelist(slabs_t *sl, char *ptr, const unsigned int id) {
    slabclass_t *p = &sl->slabclass[id];
    int x;
    for (x = 0; x < p->perslab; x++) {
        do_slabs_free(sl, ptr, 0, id);
        ptr += p->size;
    }
}

static int do_slabs_newslab(slabs_t *sl, const unsigned int id) {
    slabclass_t *p = &sl->slabclass[id];
    int len = p->size * p->perslab;
    char *ptr;

    if ((sl->mem_limit && sl->mem_malloced + len > sl->mem_limit && p->slabs > 0) ||
        (grow_slab_list(sl, id) == 0) ||
        ((ptr = memory_allocate(sl, (size_t)len)) == 0)) {

        return 0;
    }

    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist(sl, ptr, id);

    p->slab_list[p->slabs++] = ptr;
    sl->mem_malloced += len;

    return 1;
}

/*@null@*/
static void *do_slabs_alloc(slabs_t *sl, const size_t size, unsigned int id) {
    slabclass_t *p;
    void *ret = NULL;
    item_t *it = NULL;

    if (id < POWER_SMALLEST || id > sl->power_largest) {
        return NULL;
    }

    p = &sl->slabclass[id];
    assert(p->sl_curr == 0 || ((item_t *)p->slots)->clsid == 0);

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->sl_curr != 0 || do_slabs_newslab(sl, id) != 0)) {
        /* We don't have more memory available */
        ret = NULL;
    } else if (p->sl_curr != 0) {
        /* return off our freelist */
        it = (item_t *)p->slots;
        p->slots = it->next;
        if (it->next) it->next->prev = 0;
        p->sl_curr--;
        pthread_mutex_init(&it->lock, NULL);
        ret = (void *)it;
    }

    if (ret) {
        p->requested += size;
    } else {
        // TODO - Error message
    }

    return ret;
}

static void do_slabs_free(slabs_t *sl, void *ptr, const size_t size, unsigned int id) {
    slabclass_t *p;
    item_t *it;

    assert(((item_t *)ptr)->clsid == 0);
    assert(id >= POWER_SMALLEST && id <= sl->power_largest);
    if (id < POWER_SMALLEST || id > sl->power_largest)
        return;

    p = &sl->slabclass[id];

    it = (item_t *)ptr;
    it->it_flags |= ITEM_SLABBED;
    it->prev = 0;
    it->next = p->slots;
    if (it->next) it->next->prev = it;
    pthread_mutex_destroy(&it->lock);
    p->slots = it;

    p->sl_curr++;
    p->requested -= size;
    return;
}

#if 0
static int nz_strcmp(int nzlength, const char *nz, const char *z) {
    int zlength=strlen(z);
    return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c) {
    bool ret = true;

    if (add_stats != NULL) {
        if (!stat_type) {
            /* prepare general statistics for the engine */
            STATS_LOCK();
            APPEND_STAT("bytes", "%llu", (unsigned long long)stats.curr_bytes);
            APPEND_STAT("curr_items", "%u", stats.curr_items);
            APPEND_STAT("total_items", "%u", stats.total_items);
            STATS_UNLOCK();
            item_stats_totals(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "items") == 0) {
            item_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
            slabs_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
            item_stats_sizes(add_stats, c);
        } else {
            ret = false;
        }
    } else {
        ret = false;
    }

    return ret;
}

/*@null@*/
static void do_slabs_stats(ADD_STAT add_stats, void *c) {
    int i, total;
    /* Get the per-thread stats which contain some interesting aggregates */
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);

    total = 0;
    for(i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            char key_str[STAT_KEY_LEN];
            char val_str[STAT_VAL_LEN];
            int klen = 0, vlen = 0;

            APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
            APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
            APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
            APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
            APPEND_NUM_STAT(i, "used_chunks", "%u",
                            slabs*perslab - p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
            /* Stat is dead, but displaying zero instead of removing it. */
            APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
            APPEND_NUM_STAT(i, "mem_requested", "%llu",
                            (unsigned long long)p->requested);
            APPEND_NUM_STAT(i, "get_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].get_hits);
            APPEND_NUM_STAT(i, "cmd_set", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].set_cmds);
            APPEND_NUM_STAT(i, "delete_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].delete_hits);
            APPEND_NUM_STAT(i, "incr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].incr_hits);
            APPEND_NUM_STAT(i, "decr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].decr_hits);
            APPEND_NUM_STAT(i, "cas_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_hits);
            APPEND_NUM_STAT(i, "cas_badval", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_badval);
            APPEND_NUM_STAT(i, "touch_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].touch_hits);
            total++;
        }
    }

    /* add overall slab stats and append terminator */

    APPEND_STAT("active_slabs", "%d", total);
    APPEND_STAT("total_malloced", "%llu", (unsigned long long)mem_malloced);
    add_stats(NULL, 0, NULL, 0, c);
}
#endif

static void *memory_allocate(slabs_t *sl, size_t size) {
    void *ret;

    if (sl->mem_base == NULL) {
        /* We are not using a preallocated large memory chunk */
        ret = malloc(size);
    } else {
        ret = sl->mem_current;

        if (size > sl->mem_avail) {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        sl->mem_current = ((char*)sl->mem_current) + size;
        if (size < sl->mem_avail) {
            sl->mem_avail -= size;
        } else {
            sl->mem_avail = 0;
        }
    }

    return ret;
}

void *slabs_alloc(slabs_t *sl, size_t size, unsigned int id) {
    void *ret;

    pthread_mutex_lock(&sl->lock);
    ret = do_slabs_alloc(sl, size, id);
    pthread_mutex_unlock(&sl->lock);
    return ret + sizeof(item_t);
}

void slabs_free(slabs_t *sl, void *ptr, size_t size, unsigned int id) {
    pthread_mutex_lock(&sl->lock);
    do_slabs_free(sl, ptr - sizeof(item_t), size, id);
    pthread_mutex_unlock(&sl->lock);
}

#if 0
void slabs_stats(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_stats(add_stats, c);
    pthread_mutex_unlock(&slabs_lock);
}
#endif

void slabs_adjust_mem_requested(slabs_t *sl, unsigned int id, size_t old, size_t ntotal)
{
    pthread_mutex_lock(&sl->lock);
    slabclass_t *p;
    if (id < POWER_SMALLEST || id > sl->power_largest) {
        fprintf(stderr, "Internal error! Invalid slab class\n");
        abort();
    }

    p = &sl->slabclass[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&sl->lock);
}

static int slab_rebalance_start(slabs_t *sl) {
    slabclass_t *s_cls;
    int no_go = 0;

    //pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&sl->lock);

    if (sl->rebal.s_clsid < POWER_SMALLEST ||
        sl->rebal.s_clsid > sl->power_largest  ||
        sl->rebal.d_clsid < POWER_SMALLEST ||
        sl->rebal.d_clsid > sl->power_largest  ||
        sl->rebal.s_clsid == sl->rebal.d_clsid)
        no_go = -2;

    s_cls = &sl->slabclass[sl->rebal.s_clsid];

    if (!grow_slab_list(sl, sl->rebal.d_clsid)) {
        no_go = -1;
    }

    if (s_cls->slabs < 2)
        no_go = -3;

    if (no_go != 0) {
        pthread_mutex_unlock(&sl->lock);
        //pthread_mutex_unlock(&cache_lock);
        return no_go; /* Should use a wrapper function... */
    }

    s_cls->killing = 1;

    sl->rebal.slab_start = s_cls->slab_list[s_cls->killing - 1];
    sl->rebal.slab_end   = (char *)sl->rebal.slab_start +
        (s_cls->size * s_cls->perslab);
    sl->rebal.slab_pos   = sl->rebal.slab_start;
    sl->rebal.done       = 0;

    /* Also tells do_item_get to search for items in this slab */
    sl->rebalance_signal = 2;

    SHC_DEBUG2("Started a slab rebalance");

    pthread_mutex_unlock(&sl->lock);
    //pthread_mutex_unlock(&cache_lock);

 //   STATS_LOCK();
//    stats.slab_reassign_running = true;
    //STATS_UNLOCK();

    return 0;
}

enum move_status {
    MOVE_PASS=0, MOVE_DONE, MOVE_BUSY, MOVE_LOCKED
};

/* refcount == 0 is safe since nobody can incr while cache_lock is held.
 * refcount != 0 is impossible since flags/etc can be modified in other
 * threads. instead, note we found a busy one and bail. logic in do_item_get
 * will prevent busy items from continuing to be busy
 */
static int slab_rebalance_move(slabs_t *sl) {
    slabclass_t *s_cls;
    int x;
    int was_busy = 0;
    int refcount = 0;
    enum move_status status = MOVE_PASS;

    //pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&sl->lock);

    s_cls = &sl->slabclass[sl->rebal.s_clsid];

    for (x = 0; x < sl->slab_bulk_check; x++) {
        item_t *it = sl->rebal.slab_pos;
        status = MOVE_PASS;
        if (it->clsid != 255) {
            if (pthread_mutex_trylock(&it->lock) != 0) {
                status = MOVE_LOCKED;
            } else {
                refcount = __sync_add_and_fetch(&it->used, 1);
                if (refcount == 1) { /* item is unlinked, unused */
                    if (it->it_flags & ITEM_SLABBED) {
                        /* remove from slab freelist */
                        if (s_cls->slots == it) {
                            s_cls->slots = it->next;
                        }
                        if (it->next) it->next->prev = it->prev;
                        if (it->prev) it->prev->next = it->next;
                        s_cls->sl_curr--;
                        status = MOVE_DONE;
                    } else {
                        status = MOVE_BUSY;
                    }
                } 
#if 0
                // TODO - when an object goes to a ghost list we can assume it's linked but not
                // busy, but we need to make arc.c aware of this, for now if an object is in any of the
                // cache lists we assume it's busy
                else if (refcount == 2) { /* item is linked but not busy */
                    if ((it->it_flags & ITEM_LINKED) != 0) {
                        do_item_unlink_nolock(it, hv);
                        status = MOVE_DONE;
                    } else {
                        /* refcount == 1 + !ITEM_LINKED means the item is being
                         * uploaded to, or was just unlinked but hasn't been freed
                         * yet. Let it bleed off on its own and try again later */
                        status = MOVE_BUSY;
                    }
                }
#endif
                else {
                    SHC_DEBUG("Slab reassign hit a busy item: refcount: %d (%d -> %d)",
                        it->used, sl->rebal.s_clsid, sl->rebal.d_clsid);
                    status = MOVE_BUSY;
                }
                pthread_mutex_unlock(&it->lock);
            }
        }

        switch (status) {
            case MOVE_DONE:
                it->used = 0;
                it->it_flags = 0;
                it->clsid = 255;
                break;
            case MOVE_BUSY:
                __sync_sub_and_fetch(&it->used, 1);
            case MOVE_LOCKED:
                sl->rebal.busy_items++;
                was_busy++;
                break;
            case MOVE_PASS:
                break;
        }

        sl->rebal.slab_pos = (char *)sl->rebal.slab_pos + s_cls->size;
        if (sl->rebal.slab_pos >= sl->rebal.slab_end)
            break;
    }

    if (sl->rebal.slab_pos >= sl->rebal.slab_end) {
        /* Some items were busy, start again from the top */
        if (sl->rebal.busy_items) {
            sl->rebal.slab_pos = sl->rebal.slab_start;
            sl->rebal.busy_items = 0;
        } else {
            sl->rebal.done++;
        }
    }

    pthread_mutex_unlock(&sl->lock);
    //pthread_mutex_unlock(&cache_lock);

    return was_busy;
}

static void slab_rebalance_finish(slabs_t *sl) {
    slabclass_t *s_cls;
    slabclass_t *d_cls;

    //pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&sl->lock);

    s_cls = &sl->slabclass[sl->rebal.s_clsid];
    d_cls   = &sl->slabclass[sl->rebal.d_clsid];

    /* At this point the stolen slab is completely clear */
    s_cls->slab_list[s_cls->killing - 1] =
        s_cls->slab_list[s_cls->slabs - 1];
    s_cls->slabs--;
    s_cls->killing = 0;

    memset(sl->rebal.slab_start, 0, sl->item_size_max);

    d_cls->slab_list[d_cls->slabs++] = sl->rebal.slab_start;
    split_slab_page_into_freelist(sl, sl->rebal.slab_start,
        sl->rebal.d_clsid);

    sl->rebal.done       = 0;
    sl->rebal.s_clsid    = 0;
    sl->rebal.d_clsid    = 0;
    sl->rebal.slab_start = NULL;
    sl->rebal.slab_end   = NULL;
    sl->rebal.slab_pos   = NULL;

    sl->rebalance_signal = 0;

    pthread_mutex_unlock(&sl->lock);
    //pthread_mutex_unlock(&cache_lock);

    /*
    STATS_LOCK();
    stats.slab_reassign_running = false;
    stats.slabs_moved++;
    STATS_UNLOCK();
    */

    SHC_DEBUG2("finished a slab move");
}

/* Return 1 means a decision was reached.
 * Move to its own thread (created/destroyed as needed) once automover is more
 * complex.
 */
static int slab_automove_decision(slabs_t *sl, int *src, int *dst) {
    static uint64_t evicted_old[POWER_LARGEST];
    static unsigned int slab_zeroes[POWER_LARGEST];
    static unsigned int slab_winner = 0;
    static unsigned int slab_wins   = 0;
    uint64_t evicted_new[POWER_LARGEST];
    uint64_t evicted_diff = 0;
    uint64_t evicted_max  = 0;
    unsigned int highest_slab = 0;
    unsigned int total_pages[POWER_LARGEST];
    int i;
    int source = 0;
    int dest = 0;
    static unsigned int next_run;

    struct timeval now;
    gettimeofday(&now, NULL);
    /* Run less frequently than the slabmove tester. */
    if (now.tv_sec >= next_run) {
        next_run = now.tv_sec + 10;
    } else {
        return 0;
    }

    //item_stats_evictions(evicted_new);
    //pthread_mutex_lock(&cache_lock);
    for (i = POWER_SMALLEST; i < sl->power_largest; i++) {
        total_pages[i] = sl->slabclass[i].slabs;
    }
    //pthread_mutex_unlock(&cache_lock);

    /* Find a candidate source; something with zero evicts 3+ times */
    for (i = POWER_SMALLEST; i < sl->power_largest; i++) {
        evicted_diff = evicted_new[i] - evicted_old[i];
        if (evicted_diff == 0 && total_pages[i] > 2) {
            slab_zeroes[i]++;
            if (source == 0 && slab_zeroes[i] >= 3)
                source = i;
        } else {
            slab_zeroes[i] = 0;
            if (evicted_diff > evicted_max) {
                evicted_max = evicted_diff;
                highest_slab = i;
            }
        }
        evicted_old[i] = evicted_new[i];
    }

    /* Pick a valid destination */
    if (slab_winner != 0 && slab_winner == highest_slab) {
        slab_wins++;
        if (slab_wins >= 3)
            dest = slab_winner;
    } else {
        slab_wins = 1;
        slab_winner = highest_slab;
    }

    if (source && dest) {
        *src = source;
        *dst = dest;
        return 1;
    }
    return 0;
}

/* Slab rebalancer thread.
 * Does not use spinlocks since it is not timing sensitive. Burn less CPU and
 * go to sleep if locks are contended
 */
static void *slab_maintenance_thread(void *arg) {
    int src, dest;
    slabs_t *sl = (slabs_t *)arg;
    while (sl->do_run_maintenance_thread) {
      if (sl->automove == 1) {
          if (slab_automove_decision(sl, &src, &dest) == 1) {
              /* Blind to the return codes. It will retry on its own */
              slabs_reassign(sl, src, dest);
          }
          sleep(1);
      } else {
            /* Don't wake as often if we're not enabled.
             * This is lazier than setting up a condition right now. */
            sleep(5);
      }
    }
    return NULL;
}

/* Slab mover thread.
 * Sits waiting for a condition to jump off and shovel some memory about
 */
static void *slab_rebalance_thread(void *arg) {
    int was_busy = 0;
    slabs_t *sl = (slabs_t *)arg;
    /* So we first pass into cond_wait with the mutex held */
    pthread_mutex_lock(&sl->rebalance_lock);

    while (sl->do_run_rebalance_thread) {
        if (sl->rebalance_signal == 1) {
            if (slab_rebalance_start(sl) < 0) {
                /* Handle errors with more specifity as required. */
                sl->rebalance_signal = 0;
            }

            was_busy = 0;
        } else if (sl->rebalance_signal && sl->rebal.slab_start != NULL) {
            was_busy = slab_rebalance_move(sl);
        }

        if (sl->rebal.done) {
            slab_rebalance_finish(sl);
        } else if (was_busy) {
            /* Stuck waiting for some items to unlock, so slow down a bit
             * to give them a chance to free up */
            usleep(50);
        }

        if (sl->rebalance_signal == 0) {
            /* always hold this lock while we're running */
            pthread_cond_wait(&sl->rebalance_cond, &sl->rebalance_lock);
        }
    }
    return NULL;
}

/* Iterate at most once through the slab classes and pick a "random" source.
 * I like this better than calling rand() since rand() is slow enough that we
 * can just check all of the classes once instead.
 */
static int slabs_reassign_pick_any(slabs_t *sl, int dst) {
    static int cur = POWER_SMALLEST - 1;
    int tries = sl->power_largest - POWER_SMALLEST + 1;
    for (; tries > 0; tries--) {
        cur++;
        if (cur > sl->power_largest)
            cur = POWER_SMALLEST;
        if (cur == dst)
            continue;
        if (sl->slabclass[cur].slabs > 1) {
            return cur;
        }
    }
    return -1;
}

static enum reassign_result_type do_slabs_reassign(slabs_t *sl, int src, int dst) {
    if (sl->rebalance_signal != 0)
        return REASSIGN_RUNNING;

    if (src == dst)
        return REASSIGN_SRC_DST_SAME;

    /* Special indicator to choose ourselves. */
    if (src == -1) {
        src = slabs_reassign_pick_any(sl, dst);
        /* TODO: If we end up back at -1, return a new error type */
    }

    if (src < POWER_SMALLEST || src > sl->power_largest ||
        dst < POWER_SMALLEST || dst > sl->power_largest)
        return REASSIGN_BADCLASS;

    if (sl->slabclass[src].slabs < 2)
        return REASSIGN_NOSPARE;

    sl->rebal.s_clsid = src;
    sl->rebal.d_clsid = dst;

    sl->rebalance_signal = 1;
    pthread_cond_signal(&sl->rebalance_cond);

    return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(slabs_t *sl, int src, int dst) {
    enum reassign_result_type ret;
    if (pthread_mutex_trylock(&sl->rebalance_lock) != 0) {
        return REASSIGN_RUNNING;
    }
    ret = do_slabs_reassign(sl, src, dst);
    pthread_mutex_unlock(&sl->rebalance_lock);
    return ret;
}

/* If we hold this lock, rebalancer can't wake up or move */
void slabs_rebalancer_pause(slabs_t *sl) {
    pthread_mutex_lock(&sl->rebalance_lock);
}

void slabs_rebalancer_resume(slabs_t *sl) {
    pthread_mutex_unlock(&sl->rebalance_lock);
}

int start_slab_rebalance_thread(slabs_t *sl) {
    int ret;
    sl->rebalance_signal = 0;
    sl->rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env != NULL) {
        sl->slab_bulk_check = atoi(env);
        if (sl->slab_bulk_check == 0) {
            sl->slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }

    if (pthread_cond_init(&sl->rebalance_cond, NULL) != 0) {
        fprintf(stderr, "Can't intiialize rebalance condition\n");
        return -1;
    }
    pthread_mutex_init(&sl->rebalance_lock, NULL);

    if ((ret = pthread_create(&sl->rebalance_tid, NULL,
                              slab_rebalance_thread, sl)) != 0) {
        fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

int start_slab_maintenance_thread(slabs_t *sl) {
    int ret;
    sl->rebalance_signal = 0;
    sl->rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env != NULL) {
        sl->slab_bulk_check = atoi(env);
        if (sl->slab_bulk_check == 0) {
            sl->slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }

    if ((ret = pthread_create(&sl->maintenance_tid, NULL,
                              slab_maintenance_thread, sl)) != 0) {
        fprintf(stderr, "Can't create slab maint thread: %s\n", strerror(ret));
        return -1;
    }

    return 0;
}

void stop_slab_rebalance_thread(slabs_t *sl) {
    sl->do_run_rebalance_thread = 0;
    pthread_cond_signal(&sl->rebalance_cond);
    pthread_join(sl->rebalance_tid, NULL);
    sl->rebalance_tid = 0;
}

void stop_slab_maintenance_thread(slabs_t *sl) {
    //mutex_lock(&cache_lock);
    sl->do_run_maintenance_thread = 0;
    pthread_cond_signal(&sl->maintenance_cond);
    //pthread_mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(sl->maintenance_tid, NULL);
    sl->maintenance_tid = 0;
}
