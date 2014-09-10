#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <limits.h>
#include <memory.h>
#include <stddef.h>

#include <hashtable.h>
#include <refcnt.h>

#include <atomic_defs.h>

#include "shardcache_internal.h" // for MUTEX_* macros

#include "arc.h"


#define LIKELY(__e) __builtin_expect((__e), 1)
#define UNLIKELY(__e) __builtin_expect((__e), 0)

/**********************************************************************
 * Simple double-linked list, inspired by the implementation used in the
 * linux kernel.
 */
#pragma pack(push, 1)
typedef struct __arc_list {
    struct __arc_list *prev, *next;
} arc_list_t;
#pragma pack(pop)

#define arc_list_entry(ptr, type, field) \
    ((type*) (((char*)ptr) - offsetof(type, field)))

#define arc_list_each(pos, head) \
    for (pos = (head)->next; pos && pos != (head); pos = pos->next)

#define arc_list_each_prev(pos, head) \
    for (pos = (head)->prev; pos && pos != (head); pos = pos->prev)

/**********************************************************************
 * The arc state represents one of the m{r,f}u{g,} lists
 */
#pragma pack(push, 1)
typedef struct __arc_state {
    arc_list_t head;
    size_t size; // note must be accessed only via atomic functions
    uint64_t count; // note must be accessed only via atomic functions
} arc_state_t;
#pragma pack(pop)

/* This structure represents an object that is stored in the cache. Consider
 * this structure private, don't access the fields directly. When creating
 * a new object, use the arc_object_create() function to allocate and initialize it. */
#pragma pack(push, 1)
typedef struct __arc_object {
    arc_state_t *state;
    arc_list_t head;
    size_t size;
    void *ptr;
    char buf[32];
    void *key;
    size_t klen;
    refcnt_node_t *node;
    int async;
    int locked;
} arc_object_t;
#pragma pack(pop)

/* The actual cache. */
struct __arc {
    struct __arc_ops *ops;
    hashtable_t *hash;

    size_t c, p;
    size_t cos;
    struct __arc_state mrug, mru, mfu, mfug;

    int needs_balance;
    pthread_mutex_t lock;

    refcnt_t *refcnt;
};


#define MAX(a, b) ( (a) > (b) ? (a) : (b) )
#define MIN(a, b) ( (a) < (b) ? (a) : (b) )

#define ARC_OBJ_BASE_SIZE(o) (sizeof(arc_object_t) + (((o)->key == (o)->buf) ? 0 : (o)->klen))

static int arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state);



static inline void
arc_list_init( arc_list_t * head )
{
    head->next = head->prev = head;
}

static inline void
arc_list_destroy(arc_t *cache, arc_list_t *head)
{
    arc_list_t *pos = (head)->next;
    while (pos && pos != (head)) {
        arc_list_t *tmp = pos;
        arc_object_t *obj = arc_list_entry(pos, arc_object_t, head);
        pos = pos->next;
        tmp->prev = tmp->next = NULL;
        release_ref(cache->refcnt, obj->node);
    }
}

static inline void
arc_list_insert(arc_list_t *list, arc_list_t *prev, arc_list_t *next)
{
    next->prev = list;
    list->next = next;
    list->prev = prev;
    prev->next = list;
}

static inline void
arc_list_splice(arc_list_t *prev, arc_list_t *next)
{
    next->prev = prev;
    prev->next = next;
}

static inline void
arc_list_remove(arc_list_t *head)
{
    if (head->prev && head->next) {
        arc_list_splice(head->prev, head->next);
    }
    head->next = head->prev = NULL;
}

static inline void
arc_list_prepend(arc_list_t *head, arc_list_t *list)
{
    arc_list_insert(head, list, list->next);
}

static inline void
arc_list_move_to_head(arc_list_t *head, arc_list_t *list)
{
    arc_list_t *next = head->next;
    arc_list_t *prev = head->prev;
    if (prev)
        prev->next = next;
    if (next)
        next->prev = prev;
    arc_list_prepend(head, list);
}

/* Return the LRU element from the given state. */
static inline arc_object_t *
arc_state_lru(arc_state_t *state)
{
    arc_list_t *head = state->head.prev;
    return arc_list_entry(head, arc_object_t, head);
}

/* Balance the lists so that we can fit an object with the given size into
 * the cache. */
static inline void
arc_balance(arc_t *cache)
{
    MUTEX_LOCK(&cache->lock);
    if (!cache->needs_balance) {
        MUTEX_UNLOCK(&cache->lock);
        return;
    }
    /* First move objects from MRU/MFU to their respective ghost lists. */
    while (cache->mru.size + cache->mfu.size > cache->c) {
        if (cache->mru.size > cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mru);
            arc_move(cache, obj, &cache->mrug);
        } else if (cache->mfu.size > cache->c - cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mfu);
            arc_move(cache, obj, &cache->mfug);
        } else {
            break;
        }
    }

    /* Then start removing objects from the ghost lists. */
    while (cache->mrug.size + cache->mfug.size > cache->c) {
        if (cache->mfug.size > cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mfug);
            arc_move(cache, obj, NULL);
        } else if (cache->mrug.size > cache->c - cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mrug);
            arc_move(cache, obj, NULL);
        } else {
            break;
        }
    }

    cache->needs_balance = 0;
    MUTEX_UNLOCK(&cache->lock);
}

void
arc_update_resource_size(arc_t *cache, arc_resource_t res, size_t size)
{
    arc_object_t *obj = (arc_object_t *)res;
    if (obj) {
        MUTEX_LOCK(&cache->lock);
        if (LIKELY(obj->state == &cache->mru || obj->state == &cache->mfu)) {
            ATOMIC_DECREASE(obj->state->size, obj->size);
            obj->size = ARC_OBJ_BASE_SIZE(obj) + size;
            ATOMIC_INCREASE(obj->state->size, obj->size);
        }
        cache->needs_balance = 1;
        MUTEX_UNLOCK(&cache->lock);
    }
}

/* Move the object to the given state. If the state transition requires,
* fetch, evict or destroy the object. */
static inline int
arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state)
{
    arc_state_t *obj_state = NULL;

    MUTEX_LOCK(&cache->lock);

    // In the first conditional we check If the object is being locked,
    // which means someone is fetching its value and we don't what
    // don't mess up with it. Whoever is fetching will also take care of moving it
    // to one of the lists (or dropping it)
    // NOTE: while the object is being fetched it doesn't belong
    //       to any list, so there is no point in going ahead
    //       also arc_balance() should never go through this object
    //       (since in none of the lists) so it won't be affected.
    //       The only call which would silently fail is arc_remove()
    //       but if the object is being fetched and need to be removed
    //       will be determined by who is fetching the object or by the
    //       next call to arc_balance() (which would anyway happen if
    //       the object will be put into the cache by the fetcher)
    //
    // In the second conditional instead we handle a specific corner case which
    // happens when concurring threads access an item which has been just fetched
    // but also dropped (so its state is NULL).
    // If a thread entering arc_lookup() manages to get the object out of the hashtable
    // before it's being deleted it will try putting the object to the mfu list without checking first
    // if it was already in a list or not (new objects should be first moved to the 
    // mru list and not the mfu one)
    if (UNLIKELY(obj->locked || (state == &cache->mfu && obj->state == NULL))) {
        MUTEX_UNLOCK(&cache->lock);
        return 0;
    }

    if (LIKELY(obj->state != NULL)) {

        if (LIKELY(obj->state == state)) {
            // short path for recurring keys
            // (those in the mfu list being hit again)
            if (LIKELY(state->head.next != &obj->head))
                arc_list_move_to_head(&obj->head, &state->head);
            MUTEX_UNLOCK(&cache->lock);
            return 0;
        }
        obj_state = obj->state;

        // if the state is not NULL
        // (and the object is not going to be being removed)
        // move the ^ (p) marker
        if (LIKELY(state != NULL)) {
            if (obj_state == &cache->mrug) {
                size_t csize = cache->mrug.size
                             ? (cache->mfug.size / cache->mrug.size)
                             : cache->mfug.size / 2;
                cache->p = MIN(cache->c, cache->p + MAX(csize, 1));
            } else if (obj_state == &cache->mfug) {
                size_t csize = cache->mfug.size
                             ? (cache->mrug.size / cache->mfug.size)
                             : cache->mrug.size / 2;
                cache->p = MAX(0, cache->p - MAX(csize, 1));
            }
        }

        ATOMIC_DECREASE(obj->state->size, obj->size);
        arc_list_remove(&obj->head);
        ATOMIC_DECREMENT(obj->state->count);
        obj->state = NULL;
    }

    if (state == NULL) {
        if (ht_delete_if_equals(cache->hash, (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
            release_ref(cache->refcnt, obj->node);
    } else if (state == &cache->mrug || state == &cache->mfug) {
        obj->async = 0;
        arc_list_prepend(&obj->head, &state->head);
        ATOMIC_INCREMENT(state->count);
        obj->state = state;
        ATOMIC_INCREASE(obj->state->size, obj->size);
    } else if (obj_state == NULL) {

        obj->locked = 1;
        
        // unlock the cache while the backend is fetching the data
        // (the object has been locked while being fetched so nobody
        // will change its state)
        MUTEX_UNLOCK(&cache->lock);
        size_t size = 0;
        int rc = cache->ops->fetch(obj->ptr, &size, cache->ops->priv);
        switch (rc) {
            case 1:
            case -1:
            {
                if (ht_delete_if_equals(cache->hash, (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
                    release_ref(cache->refcnt, obj->node);
                return rc;
            }
            default:
            {
                if (size >= cache->c) {
                    // the (single) object doesn't fit in the cache, let's return it
                    // to the getter without (re)adding it to the cache
                    if (ht_delete_if_equals(cache->hash, (void *)obj->key, obj->klen, obj, sizeof(arc_object_t)) == 0)
                        release_ref(cache->refcnt, obj->node);
                    return 1;
                }
                MUTEX_LOCK(&cache->lock);
                obj->size = ARC_OBJ_BASE_SIZE(obj) + size;
                arc_list_prepend(&obj->head, &state->head);
                ATOMIC_INCREMENT(state->count);
                obj->state = state;
                ATOMIC_INCREASE(obj->state->size, obj->size);
                cache->needs_balance = 1;
                break;
            }
        }
        // since this object is going to be put back into the cache,
        // we need to unmark it so that it won't be ignored next time
        // it's going to be moved to another list
        obj->locked = 0;
    } else {
        arc_list_prepend(&obj->head, &state->head);
        ATOMIC_INCREMENT(state->count);
        obj->state = state;
        ATOMIC_INCREASE(obj->state->size, obj->size);
    }
    MUTEX_UNLOCK(&cache->lock);
    return 0;
}

// this is called when the refcnt garbage collector actually requests us t
// release the memory for a node
static void
free_node_ptr_callback(void *node)
{
    // we don't need locks here .... nobody references obj anymore
    arc_object_t *obj = (arc_object_t *)node;

    if (obj->key != obj->buf)
        free(obj->key);

    free(obj);
}

// this is called when the refcount of the node drops to 0
// (and the node itself is being put into the garbage collector's
// queue by the refcnt manager)
static void
terminate_node_callback(refcnt_node_t *node, void *priv)
{
    arc_object_t *obj = (arc_object_t *)get_node_ptr(node);
    arc_t *cache = (arc_t *)priv;

    if (obj->ptr && cache->ops->evict)
        cache->ops->evict(obj->ptr, cache->ops->priv);

    obj->ptr = NULL;
    obj->state = NULL;
}

/* Create a new cache. */
arc_t *
arc_create(arc_ops_t *ops, size_t c, size_t cached_object_size)
{
    arc_t *cache = calloc(1, sizeof(arc_t));

    cache->ops = ops;

    cache->hash = ht_create(1<<16, 1<<22, NULL);

    cache->c = c >> 1;
    cache->p = cache->c >> 1;
    cache->cos = cached_object_size;

    arc_list_init(&cache->mrug.head);
    arc_list_init(&cache->mru.head);
    arc_list_init(&cache->mfu.head);
    arc_list_init(&cache->mfug.head);

    MUTEX_INIT_RECURSIVE(&cache->lock);

    cache->refcnt = refcnt_create(1<<8, terminate_node_callback, free_node_ptr_callback);
    return cache;
}

/* Destroy the given cache. Free all objects which remain in the cache. */
void
arc_destroy(arc_t *cache)
{
    arc_list_destroy(cache, &cache->mrug.head);
    arc_list_destroy(cache, &cache->mru.head);
    arc_list_destroy(cache, &cache->mfu.head);
    arc_list_destroy(cache, &cache->mfug.head);
    ht_destroy(cache->hash);
    refcnt_destroy(cache->refcnt);
    MUTEX_DESTROY(&cache->lock);
    free(cache);
}

static void *
retain_obj_cb(void *data, size_t dlen, void *user)
{
    retain_ref(((arc_t *)user)->refcnt, ((arc_object_t *)data)->node);
    return data;
}

void
arc_drop_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;
    if (obj) {
        arc_move(cache, obj, NULL);
        release_ref(cache->refcnt, obj->node);
    }
}

void
arc_remove(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = ht_get_deep_copy(cache->hash, (void *)key, len, NULL, retain_obj_cb, cache);
    if (obj) {
        arc_move(cache, obj, NULL);
        release_ref(cache->refcnt, obj->node);
    }
}

/* Lookup an object with the given key. */
void
arc_release_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;
    release_ref(cache->refcnt, obj->node);
}

/* Lookup an object with the given key. */
void
arc_retain_resource(arc_t *cache, arc_resource_t res)
{
    arc_object_t *obj = (arc_object_t *)res;

    retain_ref(cache->refcnt, obj->node);
}

/* Initialize a new object with this function. */
static inline arc_object_t *
arc_object_create(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = calloc(1, sizeof(arc_object_t) + cache->cos);

    arc_list_init(&obj->head);

    obj->node = new_node(cache->refcnt, obj, cache);
    if (len > sizeof(obj->buf))
        obj->key = malloc(len);
    else
        obj->key = obj->buf;
    memcpy(obj->key, key, len);
    obj->klen = len;

    obj->size = ARC_OBJ_BASE_SIZE(obj);

    obj->ptr = (void *)((char *)obj + sizeof(arc_object_t));

    return obj;
}

// the returned object is retained, the caller must call arc_release_resource(obj) to release it
arc_resource_t 
arc_lookup(arc_t *cache, const void *key, size_t len, void **valuep, int async)
{
    // NOTE: this is an atomic operation ensured by the hashtable implementation,
    //       we don't do any real copy in our callback but we just increase the refcount
    //       of the object (if found)
    arc_object_t *obj = ht_get_deep_copy(cache->hash, (void *)key, len, NULL, retain_obj_cb, cache);
    if (obj) {
        if (UNLIKELY(arc_move(cache, obj, &cache->mfu) == -1)) {
            fprintf(stderr, "Can't move the object into the cache\n");
            return NULL;
        }

        if (valuep)
            *valuep = obj->ptr;

        arc_balance(cache);
        return obj;
    }

    obj = arc_object_create(cache, key, len);
    if (!obj)
        return NULL;

    // let our cache user initialize the underlying object
    cache->ops->init(key, len, async, (arc_resource_t)obj, obj->ptr, cache->ops->priv);
    obj->async = async;

    retain_ref(cache->refcnt, obj->node);
    // NOTE: atomicity here is ensured by the hashtable implementation
    int rc = ht_set_if_not_exists(cache->hash, (void *)key, len, obj, sizeof(arc_object_t));
    switch(rc) {
        case -1:
            fprintf(stderr, "Can't set the new value in the internal hashtable\n");
            release_ref(cache->refcnt, obj->node);
            break;
        case 1:
            // the object has been created in the meanwhile
            release_ref(cache->refcnt, obj->node);
            // XXX - yes, we have to release it twice
            release_ref(cache->refcnt, obj->node);
            return arc_lookup(cache, key, len, valuep, async);
        case 0:
            /* New objects are always moved to the MRU list. */
            rc  = arc_move(cache, obj, &cache->mru);
            if (rc >= 0) {
                arc_balance(cache);
                *valuep = obj->ptr;
                return obj;
            }
            break;
        default:
            fprintf(stderr, "Unknown return code from ht_set_if_not_exists() : %d\n", rc);
            release_ref(cache->refcnt, obj->node);
            break;
    } 
    release_ref(cache->refcnt, obj->node);
    return NULL;
}

size_t
arc_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.size) + ATOMIC_READ(cache->mfu.size);
}

size_t
arc_mru_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.size);
}

size_t
arc_mfu_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mfu.size);
}

size_t
arc_mrug_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mrug.size);
}

size_t
arc_mfug_size(arc_t *cache)
{
    return ATOMIC_READ(cache->mfug.size);
}

void
arc_get_size(arc_t *cache, size_t *mru_size, size_t *mfu_size, size_t *mrug_size, size_t *mfug_size)
{
    *mru_size = ATOMIC_READ(cache->mru.size);
    *mfu_size = ATOMIC_READ(cache->mfu.size);
    *mrug_size = ATOMIC_READ(cache->mrug.size);
    *mfug_size = ATOMIC_READ(cache->mfug.size);
}

uint64_t
arc_count(arc_t *cache)
{
    return ATOMIC_READ(cache->mru.count) + ATOMIC_READ(cache->mfu.count) +
           ATOMIC_READ(cache->mrug.count) + ATOMIC_READ(cache->mfug.count);
}

void *
arc_get_resource_ptr(arc_resource_t res)
{
    return ((arc_object_t *)res)->ptr;
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
