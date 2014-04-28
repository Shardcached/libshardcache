#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <limits.h>
#include <memory.h>
#include <stddef.h>

#include <hashtable.h>
#include <refcnt.h>

#include "arc.h"

#include "atomic.h"

#define LIKELY(__e) __builtin_expect((__e), 1)
#define UNLIKELY(__e) __builtin_expect((__e), 0)

/**********************************************************************
 * Simple double-linked list, inspired by the implementation used in the
 * linux kernel.
 */
typedef struct __arc_list {
    struct __arc_list *prev, *next;
} arc_list_t;

#define arc_list_entry(ptr, type, field) \
    ((type*) (((char*)ptr) - offsetof(type, field)))

#define arc_list_each(pos, head) \
    for (pos = (head)->next; pos && pos != (head); pos = pos->next)

#define arc_list_each_prev(pos, head) \
    for (pos = (head)->prev; pos && pos != (head); pos = pos->prev)

static inline void
arc_list_init( arc_list_t * head )
{
    head->next = head->prev = head;
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


/**********************************************************************
 * The arc state represents one of the m{r,f}u{g,} lists
 */
typedef struct __arc_state {
    size_t size;
    arc_list_t head;
} arc_state_t;

/* This structure represents an object that is stored in the cache. Consider
 * this structure private, don't access the fields directly. When creating
 * a new object, use the arc_object_create() function to allocate and initialize it. */
typedef struct __arc_object {
    arc_state_t *state;
    arc_list_t head;
    size_t size;
    void *ptr;
    void *key;
    size_t klen;
    pthread_mutex_t lock;
    refcnt_node_t *node;
    arc_t *cache;
    int async;
} arc_object_t;

/* The actual cache. */
struct __arc {
    struct __arc_ops *ops;
    hashtable_t *hash;

    size_t c, p;
    struct __arc_state mrug, mru, mfu, mfug;

    pthread_mutex_t lock;

    refcnt_t *refcnt;
};


#define MAX(a, b) ( (a) > (b) ? (a) : (b) )
#define MIN(a, b) ( (a) < (b) ? (a) : (b) )

static int arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state);

/* Initialize a new object with this function. */
static arc_object_t *arc_object_create(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = calloc(1, sizeof(arc_object_t));
    obj->state = NULL;
    obj->cache = cache;

    arc_list_init(&obj->head);

    MUTEX_INIT_RECURSIVE(&obj->lock);

    obj->node = new_node(cache->refcnt, obj);
    obj->key = malloc(len);
    memcpy(obj->key, key, len);
    obj->klen = len;

    obj->size = sizeof(arc_object_t) + len;

    //retain_ref(cache->refcnt, obj->node);

    return obj;
}

/* Return the LRU element from the given state. */
static arc_object_t *arc_state_lru(arc_state_t *state)
{
    arc_list_t *head = state->head.prev;
    return arc_list_entry(head, arc_object_t, head);
}

/* Balance the lists so that we can fit an object with the given size into
 * the cache. */
static void arc_balance(arc_t *cache, size_t size)
{
    /* First move objects from MRU/MFU to their respective ghost lists. */
    while (cache->mru.size + cache->mfu.size + size > cache->c) {
        if (cache->mru.size > cache->p) {
            arc_object_t *obj = arc_state_lru(&cache->mru);
            arc_move(cache, obj, &cache->mrug);
        } else if (cache->mfu.size > 0) {
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
            arc_remove(cache, obj->key, obj->klen);
        } else if (cache->mrug.size > 0) {
            arc_object_t *obj = arc_state_lru(&cache->mrug);
            arc_remove(cache, obj->key, obj->klen);
        } else {
            break;
        }
    }
}

void arc_update_size(arc_t *cache, void *key, size_t klen, size_t size)
{
    arc_object_t *obj = ht_get(cache->hash, key, klen, NULL);
    if (obj) {
        MUTEX_LOCK(&obj->lock);
        if (obj && (obj->state == &cache->mru || obj->state == &cache->mfu))
        {
            MUTEX_LOCK(&cache->lock);
            obj->state->size -= obj->size;
            obj->size = sizeof(arc_object_t) + obj->klen + size;
            obj->state->size += obj->size;
            MUTEX_UNLOCK(&cache->lock);
        }
        MUTEX_UNLOCK(&obj->lock);
    }
}

/* Move the object to the given state. If the state transition requires,
* fetch, evict or destroy the object. */
static int arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state)
{
    arc_state_t *obj_state = NULL;

    MUTEX_LOCK(&cache->lock);
    MUTEX_LOCK(&obj->lock);

    if (obj->state) {

        obj_state = obj->state;

        // if the state is not NULL
        // (and the object is not going to be being removed)
        // move the ^ (p) marker
        if (state) {
            if (obj_state == &cache->mrug)
                cache->p = MIN(cache->c, cache->p + MAX(cache->mrug.size ? (cache->mfug.size / cache->mrug.size) : cache->mfug.size/2, 1));
            else if (obj_state == &cache->mfug)
                cache->p = MAX(0, cache->p - MAX(cache->mfug.size ? (cache->mrug.size / cache->mfug.size) : cache->mrug.size/2, 1));
        }

        obj->state->size -= obj->size;
        arc_list_remove(&obj->head);
        obj->state = NULL;
    }

    if (state == NULL) {
        /* The object is being removed from the cache, destroy it. */
        MUTEX_UNLOCK(&obj->lock);
        release_ref(cache->refcnt, obj->node);

        MUTEX_UNLOCK(&cache->lock);
        return -1;
    } else {
        if (state == &cache->mrug || state == &cache->mfug) {
            /* The object is being moved to one of the ghost lists, evict
             * the object from the cache. */
            if (obj->ptr)
                cache->ops->evict(obj->ptr, cache->ops->priv);

            obj->async = 0;
            arc_list_prepend(&obj->head, &state->head);
            obj->state = state;
            obj->state->size += obj->size;

        } else if (obj_state != &cache->mru && obj_state != &cache->mfu) {
            /* The object is being moved from one of the ghost lists into
             * the MRU or MFU list, fetch the object into the cache. */

            // release the lock (if any) when fetching (since might take long)
            // the object is anyway locked already by our caller (arc_lookup())

            // unlock the mutex while the backend is fetching the data
            MUTEX_UNLOCK(&cache->lock);
            size_t size = 0;
            int rc = cache->ops->fetch(obj->ptr, &size, cache->ops->priv);

            if (rc == 1) {
                // don't cache the object which has been retrieved
                // using the fetch callback
                ht_delete(cache->hash, obj->key, obj->klen, NULL, NULL);
                MUTEX_UNLOCK(&obj->lock);
                release_ref(cache->refcnt, obj->node);
                return 0;
            } else if (rc == -1) {
                /* If the fetch fails we can drop the object without
                 * calling arc_balance() since we don't want a not-existing key
                 * to affect the cache.
                 * Note though that the ^ (p) marker has been updated since this
                 * item was in a ghost list, so at next arc_balance() call it
                 * will be taken into account.
                 * This is a desired behaviour because it's still an indication
                 * of 'frequency'/'recency' in terms of cache usage
                 * (regardless of the actual presence of the key, if it
                 * was in a ghost list it surely existed at some point in time)
                 * and we want to balance the cache accordingly, it's just easier
                 * (and still safe) to postpone it until next cache-related operation
                 */
                if (obj_state) {
                    /* The object is being removed from the cache, destroy it. */
                    ht_delete(cache->hash, obj->key, obj->klen, NULL, NULL);
                    release_ref(cache->refcnt, obj->node);
                }
                MUTEX_UNLOCK(&obj->lock);
                return -1;
            } else if (size >= cache->c) {
                // the object doesn't fit in the cache, let's return it
                // to the getter without (re)adding it to the cache
                release_ref(cache->refcnt, obj->node);
                MUTEX_LOCK(&cache->lock);
            } else {
                obj->size = sizeof(arc_object_t) + obj->klen + size;
                MUTEX_LOCK(&cache->lock);

                arc_list_prepend(&obj->head, &state->head);
                obj->state = state;
                obj->state->size += obj->size;
            }
        } else {
            arc_list_prepend(&obj->head, &state->head);
            obj->state = state;
            obj->state->size += obj->size;
        }

        arc_balance(cache, obj->size);
    }

    MUTEX_UNLOCK(&obj->lock);
    MUTEX_UNLOCK(&cache->lock);
    return 0;
}

static void free_node_ptr_callback(void *node) {
    // we don't need locks here .... nobody references obj anymore
    arc_object_t *obj = (arc_object_t *)node;

    if (obj->key)
        free(obj->key);

    MUTEX_DESTROY(&obj->lock);

    free(obj);
}

static void terminate_node_callback(refcnt_node_t *node, int concurrent) {
    arc_object_t *obj = (arc_object_t *)get_node_ptr(node);
    MUTEX_LOCK(&obj->lock);

    if (obj->key) {
        ht_delete(obj->cache->hash, obj->key, obj->klen, NULL, NULL);
    }
    if (obj->ptr && obj->cache->ops->destroy)
        obj->cache->ops->destroy(obj->ptr, obj->cache->ops->priv);

    obj->ptr = NULL;
    obj->state = NULL;
    MUTEX_UNLOCK(&obj->lock);
}

/* Create a new cache. */
arc_t *arc_create(arc_ops_t *ops, size_t c)
{
    arc_t *cache = calloc(1, sizeof(arc_t));

    cache->ops = ops;

    cache->hash = ht_create(1<<16, 1<<22, NULL);

    cache->c = c;
    cache->p = c >> 1;

    arc_list_init(&cache->mrug.head);
    arc_list_init(&cache->mru.head);
    arc_list_init(&cache->mfu.head);
    arc_list_init(&cache->mfug.head);

    MUTEX_INIT_RECURSIVE(&cache->lock);

    cache->refcnt = refcnt_create(1<<8, terminate_node_callback, free_node_ptr_callback);
    return cache;
}
static void arc_list_destroy(arc_t *cache, arc_list_t *head) {
    arc_list_t *pos = (head)->next;
    while (pos && pos != (head)) {
        arc_list_t *tmp = pos;
        arc_object_t *obj = arc_list_entry(pos, arc_object_t, head);
        pos = pos->next;
        tmp->prev = tmp->next = NULL;
        release_ref(cache->refcnt, obj->node);
    }
}


/* Destroy the given cache. Free all objects which remain in the cache. */
void arc_destroy(arc_t *cache)
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

void arc_remove(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = NULL;
    void *objptr = NULL;
    ht_delete(cache->hash, (void *)key, len, &objptr, NULL);
    if (objptr) {
        obj = (arc_object_t *)objptr;
        MUTEX_LOCK(&obj->lock);
        if (obj && obj->state) {
            MUTEX_UNLOCK(&obj->lock);
            arc_move(cache, obj, NULL);
        } else {
            MUTEX_UNLOCK(&obj->lock);
        }
    }
}

/* Lookup an object with the given key. */
void arc_release_resource(arc_t *cache, arc_resource_t *res) {
    arc_object_t *obj = (arc_object_t *)res;
    release_ref(cache->refcnt, obj->node);
}

/* Lookup an object with the given key. */
void arc_retain_resource(arc_t *cache, arc_resource_t *res) {
    arc_object_t *obj = (arc_object_t *)res;

    MUTEX_LOCK(&obj->lock);
    retain_ref(cache->refcnt, obj->node);
    MUTEX_UNLOCK(&obj->lock);
}

arc_resource_t  arc_lookup(arc_t *cache, const void *key, size_t len, void **valuep, int async)
{
    MUTEX_LOCK(&cache->lock);
    arc_object_t *obj = ht_get(cache->hash, (void *)key, len, NULL);
    if (obj) {
        retain_ref(cache->refcnt, obj->node);
        if (async && obj->async) {
            MUTEX_UNLOCK(&cache->lock);
            *valuep = obj->ptr;
            return obj;
        }

        // we don't need to lock the object here since
        // its status can't be updated without acquiring
        // the cache-level lock
        arc_state_t *state = obj->state;

        MUTEX_UNLOCK(&cache->lock);

        if (UNLIKELY(!state)) {
            release_ref(cache->refcnt, obj->node);
            fprintf(stderr, "Found an object with no state in the hashtable ?!\n");
            return NULL;
        }

        if (UNLIKELY(arc_move(cache, obj, &cache->mfu) != 0)) {
            release_ref(cache->refcnt, obj->node);
            fprintf(stderr, "Can't move the object into the cache\n");
            return NULL;
        }

        if (valuep)
            *valuep = obj->ptr;

        return obj;
    }

    obj = arc_object_create(cache, key, len);
    if (!obj)
        return NULL;
    void *ptr = cache->ops->create(key, len, async, (arc_resource_t *)obj, cache->ops->priv);
    obj->ptr = ptr;
    obj->async = async;

    if (!obj) {
        MUTEX_UNLOCK(&cache->lock);
        return NULL;
    }

    retain_ref(cache->refcnt, obj->node);
    MUTEX_LOCK(&obj->lock);
    ht_set(cache->hash, (void *)key, len, obj, sizeof(arc_object_t));

    /* New objects are always moved to the MRU list. */
    MUTEX_UNLOCK(&cache->lock);
    if (arc_move(cache, obj, &cache->mru) == 0) {
        *valuep = obj->ptr;
        MUTEX_UNLOCK(&obj->lock);
        // the object is retained, the caller must call
        // arc_release_resource(obj) to release it
        return obj;
    } else {
        release_ref(cache->refcnt, obj->node);
    }

    MUTEX_LOCK(&cache->lock);
    // failed to add the object to the cache, probably the key doesn't
    // exist in the storage
    ht_delete(cache->hash, (void *)key, len, NULL, NULL);
    MUTEX_UNLOCK(&obj->lock);
    MUTEX_UNLOCK(&cache->lock);
    release_ref(cache->refcnt, obj->node);

    return NULL;
}

size_t arc_size(arc_t *cache)
{
    MUTEX_LOCK(&cache->lock);
    size_t ret = cache->mru.size + cache->mfu.size;
    MUTEX_UNLOCK(&cache->lock);
    return ret;
}
