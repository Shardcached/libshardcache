
#include "arc.h"
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include <memory.h>
#include <stddef.h>
#include <hashtable.h>

#include <pthread.h>

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
    for (pos = (head)->next; pos != (head); pos = pos->next)

#define arc_list_each_prev(pos, head) \
    for (pos = (head)->prev; pos != (head); pos = pos->prev)

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
    arc_list_splice(head->prev, head->next);
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
    unsigned long size;
    arc_list_t head;
} arc_state_t;

/* This structure represents an object that is stored in the cache. Consider
 * this structure private, don't access the fields directly. When creating
 * a new object, use the arc_object_create() function to allocate and initialize it. */
typedef struct __arc_object {
    arc_state_t *state;
    arc_list_t head, hash;
    unsigned long size;
    void *ptr;
    pthread_mutex_t lock;
} arc_object_t;

/* The actual cache. */
struct __arc {
    struct __arc_ops *ops;
    hashtable_t *hash;
    
    unsigned long c, p;
    struct __arc_state mrug, mru, mfu, mfug;

    pthread_mutex_t lock;
};


#define MAX(a, b) ( (a) > (b) ? (a) : (b) )
#define MIN(a, b) ( (a) < (b) ? (a) : (b) )

/* Initialize a new object with this function. */
static arc_object_t *arc_object_create(unsigned long size, void *ptr)
{
    arc_object_t *obj = calloc(1, sizeof(arc_object_t));
    obj->state = NULL;
    obj->size = size;
    obj->ptr = ptr;

    arc_list_init(&obj->head);
    arc_list_init(&obj->hash);
    pthread_mutex_init(&obj->lock, NULL);
    return obj;
}

/* Forward-declaration needed in arc_move(). */
static void arc_balance(arc_t *cache, unsigned long size);

/* Move the object to the given state. If the state transition requires,
* fetch, evict or destroy the object. */
static arc_object_t *arc_move(arc_t *cache, arc_object_t *obj, arc_state_t *state)
{
    pthread_mutex_lock(&cache->lock);
    if (obj->state) {
        obj->state->size -= obj->size;
        arc_list_remove(&obj->head);
    }

    if (state == NULL) {
        /* The object is being removed from the cache, destroy it. */
        arc_list_remove(&obj->hash);
        cache->ops->destroy(obj->ptr, cache->ops->priv);
        free(obj);
        pthread_mutex_unlock(&cache->lock);
        return NULL;
    } else {
        if (state == &cache->mrug || state == &cache->mfug) {
            /* The object is being moved to one of the ghost lists, evict
             * the object from the cache. */
            cache->ops->evict(obj->ptr, cache->ops->priv);
        } else if (obj->state != &cache->mru && obj->state != &cache->mfu) {
            /* The object is being moved from one of the ghost lists into
             * the MRU or MFU list, fetch the object into the cache. */
            arc_balance(cache, obj->size);
            // unlock the mutex while the backend is fetching the data
            pthread_mutex_unlock(&cache->lock);
            pthread_mutex_lock(&obj->lock);
            if (cache->ops->fetch(obj->ptr, cache->ops->priv)) {
                pthread_mutex_unlock(&obj->lock);
                pthread_mutex_lock(&cache->lock);
                /* If the fetch fails, put the object back to the list
                 * it was in before. */
                if (obj->state) {
                    obj->state->size += obj->size;
                    arc_list_prepend(&obj->head, &obj->state->head);
                }
                pthread_mutex_unlock(&cache->lock);
                return NULL;
            }
            pthread_mutex_unlock(&obj->lock);
            pthread_mutex_lock(&cache->lock);
        }

        arc_list_prepend(&obj->head, &state->head);

        obj->state = state;
        obj->state->size += obj->size;
    }
    
    pthread_mutex_unlock(&cache->lock);
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
static void arc_balance(arc_t *cache, unsigned long size)
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
            arc_move(cache, obj, NULL);
        } else if (cache->mrug.size > 0) {
            arc_object_t *obj = arc_state_lru(&cache->mrug);
            arc_move(cache, obj, NULL);
        } else {
            break;
        }
    }
}


/* Create a new cache. */
arc_t *arc_create(arc_ops_t *ops, unsigned long c)
{
    arc_t *cache = calloc(1, sizeof(arc_t));

    cache->ops = ops;
    
    cache->hash = ht_create(8192);

    cache->c = c;
    cache->p = c >> 1;

    arc_list_init(&cache->mrug.head);
    arc_list_init(&cache->mru.head);
    arc_list_init(&cache->mfu.head);
    arc_list_init(&cache->mfug.head);
    
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&cache->lock, &attr);
    pthread_mutexattr_destroy(&attr);

    return cache;
}

/* Destroy the given cache. Free all objects which remain in the cache. */
void arc_destroy(arc_t *cache)
{
    arc_list_t *iter;
    
    ht_destroy(cache->hash);
    
    arc_list_each(iter, &cache->mrug.head) {
        arc_object_t *obj = arc_list_entry(iter, arc_object_t, head);
		arc_move(cache, obj, NULL);
    }
    arc_list_each(iter, &cache->mru.head) {
        arc_object_t *obj = arc_list_entry(iter, arc_object_t, head);
		arc_move(cache, obj, NULL);
    }
    arc_list_each(iter, &cache->mfu.head) {
        arc_object_t *obj = arc_list_entry(iter, arc_object_t, head);
		arc_move(cache, obj, NULL);
    }
    arc_list_each(iter, &cache->mfug.head) {
        arc_object_t *obj = arc_list_entry(iter, arc_object_t, head);
		arc_move(cache, obj, NULL);
    }

    free(cache);
}

void arc_remove(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = ht_get(cache->hash, (void *)key, len);
    if (obj) {
        arc_move(cache, obj, NULL);
    }
}

/* Lookup an object with the given key. */
void  *arc_lookup(arc_t *cache, const void *key, size_t len)
{
    arc_object_t *obj = ht_get(cache->hash, (void *)key, len);

    if (obj) {
        // XXX - possible race condition on obj->state
        if (obj->state == &cache->mru || obj->state == &cache->mfu) {
            /* Object is already in the cache, move it to the head of the
             * MFU list. */
            obj = arc_move(cache, obj, &cache->mfu);
            return obj->ptr;
        } else if (obj->state == &cache->mrug) {
            cache->p = MIN(cache->c, cache->p + MAX(cache->mfug.size / cache->mrug.size, 1));
            obj = arc_move(cache, obj, &cache->mfu);
            return obj->ptr;
        } else if (obj->state == &cache->mfug) {
            cache->p = MAX(0, cache->p - MAX(cache->mrug.size / cache->mfug.size, 1));
            obj = arc_move(cache, obj, &cache->mfu);
            return obj->ptr;
        } else {
            assert(0);
        }
    } else {
        void *ptr = cache->ops->create(key, len, cache->ops->priv);
        arc_object_t *obj = arc_object_create(rand()%100, ptr);
        if (!obj)
            return NULL;

        /* New objects are always moved to the MRU list. */
        arc_object_t *moved = arc_move(cache, obj, &cache->mru);
        if (moved) {
            ht_set(cache->hash, (void *)key, len, obj);
            return moved->ptr;
        } else {
            cache->ops->destroy(obj->ptr, cache->ops->priv);
            free(obj);
        }
    }
    return NULL;
}

