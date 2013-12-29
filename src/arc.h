#ifndef __ARC_H__
#define __ARC_H__
#include <sys/types.h>

typedef struct __arc arc_t;

typedef void * arc_resource_t;

typedef struct __arc_ops {
    /* Create a new object. The size of the new object must be know at
     * this time. Use the arc_object_init() function to initialize
     * the arc_object structure. */
    void *(*create) (const void *key, size_t len, void *priv);
    
    /* Fetch the data associated with the object. */
    size_t (*fetch) (void *obj, void *priv);
    
    /* This function is called when the cache is full and we need to evict
     * objects from the cache. Free all data associated with the object. */
    void (*evict) (void *obj, void *priv);
    
    /* This function is called when the object is completely removed from
     * the cache directory. Free the object data and the object itself. */
    void (*destroy) (void *obj, void *priv);

    /* Pointer to private data which will provided to all callbacks */
    void *priv;
} arc_ops_t;

/* Functions to create and destroy the cache. */
arc_t *arc_create(arc_ops_t *ops, size_t c);
void arc_destroy(arc_t *cache);

/* Lookup an object in the cache. The cache automatically allocates and
 * fetches the object if it does not exists yet.
 * The caller MUST call arc_release_reasource() passing the pointer returned
 * by this function when done with the returned value.
 * */
arc_resource_t arc_lookup(arc_t *cache, const void *key, size_t len, void **valuep);

/* Release the resource previously alloc'd by arc_lookup()
 * */
void arc_release_resource(arc_t *cache, arc_resource_t *res);

/* Force eviction of an item */
void arc_remove(arc_t *cache, const void *key, size_t len);

/* Returns the actual cache size (in bytes) */
size_t arc_size(arc_t *cache);


#endif /* __ARC_H__ */
