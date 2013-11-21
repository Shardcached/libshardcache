#ifndef __ARC_H__
#define __ARC_H__
#include <sys/types.h>

typedef struct __arc arc_t;

typedef struct __arc_ops {
    /* Create a new object. The size of the new object must be know at
     * this time. Use the arc_object_init() function to initialize
     * the arc_object structure. */
    void *(*create) (const void *key, size_t len, void *priv);
    
    /* Fetch the data associated with the object. */
    int (*fetch) (void *obj, void *priv);
    
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
arc_t *arc_create(arc_ops_t *ops, unsigned long c);
void arc_destroy(arc_t *cache);

/* Lookup an object in the cache. The cache automatically allocates and
 * fetches the object if it does not exists yet. */
void *arc_lookup(arc_t *cache, const void *key, size_t len);


#endif /* __ARC_H__ */
