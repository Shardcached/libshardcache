/**
 * @file arc.h.h
 * @author Andrea Guzzo
 * @brief ARC cache implementation
 *
 * @note Iniitially based on https://github.com/wereHamster/adaptive-replacement-cache.git
 */
#ifndef __ARC_H__
#define __ARC_H__
#include <sys/types.h>

typedef struct __arc arc_t;

typedef void * arc_resource_t;

typedef struct __arc_ops {
    /**
     * @brief Create a new object.
     *
     * The size of the new object must be know at
     * this time. Use the arc_object_init() function to initialize
     * the arc_object structure.
     * */
    void *(*create) (const void *key, size_t len, void *priv);
    
    //! Fetch the data associated with the object.
    size_t (*fetch) (void *obj, void *priv);
    
    /**
     * @brief This function is called when the cache is full and we need to evict
     * objects from the cache.
     *
     * The callback CAN free all data associated with the object but MUST keep the
     * object itself alive and DO NOT free it now. the destroy() callback will be
     * called when the object can be fully released
     * */
    void (*evict) (void *obj, void *priv);
    
    /** 
     * @brief This function is called when the object is completely removed from
     * the cache directory.
     *
     * The callback CAN free the object data and the object itself.
     * */
    void (*destroy) (void *obj, void *priv);

    //! Pointer to private data which will provided to all callbacks
    void *priv;
} arc_ops_t;

/**
 * @brief Create an ARC cache instance
 *
 * @param ops : A valid pointer to an initialized arc_ops_t structure
 * @param c   : The size of the cache
 * @return    : A valid pointer to an initialized arc_t structure
 */
arc_t *arc_create(arc_ops_t *ops, size_t c);

/**
 * @brief Release an existing ARC cache instance
 * @param cache : A valid pointer to an initialized arc_t structure
 */
void arc_destroy(arc_t *cache);

/**
 * @brief Lookup an object in the cache.
 *
 * The cache automatically allocates and fetches the object
 * if it does not exists yet.
 *
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param key    : The key
 * @param len    : The length of the key
 * @param valuep : a reference to the pointer where to copy the retrieved value
 * @return An opaque ARC resource which needs to be released using arc_release_resource()
 *
 * @note ARC resources are internally reference counted. So when giving back to the caller
 *       a cached object (which is contained in an ARC resource) it will be retained until
 *       the caller releases it using the arc_release_resource() function
 */
arc_resource_t arc_lookup(arc_t *cache, const void *key, size_t len, void **valuep);

/**
 * @brief Release the resource previously alloc'd by arc_lookup()
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param res    : An opaque ARC resource previously returned by arc_lookup()
 */
void arc_release_resource(arc_t *cache, arc_resource_t *res);

/**
 * @brief Force eviction of an item
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param key    : The key
 * @param len    : The length of the key
 */
void arc_remove(arc_t *cache, const void *key, size_t len);

/**
 * @brief Returns the actual cache size (in bytes)
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_size(arc_t *cache);


#endif /* __ARC_H__ */
