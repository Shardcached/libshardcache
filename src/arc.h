/**
 * @file arc.h.h
 * @author Andrea Guzzo
 * @brief ARC cache implementation
 *
 * @note Iniitially based on https://github.com/wereHamster/adaptive-replacement-cache.git
 */
#ifndef SHARDCACHE_ARC_H
#define SHARDCACHE_ARC_H
#include <sys/types.h>
#include "shardcache.h"

typedef struct _arc arc_t;

typedef void * arc_resource_t;

typedef struct _arc_ops {
    /**
     * @brief Initialize a new object.
     *
     * The size of the new object has been provided to arc_create()
     * ptr will point to a prealloc'd memory where the cached object is stored
     * and needs to be initialized by this callback
     */
    void (*init) (const void *key, size_t klen, int async, arc_resource_t res, void *ptr, void *priv);
    
    /**
     * @brief Fetch the data associated with the object.
     * @return 0 on success and *size is set to the actual object size.
     *         1 if the object was retrieved successfully but it shouldn't be
     *           kept in the cache, *size is set to the actual object size.
     *        -1 in case of errors, *size will not be modified
     */
    int (*fetch) (void *obj, size_t *size, void *priv);

    int (*fetch_multi) (void **objs, size_t *sizes, int *statuses, int num_objects, void *priv);

    void (*store) (void *obj, void *data, size_t size, void *priv);
    
    /**
     * @brief This function is called when the cache is full and we need to evict
     * objects from the cache.
     *
     * The callback CAN free all data associated with the object and the object itself
     */
    void (*evict) (void *obj, void *priv);

    //! Pointer to private data which will provided to all callbacks
    void *priv;
} arc_ops_t;

/**
 * @brief Create an ARC cache instance
 *
 * @param ops : A valid pointer to an initialized arc_ops_t structure
 * @param c   : The size of the cache
 * @param mode : 0 for strict mode, 1 for loose_mode
 * @return    : A valid pointer to an initialized arc_t structure
 */
arc_t *arc_create(arc_ops_t *ops, size_t c, size_t cached_object_size, uint64_t *lists_size[4], arc_mode_t mode);

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
 * @param klen   : The length of the key
 * @param valuep : a reference to the pointer where to copy the retrieved value
 * @return An opaque ARC resource which needs to be released using arc_release_resource()
 *         once the object is not going to be referenced anymore
 *
 * @note ARC resources are internally reference counted. So when giving back to the caller
 *       a cached object (which is contained in an ARC resource) it will be retained until
 *       the caller releases it using the arc_release_resource() function
 */
arc_resource_t arc_lookup(arc_t *cache, const void *key, size_t klen, void **valuep, int async);

arc_resource_t arc_lookup_nofetch(arc_t *cache, const void *key, size_t len, void **valuep);

int arc_lookup_multi(arc_t *cache,
                     void **keys,
                     size_t *klens,
                     arc_resource_t *resources,
                     int num_keys);

int arc_load(arc_t *cache, const void *key, size_t klen, void *valuep, size_t vlen);

/**
 * @brief Release the resource previously alloc'd by arc_lookup()
 * @note  The retain count will be decreased by 1.\nThe underlying
 *        resources will be free'd if the retain reaches 0
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param res    : An opaque ARC resource previously returned by arc_lookup()
 */
void arc_release_resource(arc_t *cache, arc_resource_t res);

/**
 * @brief Retain an ARC resource preventing it from being released
 * @note  The retain count will be increased by 1
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param res    : An opaque ARC resource to retain
 * @note Retained resources MUST be released calling arc_release_resource()
 *       once it is not going to be referenced anymore
 */
void arc_retain_resource(arc_t *cache, arc_resource_t res);

void arc_drop_resource(arc_t *cache, arc_resource_t res);

void *arc_get_resource_ptr(arc_resource_t res);

/**
 * @brief Force complete removal of an item from the cache
 * @note the item will be completely removed from the cache and not moved
 *       to a ghost list first
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param key    : The key
 * @param klen   : The length of the key
 */
void arc_remove(arc_t *cache, const void *key, size_t klen);

/**
 * @brief Force eviction of an item which, if in the mru or mfu list,
 *        will be moved to the related ghost list (otherwise it will be untouched)
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param key    : The key
 * @param klen   : The length of the key
 */
void arc_evict(arc_t *cache, const void *key, size_t len);

/**
 * @brief Update the size of a cached object (if any)
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param res    : An opaque ARC resource previously returned by arc_lookup()
 * @param size   : The new size of the cached object
 */
void arc_update_resource_size(arc_t *cache, arc_resource_t res, size_t size);

/**
 * @brief Returns the actual cache size (in bytes)
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_size(arc_t *cache);

/**
 * @brief Returns the size of the mru list
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_mru_size(arc_t *cache);

/**
 * @brief Returns the size of the mfu list
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_mfu_size(arc_t *cache);

/**
 * @brief Returns the size of the mrug list
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_mrug_size(arc_t *cache);

/**
 * @brief Returns the size of the mfug list
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @return The actual size of the cache
 */
size_t arc_mfug_size(arc_t *cache);

/**
 * @brief Get the size for all the lists at once
 * @param cache  : A valid pointer to an initialized arc_t structure
 * @param mru_size
 * @param mfu_size
 * @param mrug_size
 * @param mfug_size
 */
void arc_get_size(arc_t *cache,
                  size_t *mru_size,
                  size_t *mfu_size,
                  size_t *mrug_size,
                  size_t *mfug_size);

/**
 * @brief Returns the number of items actually cached
 * @param cache : A valid pointer to an initialized arc_t structure
 * @return The total number of items in the cache
 */
uint64_t arc_count(arc_t *cache);

void arc_set_mode(arc_t *cache, arc_mode_t mode);

#endif /* SHARDCACHE_ARC_H */

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
