#ifndef __SHARDCACHE_H__
#define __SHARDCACHE_H__

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <sys/types.h>

#define SHARDCACHE_PORT_DEFAULT 9874

/**
 * @brief Opaque structure representing the actual shardcache instance
 */
typedef struct __shardcache_s shardcache_t;

typedef struct {
    uint32_t ngets;
    uint32_t nsets;
    uint32_t ndels;
    uint32_t nevicts;
    uint32_t ncache_misses;
    uint32_t nnot_found;
} shardcache_stats_t;


void shardcache_get_stats(shardcache_t *cache, shardcache_stats_t *stats);
void shardcache_clear_stats(shardcache_t *cache);

/**
 * @brief Callback to provide the value for a given key.
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache.
 * @return A pointer to initialized memory containing the stored value.
 *         NOTE: The returned value is a volatile copy and the caller
 *               MUST release its resources
 */
typedef void *(*shardcache_fetch_item_callback_t)(void *key, size_t len, size_t *vlen, void *priv);

/**
 * @brief Callback to store a new value for a given key.
 *        The shardcache instance will call this callback
 *        if a new value needs to be set in the underlying storage
 */
typedef int (*shardcache_store_item_callback_t)(void *key, size_t len, void *value, size_t vlen, void *priv);

/**
 * @brief Callback to remove an existing value for a given key.
 *        The shardcache instance will call this callback
 *        if the value for a given key needs to be removed from the underlying storage
 *        NOTE: the value might still be living in the cache, so only the underlying storage
 *              should be updated accordingly to the removal request. The value pointer
 *              shouldn't still be released at this point.
 *              The shardcache_remove_item_callback_t will be called by the shardcache
 *              instance when the memory associated to the value can be safely released
 */
typedef int (*shardcache_remove_item_callback_t)(void *key, size_t len, void *priv);

/**
 * @brief This callback is called when creating the shardcache instance
 *        to initialize the underlying storage.
 * @return if a pointer is returned by the callback, the same pointer will
 *         be passed as 'priv' to all further calls to the storage
 */
typedef void *(*shardcache_init_storage_callback_t)(const char **options);

/**
 * @brief Callback called when the shardcache is going to be destroyed
 *        so that the underlying storage can take all the necessary
 *        actions before exiting.
 *        If a pointer was returned from the init_storage() callback
 *        it will be passed to this function as well
 */
typedef void (*shardcache_destroy_storage_callback_t)(void *priv);

/**
 * @brief Structure holding all the callback pointers required
 *        by shardcache to interact with underlying storage
 */
typedef struct __shardcache_storage_s {
    shardcache_init_storage_callback_t     init_storage;
    shardcache_destroy_storage_callback_t  destroy_storage;
    shardcache_fetch_item_callback_t       fetch_item;
    shardcache_store_item_callback_t       store_item;
    shardcache_remove_item_callback_t      remove_item;
    const char                           **options;
} shardcache_storage_t;

/**
 * @brief Create a new shardcache instance
 * @arg me : a valid <address:port> null-terminated string representing the new node to be created
 * @arg peers : a list of <address:port> strings representing the peers taking part to the shardcache 'cloud'
 * @arg num_peers : the number of peers present in the peers list
 * @arg storage : a shardcache_storage_t structure holding pointers to the storage callbacks.
 *                NOTE: The newly created instance will copy the pointers to its internal descriptor so
 *                      the resources allocated for the storage structure can be safely released after
 *                      calling shardcache_create()
 * @arg secret : a null-terminated string containing the shared secret used to authenticate incoming messages
 */
shardcache_t *shardcache_create(char *me, 
                        char **peers,
                        int num_peers,
                        shardcache_storage_t *storage,
                        char *secret,
                        int num_workers);

/**
 * @brief Release all the resources used by the shardcache instance
 * @arg cache : the instance to release
 */
void shardcache_destroy(shardcache_t *cache);

/**
 * @brief Get the value for a key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg vlen : If provided the length of the returned value will be stored at the location pointed by vlen
 * @return A pointer to the stored value if any, NULL otherwise
 *         NOTE: the caller is responsible of releasing the memory of the returned value
 */
void *shardcache_get(shardcache_t *cache, void *key, size_t klen, size_t *vlen);

/**
 * @brief Set the value for a key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg value : A valid pointer to the value
 * @arg vlen : The length of the value
 * @return 0 on success, -1 otherwise
 */
int shardcache_set(shardcache_t *cache, void *key, size_t klen, void *value, size_t vlen);

/**
 * @brief Remove the value for a key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_del(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Remove the value from the cache for a key
 *        NOTE: the value will not be removed from the underlying storage
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_evict(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Get the list of all peers (including this node itself)
 *        taking part to the shardcache 'cloud'
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg num_peers : If provided the number of peers in the returned array will be 
 *                  will be stored at the location pointed by num_peers
 * @return A NULL terminated list containing all the peers <address:port> strings
 */
char **shardcache_get_peers(shardcache_t *cache, int *num_peers);

/**
 * @brief Get the peer owning a specific key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg owner : If provided the pointed pointer will be set to the name
 *              (<address:port>) of the peer owning the key
 * @return 1 if the current node (represented by cache) is the owner of the key, 0 otherwise
 */
int shardcache_test_ownership(shardcache_t *cache, void *key, size_t klen, char *owner, size_t *len);

#endif
