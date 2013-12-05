#ifndef __SHARDCACHE_H__
#define __SHARDCACHE_H__

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <sys/types.h>
#include <stdint.h>
#include "counters.h"

#define SHARDCACHE_PORT_DEFAULT 9874

/**
 * @brief Opaque structure representing the actual shardcache instance
 */
typedef struct __shardcache_s shardcache_t;

int shardcache_get_counters(shardcache_t *cache, shardcache_counter_t **counters);
void shardcache_clear_counters(shardcache_t *cache);


/**
 * @brief Callback to provide the value for a given key.
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache.
 * @return A pointer to initialized memory containing the stored value.
 *         NOTE: The returned value MUST be a volatile copy and the caller
 *               WILL release its resources
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
 */
typedef int (*shardcache_remove_item_callback_t)(void *key, size_t len, void *priv);

typedef struct __shardcache_storage_index_item_s {
    void *key;
    size_t klen;
    size_t vlen;
} shardcache_storage_index_item_t;

typedef struct __shardcache_storage_index_s {
    shardcache_storage_index_item_t *items;
    size_t size;
} shardcache_storage_index_t;

/**
 * @brief Callback to fetch the index of stored keys
 *        The shardcache instance will call this callback
 *        when it will need to retrieve the full index of the keys
 *        owned (and stored) by the instance
 * @arg index:   An array of shardcache_storage_index_item_t structures to hold the index
 * @arg isize:   The number of slots in the provided index array
 * @arg priv:    The priv pointer owned by the storage
 * @return The number of items in the index, 0 if none or errors
 *        NOTE: If the caller didn't previously check the number of item via the count() callback,
 *              it MUST check if the returned value matches the passed isize and in case try again
 *              with a bigger index size to ensure there are no more items.
 *              Using the count() method ensure to get all the items which are expected to exist
 */
typedef size_t (*shardcache_get_index_callback_t)(shardcache_storage_index_item_t *index, size_t isize, void *priv);

typedef size_t (*shardcache_count_items_callback_t)(void *priv);

shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache);
void shardcache_free_index(shardcache_storage_index_t *index);

/**
 * @brief Structure holding all the callback pointers required
 *        by shardcache to interact with underlying storage
 */
typedef struct __shardcache_storage_s {
    shardcache_fetch_item_callback_t       fetch;
    shardcache_store_item_callback_t       store;
    shardcache_remove_item_callback_t      remove;
    shardcache_get_index_callback_t        index;
    shardcache_count_items_callback_t      count;
    void                                   *priv;
} shardcache_storage_t;

typedef shardcache_storage_t *(*shardcache_storage_constructor)(const char **options);
typedef void (*shardcache_storage_destructor)(shardcache_storage_t *storage);

typedef struct shardcache_node_s {
    char label[256];
    char address[256];
} shardcache_node_t;

/**
 * @brief Create a new shardcache instance
 * @arg me : a valid <address:port> null-terminated string representing the new node to be created
 * @arg nodes : a list of <address:port> strings representing the nodes taking part to the shardcache 'cloud'
 * @arg num_nodes : the number of nodes present in the nodes list
 * @arg storage : a shardcache_storage_t structure holding pointers to the storage callbacks.
 *                NOTE: The newly created instance will copy the pointers to its internal descriptor so
 *                      the resources allocated for the storage structure can be safely released after
 *                      calling shardcache_create()
 * @arg secret : a null-terminated string containing the shared secret used to authenticate incoming messages
 */
shardcache_t *shardcache_create(char *me, 
                        shardcache_node_t *nodes,
                        int num_nodes,
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
 * @brief Get the list of all nodes (including this node itself)
 *        taking part to the shardcache 'cloud'
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg num_nodes : If provided the number of nodes in the returned array will be 
 *                  will be stored at the location pointed by num_nodes
 * @return A list containing all the nodes <address:port> strings
 */
const shardcache_node_t *shardcache_get_nodes(shardcache_t *cache, int *num_nodes);

/**
 * @brief Get the node owning a specific key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg owner : If provided the pointed pointer will be set to the name
 *              (<address:port>) of the node owning the key
 * @return 1 if the current node (represented by cache) is the owner of the key, 0 otherwise
 */
int shardcache_test_ownership(shardcache_t *cache, void *key, size_t klen, char *owner, size_t *len);

#endif
