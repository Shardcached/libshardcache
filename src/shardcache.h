#ifndef __SHARDCACHE_H__
#define __SHARDCACHE_H__

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <sys/types.h>
#include <stdint.h>

#define SHARDCACHE_PORT_DEFAULT 9874

/**
 * @brief Structure representing an exposed counter.
 *        Anywhere in the shardcache code it is possible to export counters
 *        (uin32_t integers)for stats purposes.
 *        Any module can add new counters which will be included in the array
 *        returned by shardcache_get_counters().
 *        The value member of the structure will be always accessed via
 *        the atomic builtins and the same is expected from the module 
 *        exporting it.
 */
typedef struct {
    char name[256];
    uint32_t value;
} shardcache_counter_t;


/**
 * @brief Opaque structure representing the actual shardcache instance
 */
typedef struct __shardcache_s shardcache_t;

/**
 * @brief returns the list of registered counters with their actual value
 * @arg cache    : A valid pointer to a shardcache_t structure
 * @arg counters : an array of shardcache_counter_t structures to fill in
 *                 Note the array must be sized
 * @return the number of counters contained in the counters array
 */
int shardcache_get_counters(shardcache_t *cache,
                            shardcache_counter_t **counters);

/**
 * @brief clear all the counters
 * @arg cache : A valid pointer to a shardcache_t structure
 */
void shardcache_clear_counters(shardcache_t *cache);


/**
 * @brief Callback to provide the value for a given key.
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache.
 * @return A pointer to initialized memory containing the stored value.
 *         NOTE: The returned value MUST be a volatile copy and the caller
 *               WILL release its resources
 */
typedef void *(*shardcache_fetch_item_callback_t)
    (void *key, size_t len, size_t *vlen, void *priv);

/**
 * @brief Callback to store a new value for a given key.
 *        The shardcache instance will call this callback
 *        if a new value needs to be set in the underlying storage
 */
typedef int (*shardcache_store_item_callback_t)
    (void *key, size_t len, void *value, size_t vlen, void *priv);

/**
 * @brief Callback to remove an existing value for a given key.
 *        The shardcache instance will call this callback
 *        if the value for a given key needs to be removed
 *        from the underlying storage
 */
typedef int
(*shardcache_remove_item_callback_t)(void *key, size_t len, void *priv);

/**
 * @brief Callback to check if a specific key exists on the storage
 */
typedef int
(*shardcache_exist_item_callback_t)(void *key, size_t len, void *priv);

/**
 * @brief structure representing an item in the storage index
 */
typedef struct __shardcache_storage_index_item_s {
    void *key;
    size_t klen;
    size_t vlen;
} shardcache_storage_index_item_t;

/**
 * @brief structure representing the storage index, holding an array
 *        of pointers to shardcache_storage_index_item_t structures
 */
typedef struct __shardcache_storage_index_s {
    shardcache_storage_index_item_t *items;
    size_t size;
} shardcache_storage_index_t;

/**
 * @brief Callback to fetch the index of stored keys
 *        The shardcache instance will call this callback
 *        when it will need to retrieve the full index of the keys
 *        owned (and stored) by the instance
 *
 * @arg index :   An array of shardcache_storage_index_item_t structures
 *                to hold the index
 * @arg isize :   The number of slots in the provided index array
 * @arg priv  :   The priv pointer owned by the storage
 *
 * @return The number of items in the index, 0 if none or errors
 *        NOTE: If the caller didn't previously check the number of items
 *              via the count() callback, it MUST check if the returned value
 *              matches the passed isize and in case try again with a bigger
 *              index size to ensure there are no more items.
 *              Using the count() method ensure to get all the items
 *              which are expected to exist
 */
typedef size_t (*shardcache_get_index_callback_t)
    (shardcache_storage_index_item_t *index, size_t isize, void *priv);

/**
 * @brief Callback returning the number of items in the index
 */
typedef size_t (*shardcache_count_items_callback_t)(void *priv);

/**
 * @brief Structure holding all the callback pointers required
 *        by shardcache to interact with underlying storage
 */
typedef struct __shardcache_storage_s {
    shardcache_fetch_item_callback_t       fetch;
    shardcache_store_item_callback_t       store;
    shardcache_remove_item_callback_t      remove;
    shardcache_exist_item_callback_t       exist;
    shardcache_get_index_callback_t        index;
    shardcache_count_items_callback_t      count;
    void                                   *priv;
} shardcache_storage_t;

/**
 * @brief Callback used to create a new instance of a storage module
 *        All storage modules need to expose this callback which will be 
 *        called by the shardcache instance at initialization time 
 *        to let the storage initialize and create the shardcache_storage_t
 *        structure and its internals
 *
 * @arg options  : a null-termianted array of strings holding the options
 *                 specific to the storage module
 *
 * @return A newly initialized shardcache_storage_t structure usable by the
 *         shardcache instance
 */
typedef shardcache_storage_t *
        (*shardcache_storage_constructor)(const char **options);

/**
 * @brief Callback used to dispose all resources associated to a previously
 *        initialized storage module
 *
 * @arg storage : A pointer to a valid (and previously initialized)
 *                shardcache_storage_t structure
 */
typedef void (*shardcache_storage_destructor)(shardcache_storage_t *storage);

/**
 * @brief Structure representing a node taking part in the shard cache
 */

#define SHARDCACHE_NODE_LABEL_MAXLEN 256
#define SHARDCACHE_NODE_ADDRESSL_MAXLEN 256

typedef struct shardcache_node_s {
    char label[SHARDCACHE_NODE_LABEL_MAXLEN];
    char address[SHARDCACHE_NODE_ADDRESSL_MAXLEN];
} shardcache_node_t;

/**
 * @brief Create a new shardcache instance
 * @arg me              : a valid <address:port> null-terminated string
 *                        representing the new node to be created
 * @arg nodes           : a list of <address:port> strings representing the nodes
 *                        taking part to the shardcache 'cloud'
 * @arg num_nodes       : the number of nodes present in the nodes list
 * @arg storage         : a shardcache_storage_t structure holding pointers to the
 *                        storage callbacks.
 *                        NOTE: The newly created instance will copy the pointers to
 *                              its internal descriptor so the resources allocated
 *                              for the storage structure can be safely released after
 *                              calling shardcache_create()
 * @arg secret          : a null-terminated string containing the shared secret used to
 *                         authenticate incoming messages
 * @arg num_workers     : number of worker threads taking care of serving input connections
 * @arg cache_size      : the maximum size of the ARC cache
 * @arg evict_on_delete : controls if an evict command is sent to all nodes when an item is removed
 */
shardcache_t *shardcache_create(char *me, 
                        shardcache_node_t *nodes,
                        int num_nodes,
                        shardcache_storage_t *storage,
                        char *secret,
                        int num_workers,
                        size_t cache_size,
                        int evitct_on_delete);

/**
 * @brief Release all the resources used by the shardcache instance
 * @arg cache : the instance to release
 */
void shardcache_destroy(shardcache_t *cache);

/**
 * @brief Get the value for a key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key   : A valid pointer to the key
 * @arg klen  : The length of the key
 * @arg vlen  : If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 *
 * @return A pointer to the stored value if any, NULL otherwise
 *         NOTE: the caller is responsible of releasing the memory of the
 *               returned value
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
int shardcache_set(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen);
/**
 * @brief Set a volatile value for a key
 * @arg cache  : A valid pointer to a shardcache_t structure
 * @arg key    : A valid pointer to the key
 * @arg klen   : The length of the key
 * @arg value  : A valid pointer to the value
 * @arg vlen   : The length of the value
 * @arg expire : The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *               storage backend as the filesystem or database ones)
 * @return 0 on success, -1 otherwise
 */
int shardcache_set_volatile(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire);

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
 * @arg num_nodes : If provided the number of nodes in the returned array
 *                  will be will be stored at the location pointed by num_nodes
 * @return A list containing all the nodes <address:port> strings
 */
const shardcache_node_t *
shardcache_get_nodes(shardcache_t *cache, int *num_nodes);

/**
 * @brief Get the node owning a specific key
 * @arg cache : A valid pointer to a shardcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg owner : If provided the pointed pointer will be set to the name
 *              (<address:port>) of the node owning the key
 * @return 1 if the current node (represented by cache) is the owner
 *           of the key, 0 otherwise
 */
int shardcache_test_ownership(shardcache_t *cache,
                              void *key,
                              size_t klen,
                              char *owner,
                              size_t *len);

/**
 * @brief Get the index of keys managed by the specific shardcache instance
 *        by querying the storage module
 * @return A pointer to a shardcache_storage_index_t structure holding 
 *         the index.
 *         NOTE: The caller MUST release the returned pointer once done with it
 *               by using the shardcache_free_index() function
 */
shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache);

/**
 * @brief Release all resources used by the index provided as argument
 * @arg index : A pointer to a valid shardcache_storage_index_t structure
 *              previously obtained via the shardcache_get_index() function
 */
void shardcache_free_index(shardcache_storage_index_t *index);

/**
 * @brief : Start a migration process
 * @arg cache     : A valid pointer to a shardcache_t structure
 * @arg nodes     : The list of nodes representing the new group to migrate to
 * @arg num_nodes : The number of nodes in the list
 * @arg forward   : A boolean flag indicating if the migration command needs to be
 *                  forwarded to all other peers
 * @return 0 on success, -1 otherwise
 */
int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t *nodes,
                               int num_nodes,
                               int forward);

/**
 * @brief : Abort the current migration process
 * @arg cache     : A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_abort(shardcache_t *cache);

/**
 * @brief : End the current migration process by swapping the two continua
 *          and releasing resources for the old one
 * @arg cache     : A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_end(shardcache_t *cache);


#endif
