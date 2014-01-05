#ifndef __SHARDCACHE_H__
#define __SHARDCACHE_H__

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <sys/types.h>
#include <sys/time.h>
#include <stdint.h>

#define SHARDCACHE_PORT_DEFAULT 9874

/*
 *******************************************************************************
 * Storage API 
 *******************************************************************************
 */

/**
 * @brief Callback to provide the value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if the value has not been found in the cache.
 *
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param vlen    If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param priv    The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
 * @return A pointer to initialized memory containing the stored value.
 *         A NULL pointer if not found.
 * @note The returned value MUST be a volatile copy and the caller
 *       WILL release its resources
 */
typedef void *(*shardcache_fetch_item_callback_t)
    (void *key, size_t klen, size_t *vlen, void *priv);

/**
 * @brief Callback to store a new value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if a new value needs to be set in the underlying storage
 *
 * @param key      A valid pointer to the key
 * @param klen     The length of the key
 * @param value    A valid pointer to the value to store
 * @param vlen     The length of the value
 * @param priv     The 'priv' pointer previously stored in the shardcache_storage_t
 *               structure at initialization time
 * @return 0 if success, -1 otherwise
 */
typedef int (*shardcache_store_item_callback_t)
    (void *key, size_t klen, void *value, size_t vlen, void *priv);

/**
 * @brief Callback to remove an existing value for a given key.
 *
 *        The shardcache instance will call this callback
 *        if the value for a given key needs to be removed
 *        from the underlying storage
 *
 * @param key      A valid pointer to the key
 * @param klen     The length of the key
 * @param priv     The 'priv' pointer previously stored in the shardcache_storage_t
 *               structure at initialization time
 * @return 0 if success, -1 otherwise
 */
typedef int
(*shardcache_remove_item_callback_t)(void *key, size_t klen, void *priv);

/**
 * @brief Callback to check if a specific key exists on the storage
 * @param key      A valid pointer to the key
 * @param klen     The length of the key
 * @param priv     The 'priv' pointer previously stored in the shardcache_storage_t
 *                 structure at initialization time
 * @return 1 if exists, 0 otherwise
 */
typedef int
(*shardcache_exist_item_callback_t)(void *key, size_t klen, void *priv);

/**
 * @struct shardcache_storage_index_item_t
 *
 * @brief structure representing an item in the storage index
 *
 */
typedef struct __shardcache_storage_index_item_s {
    void *key;   //!<A valid pointer to the key
    size_t klen; //!<The length of the key
    size_t vlen; //!<The length of the value
} shardcache_storage_index_item_t;

/**
 * @struct shardcache_storage_index_t
 *
 * @brief structure representing the storage index, holding an array
 *        of shardcache_storage_index_item_t structures. 
 * @note  Can be obtained calling shardcache_get_index()
 *        and MUST be disposed using shardcache_free_index()
 */
typedef struct __shardcache_storage_index_s {
    //! Array of shardcache_storage_index_item_t structures
    shardcache_storage_index_item_t *items;
    //! The number of elements in the 'items' array
    size_t size;
} shardcache_storage_index_t;

/**
 * @brief Callback returning the number of items in the index
 *
 * @param priv     The priv pointer owned by the storage
 *
 * @return The total number of items in the storage
 * @note This callback is mandatory if the index callback is set, otherwise it's optional
 * @see shardcache_get_index_callback_t
 * @see shardcache_get_index()
 */
typedef size_t (*shardcache_count_items_callback_t)(void *priv);

/**
 * @brief Callback to fetch the index of stored keys.
 *
 *        The shardcache instance will call this callback
 *        when it will need to retrieve the full index of the keys
 *        owned (and stored) by the instance
 *
 * @param index    An array of shardcache_storage_index_item_t structures
 *               to hold the index
 * @param isize    The number of slots in the provided index array
 * @param priv     The priv pointer owned by the storage
 *
 * @return The number of items in the index, 0 if none or errors
 * @note If the caller didn't previously check the number of items
 *       via the count() callback, it MUST check if the returned value
 *       matches the passed isize and in case try again with a bigger
 *       index size to ensure there are no more items.
 *       Using the count() method ensure to get all the items
 *       which are expected to exist
 *
 * @note The count might change between the time the count callback returns
 *       and the this callback is called.\n
 *       If this happens the cases are 2 :\n
 *          1 - More items have been added in the meanwhile.
 *              Which means that the caller will get exactly the expected number
 *              of items but there will be more new items in the storage which are not returned.\n
 *          2 - Some items have been removed in the meanwhile.
 *              The caller will get less items than expecting but this is not a problem because
 *              the alloc'd memory is enough and the caller will also know how many items 
 *              have been actually returned by the index callback.
 *
 */
typedef size_t (*shardcache_get_index_callback_t)
    (shardcache_storage_index_item_t *index, size_t isize, void *priv);

/**
 * @struct shardcache_storage_t
 *
 * @brief      Structure holding all the callback pointers required
 *             by shardcache to interact with underlying storage
 */
typedef struct __shardcache_storage_s {
    //! The fecth callback
    shardcache_fetch_item_callback_t       fetch;
    //! The store callback (optional if the storage is indended to be read-only)
    shardcache_store_item_callback_t       store;
    //! The remove callback (optional if the storage is intended to be read-only)
    shardcache_remove_item_callback_t      remove;
    /**
     * @brief Optional callback which can be used to 'quickly' check if a key exists in the storage
     * @note The speed of this callback strictly depends on the storage implementation
     */
    shardcache_exist_item_callback_t       exist;
    /**
     * @brief Optional callback which returns the index of keys accessible through the storage
     * @note check shardcache_get_index() documentation for more details
     */
    shardcache_get_index_callback_t        index;
    /**
     * @brief If the index callback is set, this callback will be used to determine how many
     *        items are going to be returned in the index.
     * @note This callback is mandatory if the index callback is set, otherwise it's optional
     * @note check shardcache_get_index() documentation for more details
     */
    shardcache_count_items_callback_t      count;
    /**
     * @brief If set to a non zero value, shardcache will assume that all the peers
     *        can access the same keys on the storage (for example a shared database,
     *        or a shared filesystem))
     */
    int                                    shared;
    /**
     * @brief Pointer to private data which will passed to all the callbacks
     * @note The implementation can use this pointer to keep its internal data/status/handlers/whatever
     */
    void                                   *priv;
} shardcache_storage_t;

/**
 * @brief Callback used to create a new instance of a storage module
 *
 *        All storage modules need to expose this callback which will be 
 *        called by the shardcache instance at initialization time 
 *        to let the storage initialize and create the shardcache_storage_t
 *        structure and its internals
 *
 * @param options   A null-termianted array of strings holding the options
 *                specific to the storage module
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
 * @param storage   A pointer to a valid (and previously initialized)
 *                shardcache_storage_t structure
 */
typedef void (*shardcache_storage_destructor)(shardcache_storage_t *storage);

/*
 *******************************************************************************
 * Counters API 
 *******************************************************************************
 */

/**
 * @brief Structure representing an exposed counter.
 *
 *        Any of the internal shardcache modules can export counters
 *        (as uin32_t integers) for stats purposes.
 *        Any module can add new counters which will be included in the array
 *        returned by shardcache_get_counters().
 *        The value member of the structure will be always accessed via
 *        the atomic builtins and the same is expected from the module 
 *        exporting it.
 *
 * @note  A list of exported counters can be obtained using shardcache_get_counters()
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
 * @brief Returns the list of registered counters with their actual value
 * @param cache     A valid pointer to a shardcache_t structure
 * @param counters  A reference to a pointer which will be set to the initialized
 *                  memory holding the array of counters
 * @note            The counters array needs to be released using
 *                  free() once not necessary anymore.
 * @return The number of counters contained in the counters array
 */
int shardcache_get_counters(shardcache_t *cache,
                            shardcache_counter_t **counters);

/**
 * @brief Resets all the counters to 0
 * @param cache   A valid pointer to a shardcache_t structure
 */
void shardcache_clear_counters(shardcache_t *cache);


/*
 *******************************************************************************
 * Shardcache API 
 *******************************************************************************
 */

#define SHARDCACHE_NODE_LABEL_MAXLEN 256
#define SHARDCACHE_NODE_ADDRESS_MAXLEN 256

/**
 * @brief Structure representing a node taking part in the shard cache
 * @see shardcache_create()
 * @see shardcache_get_nodes()
 */
typedef struct shardcache_node_s {
    char label[SHARDCACHE_NODE_LABEL_MAXLEN];
    char address[SHARDCACHE_NODE_ADDRESS_MAXLEN];
} shardcache_node_t;

/**
 * @brief Create a new shardcache instance
 * @param me              a valid <address:port> null-terminated string
 *                        representing the new node to be created
 * @param nodes           a list of <address:port> strings representing the nodes
 *                        taking part to the shardcache 'cloud'
 * @param num_nodes       the number of nodes present in the nodes list
 * @param storage         a shardcache_storage_t structure holding pointers to the
 *                        storage callbacks.
 * @param secret          a null-terminated string containing the shared secret used to
 *                        authenticate incoming messages
 * @param num_workers     number of worker threads taking care of serving input connections
 * @param cache_size      the maximum size of the ARC cache
 * @param evict_on_delete controls if an evict command is sent to all nodes when an item is removed
 * @return a newly initialized shardcache descriptor
 * 
 * @note The returned shardcache_t structure MUST be disposed using shardcache_destroy()
 *
 * @note The newly created instance will copy all the pointers contained in the storage structure 
 *       to its internal descriptor so the resources eventually allocated for the storage structure
 *       passed as argument can be safely released after calling shardcache_create()
 */
shardcache_t *shardcache_create(char *me, 
                        shardcache_node_t *nodes,
                        int num_nodes,
                        shardcache_storage_t *storage,
                        char *secret,
                        int num_workers,
                        size_t cache_size,
                        int evict_on_delete);

/**
 * @brief Release all the resources used by the shardcache instance
 * @param cache   the instance to release
 */
void shardcache_destroy(shardcache_t *cache);

/**
 * @brief Get the value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param vlen    If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param timestamp   If provided the timestamp of when the object was loaded into the cache
 *                  will be stored at the specified address
 *
 * @return A pointer to the stored value if any, NULL otherwise
 * @note the caller is responsible of releasing the memory of the
 *       returned value
 */
void *shardcache_get(shardcache_t *cache, void *key, size_t klen, size_t *vlen, struct timeval *timestamp);

/**
 * @brief Get partial value data value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param head    A pointer to the memory where to store the partial data
 * @param hlen    The size of the memory pointed by 'head'
 * @param timestamp   If provided the timestamp of when the object was loaded into the cache
 *                  will be stored at the specified address
 *
 * @return The size copied in the 'head' pointer
 * @note The returned size might be less than what specified in 'hlen'
 *       if the complete data is smaller than hlen
 */
size_t shardcache_head(shardcache_t *cache, void *key, size_t klen, void *head, size_t hlen, struct timeval *timestamp);

/**
 * @brief Set the value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen   The length of the key
 * @param value   A valid pointer to the value
 * @param vlen   The length of the value
 * @return 0 on success, -1 otherwise
 * @see shardcache_set_volatile()
 */
int shardcache_set(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen);
/**
 * @brief Set a volatile value for a key
 * @param cache    A valid pointer to a shardcache_t structure
 * @param key      A valid pointer to the key
 * @param klen     The length of the key
 * @param value    A valid pointer to the value
 * @param vlen     The length of the value
 * @param expire   The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *               storage backend as the filesystem or database ones)
 * @return 0 on success, -1 otherwise
 * @see shardcache_set()
 */
int shardcache_set_volatile(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire);

/**
 * @brief Remove the value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen   The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_del(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Remove the value from the cache for a key
 * @note the value will not be removed from the underlying storage
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen   The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_evict(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Get the list of all nodes (including this node itself)
 *        taking part to the shardcache 'cloud'
 * @param cache   A valid pointer to a shardcache_t structure
 * @param num_nodes   If provided the number of nodes in the returned array
 *                  will be will be stored at the location pointed by num_nodes
 * @return A list containing all the nodes <address:port> strings
 * @note the caller MUST release the returned pointer once done with it
 */
shardcache_node_t *
shardcache_get_nodes(shardcache_t *cache, int *num_nodes);

/**
 * @brief Get the node owning a specific key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen The length of the key
 * @param owner If provided the pointed pointer will be set to the name
 *              (<address:port>) of the node owning the key
 * @param len If not NULL, the size of the owner string will be stored
 *            in the memory pointed by 'len'
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
 * @note The caller MUST release the returned pointer once done with it
 *       by using the shardcache_free_index() function
 */
shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache);

/**
 * @brief Release all resources used by the index provided as argument
 * @param index   A pointer to a valid shardcache_storage_index_t structure
 *              previously obtained via the shardcache_get_index() function
 */
void shardcache_free_index(shardcache_storage_index_t *index);

/**
 * @brief   Start a migration process
 * @param cache       A valid pointer to a shardcache_t structure
 * @param nodes       The list of nodes representing the new group to migrate to
 * @param num_nodes   The number of nodes in the list
 * @param forward     A boolean flag indicating if the migration command needs to be
 *                  forwarded to all other peers
 * @return 0 on success, -1 otherwise
 */
int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t *nodes,
                               int num_nodes,
                               int forward);

/**
 * @brief   Abort the current migration process
 * @param cache       A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_abort(shardcache_t *cache);

/**
 * @brief   End the current migration process by swapping the two continua
 *          and releasing resources for the old one
 * @param cache       A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_end(shardcache_t *cache);


#endif
