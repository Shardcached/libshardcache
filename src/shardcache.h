#ifndef __SHARDCACHE_H__
#define __SHARDCACHE_H__

/**
 * @file shardcache.h
 * @author Andrea Guzzo
 * @brief shardcache C implementation
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <syslog.h>


#define SHARDCACHE_PORT_DEFAULT 4444
#define SHARDCACHE_TCP_TIMEOUT_DEFAULT 5000 // (in millisecs) == 5 secs
#define SHARDCACHE_EXPIRE_TIME_DEFAULT 0 // don't expire keys by default
#define SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW 100000 // (in microsecs)
#define SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH 500000 // (in microsecs)

extern const char *LIBSHARDCACHE_VERSION;

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
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param vlen  If provided the length of the returned value will be stored
 *              at the location pointed by vlen
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
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
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param value A valid pointer to the value to store
 * @param vlen  The length of the value
 * @param priv  The 'priv' pointer previously stored in the shardcache_storage_t
 *              structure at initialization time
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
 * @param key  A valid pointer to the key
 * @param klen The length of the key
 * @param priv The 'priv' pointer previously stored in the shardcache_storage_t
 *             structure at initialization time
 * @return 0 if success, -1 otherwise
 */
typedef int
(*shardcache_remove_item_callback_t)(void *key, size_t klen, void *priv);

/**
 * @brief Callback to check if a specific key exists on the storage
 * @param key  A valid pointer to the key
 * @param klen The length of the key
 * @param priv The 'priv' pointer previously stored in the shardcache_storage_t
 *             structure at initialization time
 * @return 1 if exists, 0 otherwise
 */
typedef int
(*shardcache_exist_item_callback_t)(void *key, size_t klen, void *priv);

typedef struct __shardcache_record_s {
    void *v;
    size_t l;
} shardcache_record_t;

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
 * @param priv The priv pointer owned by the storage
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
 * @param index An array of shardcache_storage_index_item_t structures
 *              to hold the index
 * @param isize The number of slots in the provided index array
 * @param priv  The priv pointer owned by the storage
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
    int                                    global;

    /**
     * @brief If set to a non zero value, shardcache will assume that all the replicas
     *        of a given peer can access the same storage son set commands won't be propagated
     *        to all the replicas by a receiving node
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
 * @param options A null-termianted array of strings holding the options
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
 * @param storage A pointer to a valid (and previously initialized)
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
 *        (as uint32_t integers) for stats purposes.
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
 * @param cache    A valid pointer to a shardcache_t structure
 * @param counters A reference to a pointer which will be set to the initialized
 *                 memory holding the array of counters
 * @note           The counters array needs to be released using
 *                 free() once not necessary anymore.
 * @return The number of counters contained in the counters array
 */
int shardcache_get_counters(shardcache_t *cache,
                            shardcache_counter_t **counters);

/**
 * @brief Resets all the counters to 0
 * @param cache A valid pointer to a shardcache_t structure
 */
void shardcache_clear_counters(shardcache_t *cache);


/*
 *******************************************************************************
 * Shardcache API 
 *******************************************************************************
 */

/**
 * @brief Structure representing a node taking part in the shard cache
 * @see shardcache_create()
 * @see shardcache_get_nodes()
 */
typedef struct __shardcache_node_s shardcache_node_t;

/**
 * @brief Create a new shardcache_node_t structure
 * @param label The label of the node
 * @param addresses Array containing the addresses for all the replicas representing the node
 * @param num_addresses The number of items in the addresses array
 * @return A newly initialized shardcache_node_t structure
 * @note The caller MUST release the resources used to represent the node by calling
 *       shardcache_node_destroy() on the returned pointer
 */
shardcache_node_t *shardcache_node_create(char *label, char **addresses, int num_addresses);

/**
 * @brief Create a copy of an existing shardcache_node_t structure
 * @param node A previously initialized and valid shardcache_node_t structure
 * @return A newly initialized shardcache_node_t structure identical to the original one
 * @note The caller MUST release the resources used to represent the node by calling
 *       shardcache_node_destroy() on the returned pointer
 */
shardcache_node_t *shardcache_node_copy(shardcache_node_t *node);

/**
 * @brief Release all the resources used by a shardcache_node_t structure
 * @param node A previously initialized and valid shardcache_node_t structure
 */
void shardcache_node_destroy(shardcache_node_t *node);

/**
 * @brief Get the label for a given node
 * @param node A previously initialized and valid shardcache_node_t structure
 * @return The label for the node passed as argument
 */
char *shardcache_node_get_label(shardcache_node_t *node);


/**
 * @brief Get the node-string representing a given node
 * @param node A previously initialized and valid shardcache_node_t structure
 * @return The node-string for the node passed as argument
 */
char *shardcache_node_get_string(shardcache_node_t *node);

/**
 * @brief Get a valid address for a given node (from any of the replicas)
 * @param node A previously initialized and valid shardcache_node_t structure
 * @return One of the valid addresses for the node passed as argument
 * @note If replicas are used this function may return a different value
 *       (among the configured replicas) each time it's called
 */
char *shardcache_node_get_address(shardcache_node_t *node);

/**
 * @brief Get the number of addresses (replicas) configured for a given node
 * @param node A previously initialized and valid shardcache_node_t structure
 * @return The number of addresses for the given node
 */
int shardcache_node_num_addresses(shardcache_node_t *node);

/**
 * @brief Get all the addresses (up to num_addresses) configured for a given node
 * @param node A previously initialized and valid shardcache_node_t structure
 * @param addresses A prealloc'd array of pointers where to store the addresses
 * @param num_addresses the number of addresses that can fit into the provided (prealloc'd) array
 * @return The total number of addresses configured for the given node
 * @note the pointers stored in the array point directly to the internal node storage so the caller
 *       should neither modify nor release them.
 *       (The array itself, prealloc'd by the caller, should instead be normally released)
 *
 * @todo Cleanup/Improve this API
 */
int shardcache_node_get_all_addresses(shardcache_node_t *node, char **addresses, int num_addresses);

/**
 * @brief Get one of the addresses (at a specific index) configured for a given node
 * @param node A previously initialized and valid shardcache_node_t structure
 * @param index the index of the address we want to ge
 * @return The address at the specified index if any, NULL otherwise
 *
 * @note the returned pointer points directly to the internal node storage so the caller
 *       should neither modify nor release it
 *
 * @todo Cleanup/Improve this API
 */
char *shardcache_node_get_address_at_index(shardcache_node_t *node, int index);

/**
 * @brief Select a node by its label
 * @param cache   A valid pointer to a shardcache_t structure
 * @param label   The label of the node to select
 * @return A valid shardcache_node_t structure representing the requested node,
 *         NULL if no node has been found matching the label passed as argument
 */
shardcache_node_t * shardcache_node_select(shardcache_t *cache, char *label);

/**
 * @brief Get the list of all nodes (including this node itself)
 *        taking part to the shardcache 'cloud'
 * @param cache   A valid pointer to a shardcache_t structure
 * @param num_nodes   If provided the number of nodes in the returned array
 *                  will be will be stored at the location pointed by num_nodes
 * @return A list containing all the nodes <address:port> strings
 * @note the caller MUST release the returned pointer once done with it
 *       by using the shardcache_free_nodes() function on the returned list
 */
shardcache_node_t **shardcache_get_nodes(shardcache_t *cache, int *num_nodes);

/**
 * @brief Release resources for a list of nodes
 * @param nodes A valid list of shardcache_node_t structures
 *              (as returned by shardcache_get_nodes())
 * @param num_nodes The number of nodes in the array
 */
void shardcache_free_nodes(shardcache_node_t **nodes, int num_nodes);

/**
 * @brief Get the labels of all nodes (including this node itself)
 *        taking part to the shardcache 'cloud'
 * @param cache      A valid pointer to a shardcache_t structure
 * @param num_labels If provided the number of nodes in the returned array
 *                   will be will be stored at the location pointed by num_nodes
 *
 * @return A list containing labels from all known nodes
 * @note the caller MUST release the returned pointer once done with it
 *       by using the shardcache_free_labels() function on the returned list
 */
char **shardcache_get_node_labels(shardcache_t *cache, int *num_labels);

/**
 * @brief Release resources for a list of labels
 *
 * @param labels     A valid list of labels (as returned by shardcache_get_node_labels())
 * @param num_labels The number of nodes in the array
 */
void shardcache_free_labels(shardcache_node_t **labels, int num_labels);

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
 * @return a newly initialized shardcache descriptor
 * 
 * @note The returned shardcache_t structure MUST be disposed using shardcache_destroy()
 *
 * @note The newly created instance will copy all the pointers contained in the storage structure 
 *       to its internal descriptor so the resources eventually allocated for the storage structure
 *       passed as argument can be safely released after calling shardcache_create()
 */
shardcache_t *shardcache_create(char *me, 
                        shardcache_node_t **nodes,
                        int num_nodes,
                        shardcache_storage_t *storage,
                        char *secret,
                        int num_workers,
                        size_t cache_size);

/*
 * @brief Allows to switch between using persistent connections, or making a new connection for
 *        each message sent to a peer
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if persistent connections should be used, 0 otherwise.
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the use_persistent_connections setting
 * @note if evict-on-delete is true, an evict command is sent to all other nodes
 *       when an item is removed from the storage
 * @note defaults to 1
 */
int shardcache_use_persistent_connections(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the evict_on_delete behaviour at runtime
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if evict_on_delete is desired, 0 otherwise.\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the evict_on_delete setting
 * @note defaults to 1
 */
int shardcache_evict_on_delete(shardcache_t *cache, int new_value);

/*
 * @brief Allows to force caching of remote items
 *        (as opposed to the default behaviour  of caching only hot items)
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if force_caching is desired, 0 otherwise.\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the force_caching setting
 * @note defaults to 1
 */
int shardcache_force_caching(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout used when creating tcp connections
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   The amount of seconds to use as timeout.
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the tcp_timeout setting
 * @note defaults to SHARDCACHE_TCP_TIMEOUT_DEFAULT
 */
int shardcache_tcp_timeout(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout passed to iomux_run()
 *               by the serving workers and the async reader
 * @note  Both the workers and the async reader use a low timeout
 *        to allow newly added filedescriptors to be included in the
 *        iomux as soon as possible
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of microseconds to use as timeout.
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status).
 * @return the previous value for the iomux_run_timeout_low setting
 * @note defaults to SHARDCACHE_IOMUX_RUN_TIMEOUT_LOW
 */
int shardcache_iomux_run_timeout_low(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the timeout passed to iomux_run()
 *               by the listener and expirer
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of microseconds to use as timeout.
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status).
 * @return the previous value for the iomux_run_timeout_high setting
 * @note defaults to SHARDCACHE_IOMUX_RUN_TIMEOUT_HIGH
 */
int shardcache_iomux_run_timeout_high(shardcache_t *cache, int new_value);

/*
 * @brief Allows to change the expiration time for cached items
 * @param cache A valid pointer to a shardcache_t structure
 * @param new_value The amount of seconds to use as expire time.\n
 *                  If 0 no expiration will be scheduled for cached items;\n
 *                  If -1 is provided as new_value, no change will be applied
 *                  but the actual value will still be returned
 *                  (effectively querying the actual status).
 * @return the previous value for the expire_time setting
 * @note defaults to SHARDCACHE_IOMUX_EXPIRE_TIME
 */
int shardcache_expire_time(shardcache_t *cache, int new_value);

/*
 * @brief Allows to enable/disable the 'lazy_expiration' mode
 * @param cache       A valid pointer to a shardcache_t structure
 * @param new_value   1 if lazy_expiration is desired, 0 otherwise.\n
 *                    If -1 is provided as new_value, no change will be applied
 *                    but the actual value will still be returned
 *                    (effectively querying the actual status).
 * @return the previous value for the lazy_expiration setting
 * @note When lazy expiration is enabled, cached items will be expired
 *       only when fetched, otherwise the expirer thread will take care
 *       of expiring the cached items as well as volatile items
 * @note defaults to 0
 */
int shardcache_lazy_expiration(shardcache_t *cache, int new_value);

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
void *shardcache_get(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     size_t *vlen,
                     struct timeval *timestamp);

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
size_t shardcache_head(shardcache_t *cache,
                       void *key,
                       size_t klen,
                       void *head,
                       size_t hlen,
                       struct timeval *timestamp);

/**
 * @brief Get partial value data value for a key
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param out     A pointer to the memory where to store the partial data
 * @param olen    The size of the memory pointed by 'out'
 * @param offset  The offset from the beginning of the data we want to retrieve
 * @param timestamp   If provided the timestamp of when the object was loaded into the cache
 *                  will be stored at the specified address
 *
 * @return The size copied in the 'head' pointer
 * @note The returned size might be less than what specified in 'hlen'
 *       if the complete data is smaller than hlen
 */
size_t shardcache_get_offset(shardcache_t *cache,
                             void *key,
                             size_t klen,
                             void *out,
                             size_t *olen,
                             size_t offset,
                             struct timeval *timestamp);

/**
 * @brief Callback passed to shardcache_get_async() to receive the data asynchronously
 * @param key         A valid pointer to the key
 * @param klen        The length of the key
 * @param data        The pointer to the chunk of data
 * @param dlen        The length of the current chunk of data
 * @param total_size  If non zero, it indicates that this is the last chunk and
 *                    tells total size of the data.
 * @note this argument is set only when the data is completed and we know its
 *       real total size. The callback will not be called anymore once this
 *       parameter has been provided
 * @param timestamp   If not NULL, it indicates that this is the last chunk
 *                    and points to the timeval holding the timestamp of when
 *                    the data has been loaded into the cache
 * @note this argument is set only when the data is completed and we know it
 *       has been completely loaded into the cache.
 *       The callback will not be called anymore once this parameter has been
 *       provided
 * @param priv        The priv pointer passed to shardcache_get_async()
 * @return 0 if no errors occurred and more data can be provided safely;\n
 *         -1 if an error occurs and we don't want the callback to be called
 *         again (which could eventually abort the fetch operation)
 */
typedef int (*shardcache_get_async_callback_t)(void *key,
                                               size_t klen,
                                               void *data,
                                               size_t dlen,
                                               size_t total_size,
                                               struct timeval *timestamp,
                                               void *priv);

/**
 * @brief Get the value for a key asynchronously
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param cb      The shardcache_get_async_callback_t which will be
 *                called for each received chunk
 * @param priv    A pointer which will be passed to the
 *                shardcache_get_async_callback_t at each call
 *
 * @return 0 on success, -1 otherwise
 *
 * @note This function might return immediately if the value for the
 *       requested key is already being downloaded (but not complete yet)
 *       In such a case The callback function will be called by the 
 *       thread which is actually downloading the data hence the callback
 *       needs to be thread-safe.
 *
 * @note If the data requested is partially downloaded, the available data
 *       will be immediately passed to the callback and the rest will be 
 *       passed while it's being downloaded.
 */
int shardcache_get_async(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         shardcache_get_async_callback_t cb,
                         void *priv);


/**
 * @brief Get partial value data value for a key asynchronously
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param offset  The offset from where to start retrieving data
 * @param length  The amount of data to fetch
 * @param cb      The shardcache_get_async_callback_t which will be
 *                called for each received chunk
 * @param priv    A pointer which will be passed to the
 *                shardcache_get_async_callback_t at each call
 *
 * @return 0 on success, -1 otherwise
 *
 * @note This function might return immediately if the value for the
 *       requested key is already being downloaded (but not complete yet)
 *       In such a case The callback function will be called by the 
 *       thread which is actually downloading the data hence the callback
 *       needs to be thread-safe.
 *
 * @note If the data requested is partially downloaded, the available data
 *       will be immediately passed to the callback and the rest will be 
 *       passed while it's being downloaded.
 */

int
shardcache_get_offset_async(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            size_t offset,
                            size_t length,
                            shardcache_get_async_callback_t cb,
                            void *priv);

/**
 * @brief Callback expected by all the _async() routines returning an integer result
 *        (basically all apart shardcache_get_async() shardcache_offset_async())
 * @param key     A valid pointer to the key of the command this response refers to
 * @param klen    The length of the key
 * @param res     The integer response returned from the issued command
 * @param priv    The 'priv' pointer previously passed to the _async() function
 * 
 */
typedef void (*shardcache_async_response_callback_t)(void *key, size_t klen, int res, void *priv);

/**
 * @brief Check if a specific key exists on the node responsible for it
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param cb      The shardcache_async_response_callback_t which will be
 *                called once the response is completely retrieved
 * @param priv    A pointer which will be passed to the
 *                shardcache_async_response_callback_t when called
 * @return 1 if exists, 0 if doesn't exist, -1 in case of errors
 */
int shardcache_exists_async(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            shardcache_async_response_callback_t cb,
                            void *priv);

/**
 * @brief Load a key into the cache if not present already,
 *        otherwise update the loaded-timestamp
 * @param cache   A valid pointer to a shardcache_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @return 0 if successfully touched the item, -1 if it doesn't exists
 *           or in case of errors
 */
int shardcache_touch(shardcache_t *cache,
                     void *key,
                     size_t klen);

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
 * @brief Set the value for a key fetching the response asyncrhonously
 * @param cache  A valid pointer to a shardcache_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param value  A valid pointer to the value
 * @param vlen   The length of the value
 * @param expire The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *               storage backend as the filesystem or database ones)
 * @param if_not_exists If this param is true, the value will be set only
 *                      if there isn't one already stored
 * @param cb     The shardcache_async_response_callback_t which will be
 *               called once the result has been retreived
 * @param priv   A pointer which will be passed to the
 *               shardcache_async_response_callback_t when called
 * @return 0 on success, -1 otherwise
 * @see shardcache_set_volatile()
 */
int shardcache_set_async(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         void *value,
                         size_t vlen,
                         time_t expire,
                         int    if_not_exists,
                         shardcache_async_response_callback_t cb,
                         void *priv);


/**
 * @brief Set the value for a key if it doesn't already exist 
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param value A valid pointer to the value
 * @param vlen  The length of the value
 * @return 0 on success, 1 if the key already exists, -1 in case of error
 * @see shardcache_set_volatile()
 */
int shardcache_add(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen);

/**
 * @brief Set a volatile value for a key
 * @param cache  A valid pointer to a shardcache_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param value  A valid pointer to the value
 * @param vlen   The length of the value
 * @param expire The number of seconds after which the volatile value expires
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
 * @brief Set the volatile value for a key if it doesn't already exist 
 * @param cache  A valid pointer to a shardcache_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param value  A valid pointer to the value
 * @param vlen   The length of the value
 * @param expire The number of seconds after which the volatile value expires
 *               If 0 the value will not expire and it will be stored using the
 *               actual storage module (which might evntually be a presistent
 *               storage backend as the filesystem or database ones)
 * @return 0 on success, 1 if the key already exists, -1 in case of error
 * @see shardcache_set_volatile()
 */
int shardcache_add_volatile(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire);

/**
 * @brief Remove the value for a key
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @return 0 on success, -1 otherwise
 * @see shardcache_set_volatile()
 */
int shardcache_del(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Remove the value for a key asynchronously
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param cb    The shardcache_async_response_callback_t which will be
 *              called once the result has been retreived
 * @param priv  A pointer which will be passed to the
 *              shardcache_async_response_callback_t when called
 * @return 0 on success, -1 otherwise
 * @see shardcache_set_volatile()
 */
int shardcache_del_async(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         shardcache_async_response_callback_t cb,
                         void *priv);

/**
 * @brief Remove the value from the cache for a key
 * @note the value will not be removed from the underlying storage
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @return 0 on success, -1 otherwise
 */
int shardcache_evict(shardcache_t *cache, void *key, size_t klen);

/**
 * @brief Get the node owning a specific key
 * @param cache A valid pointer to a shardcache_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @param owner If provided the pointed pointer will be set to the name
 *              (<address:port>) of the node owning the key
 * @param len   If not NULL, the size of the owner string will be stored
 *              in the memory pointed by 'len'
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
 * @param index A pointer to a valid shardcache_storage_index_t structure
 *              previously obtained via the shardcache_get_index() function
 */
void shardcache_free_index(shardcache_storage_index_t *index);

/**
 * @brief   Start a migration process
 * @param cache     A valid pointer to a shardcache_t structure
 * @param nodes     The list of nodes representing the new group to migrate to
 * @param num_nodes The number of nodes in the list
 * @param forward   A boolean flag indicating if the migration command needs to be
 *                  forwarded to all other peers
 * @return 0 on success, -1 otherwise
 */
int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t **nodes,
                               int num_nodes,
                               int forward);

/**
 * @brief   Abort the current migration process
 * @param cache A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_abort(shardcache_t *cache);

/**
 * @brief   End the current migration process by swapping the two continua
 *          and releasing resources for the old one
 * @param cache A valid pointer to a shardcache_t structure
 * @return 0 on success, -1 in case of errors
 *         (for instance if no migration is in progress
 *         when this function is called)
 */
int shardcache_migration_end(shardcache_t *cache);

/*
 *******************************************************************************
 * LOG API 
 *******************************************************************************
 */

/**
 * @brief Initialize the internal log subsystem
 * @param ident The ident used in syslog messages
 * @param loglevel The loglevel to use
 */
void shardcache_log_init(char *ident, int loglevel);

/**
 * @brief Returns the actually configured log lovel
 * @return The loglevel configured at initialization time
 */
unsigned int shardcache_log_level();

/**
 * @brief Log a message at the specified prio and for the specified loglevel
 * @param prio The priority of the message
 * @param dbglevel The debuglevel of this message
 * @param fmt The message
 */
void shardcache_log_message(int prio, int dbglevel, const char *fmt, ...);

/**
 * @brief Convert a binary buffer to an hexstring
 * @param buf The buffer
 * @param len The size of the input buffer
 * @param limit Don't output more than 'limit' bytes
 */
char *shardcache_hex_escape(const char *buf, int len, int limit);

/**
 * @brief Escape all occurences of a specific byte using the provided escape character
 * @param ch     The byte to escape
 * @param esc    The escape byte to use when 'ch' is encountered
 * @param buffer The input buffer to scan
 * @param len    The size of the input buffer
 * @param dest   Where to store the escaped string
 * @param newlen The size of the escaped string
 * @return The number of input bytes scanned
 * @note  The caller MUST release the memory used of the output string when done
 */
unsigned long shardcache_byte_escape(char ch, char esc, char *buffer, unsigned long len, char **dest, unsigned long *newlen);

#define SHC_ERROR(__fmt, __args...)      do { shardcache_log_message(LOG_ERR,     0, __fmt, ## __args); } while (0)
#define SHC_WARNING(__fmt, __args...)    do { shardcache_log_message(LOG_WARNING, 0, __fmt, ## __args); } while (0)
#define SHC_WARN(__fmt, __args...) WARNING(__fmt, ## __args)
#define SHC_NOTICE(__fmt, __args...)     do { shardcache_log_message(LOG_NOTICE,  0, __fmt, ## __args); } while (0)
#define SHC_INFO(__fmt, __args...)       do { shardcache_log_message(LOG_INFO,    0, __fmt, ## __args); } while (0)
#define SHC_DIE(__fmt, __args...)        do { SHC_ERROR(__fmt, ## __args); exit(-1); } while (0)

#define __SHC_DEBUG(__n, __fmt, __args...)  do { if (shardcache_log_level() >= LOG_DEBUG + __n) \
    shardcache_log_message(LOG_DEBUG,   __n + 1, __fmt, ## __args); } while (0)

#define SHC_DEBUG(__fmt, __args...)  __SHC_DEBUG(0, __fmt, ## __args)
#define SHC_DEBUG1(__fmt, __args...) SHC_DEBUG(__fmt, ## __args)
#define SHC_DEBUG2(__fmt, __args...) __SHC_DEBUG(1, __fmt, ## __args)
#define SHC_DEBUG3(__fmt, __args...) __SHC_DEBUG(2, __fmt, ## __args)
#define SHC_DEBUG4(__fmt, __args...) __SHC_DEBUG(3, __fmt, ## __args)
#define SHC_DEBUG5(__fmt, __args...) __SHC_DEBUG(4, __fmt, ## __args)

#endif
