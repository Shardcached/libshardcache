#ifndef __SHARDCACHE_CLIENT_H__
#define __SHARDCACHE_CLIENT_H__

#include <shardcache.h>

#define SHARDCACHE_CLIENT_OK             0
#define SHARDCACHE_CLIENT_ERROR_NODE     1
#define SHARDCACHE_CLIENT_ERROR_NETWORK  2
#define SHARDCACHE_CLIENT_ERROR_ARGS     3
#define SHARDCACHE_CLIENT_ERROR_PROTOCOL 4
#define SHARDCACHE_CLIENT_ERROR_INTERNAL 5

/**
 * @brief Opaque structure representing the shardcache client
 */
typedef struct shardcache_client_s shardcache_client_t;

/**
 * @brief Create a new shardcache client
 *
 * @param nodes           A list of <address:port> strings representing the nodes
 *                        taking part to the shardcache 'cloud'
 * @param num_nodes       The number of nodes present in the nodes list
 * @param auth            A null-terminated string containing the shared secret used to
 * @return A newly initialized shardcache client descriptor
 * @note The returned shardcache_client_t structure MUST be disposed using shardcache_client_destroy()
 */
shardcache_client_t *shardcache_client_create(shardcache_node_t **nodes, int num_nodes, char *auth);

/**
 * @brief Get and/or set the timeout (in milliseconds) used when establishing new tcp connections or
 *        reading/writing from/to existing ones
 * @param c         A valid pointer to a shardcache_client_t structure
 * @param new_value If greater or equal to 0 the new value will be set.
 *                  Otherwise the old value will be queried but no new value
 *                  will be set
 * @return The previously configured tcp timeout
 *         (still valid if no new value has been provided)
 */
int shardcache_client_tcp_timeout(shardcache_client_t *c, int new_value);


/**
 * @brief Get and/or set the timeout used when determining if a spare connection needs
 *        to be checked before being returned to the caller for further usage
 * @param c         A valid pointer to a shardcache_client_t structure
 * @param new_value If greater or equal to 0 the new value will be set.
 *                  Otherwise the old value will be queried but no new value
 *                  will be set
 * @return The previously configured check_connection timeout
 *         (still valid if no new value has been provided)
 */
int shardcache_client_check_connection_timeout(shardcache_client_t *c, int new_value);

/**
 * @brief Get and/or set the maximum time (in seconds) to wait on multi commands
 *        (get_multi/set_multi) when no data is arriving.
 * @note This setting represents a timeout which is reset each time some data is received
 *       and it prevents the get_multi()/set_multi() commands from hanging indefinitely 
 *       waiting for data. A value of 0 indicates no timeout
 * @param c         A valid pointer to a shardcache_client_t structure
 * @param new_value If greater or equal to 0 the new value will be set.
 *                  Otherwise the old value will be queried but no new value
 *                  will be set. A value of 0 indicates no timeout.
 * @return The previously configured max_wait timeout
 *         (still valid if no new value has been provided)
 */
int shardcache_client_multi_command_max_wait(shardcache_client_t *c, int new_value);

/**
 * @brief Get and/or set the random_node mode on a shardcache client instance.
 *        When on a single (random) node will be used for all the commands instead
 *        of using the chash algorithm to determine the owner.
 * @param c         A valid pointer to a shardcache_client_t structure
 * @param new_value If false, the chash algorithm will be used against the list
 *        of known node to determine the owner of a key and commands will be sent
 *        only to owners.\n If true a single random node will be used instead
 *
 */
int shardcache_client_use_random_node(shardcache_client_t *c, int new_value);

/**
 * @brief Get and/or set the maximum number of requests that can be pipelined
 *        on a single connection
 * @param c         A valid pointer to a shardcache_client_t structure
 * @param new_value If greater or equal to 0 the new value will be set.
 *                  Otherwise the old value will be queried but no new value
 *                  will be set
 * @note  this setting affects only the _multi commands, which will pipeline
 *        at most pipeline_max requests on a single connections and creating
 *        how many connections are necessary to fulfill all the requests
 * @return The previously configured value for the pipeline_max option
 *         (still valid if no new value has been provided)
 */
int shardcache_client_pipeline_max(shardcache_client_t *c, int new_value);

/**
 * @brief Get the value for a key
 * @param c       A valid pointer to a shardcache_client_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param data    A reference to the pointer which will be set to point to the memory
 *                holding the retrieved value
 *
 * @return the size of the memory pointed by *data, 0 if no data was found or in case of error
 * @note The caller can distinguish between 'no-data' and 'error' conditions by looking at the
 *       internal errno by using shardcache_client_errno()
 * @note The caller is responsible of releasing the memory pointed by *data (if any)
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 *
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data);

/**
 * @brief Get part of the value for a key
 * @param c       A valid pointer to a shardcache_client_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param offset  The offset from which to start copying the value
 * @param data    A pointer where the partial data will be copied
 * @param dlen    The size of the data pointer (also the size we are interested into)
 *
 * @return the size actually copied to the data pointer
 *
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 *
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
size_t shardcache_client_offset(shardcache_client_t *c, void *key, size_t klen, uint32_t offset, void *data, uint32_t dlen);

/**
 * @brief Callback passed to shardcache_client_get_async()
 *        to retrieve the data asynchronously
 * @param node   The node from which we are receiving the new chunk of data
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param data   The pointer to the chunk of data
 * @param dlen   The length of the current chunk of data
 * @param error  True (1) if there was an error. data will point to NULL and dlen will be 0 as well
 * @param priv   The priv pointer passed to shardcache_client_get_async()
 *
 * @note data == NULL and dlen == 0 indicates that the opartion finished
 *       error will be set to 1 if the operation failed, 0 otherwise
 */
typedef int (*shardcache_client_get_aync_data_cb)(char *node,
                                                  void *key,
                                                  size_t klen,
                                                  void *data,
                                                  size_t dlen,
                                                  int error,
                                                  void *priv);

/**
 * @brief Get the value for a key asynchronously
 * @param c       A valid pointer to a shardcache_client_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @param cb      The shardcache_client_get_aync_data_cb which will be
 *                called for each received chunk
 * @param priv    A pointer which will be passed to the
 *                shardcache_client_get_aync_data_cb at each call
 *
 * @return 0 on success, -1 otherwise
 *
 * @note This function will block and call the provided callback as soon 
 *       as a chunk of data is read from the node.
 *       The control will be returned to the caller when there is no
 *       more data to read or an error occurred
 */
int shardcache_client_get_async(shardcache_client_t *c,
                                void *key,
                                size_t klen,
                                shardcache_client_get_aync_data_cb cb,
                                void *priv);

/**
 * @brief Check if a specific key exists on the node responsible for it
 * @param c      A valid pointer to a shardcache_client_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @return 1 if exists, 0 if doesn't exist, -1 in case of errors
 */
int shardcache_client_exists(shardcache_client_t *c, void *key, size_t klen);

/**
 * @brief Force loading a key into the cache of the node responsible for it.
 *        If the key is already loaded, update the loaded-timestamp.
 * @param c      A valid pointer to a shardcache_client_t structure
 * @param key     A valid pointer to the key
 * @param klen    The length of the key
 * @return 0 if successfully touched the item, -1 in case of errors
 */
int shardcache_client_touch(shardcache_client_t *c, void *key, size_t klen);


/**
 * @brief Set the value for a key if it doesn't exist already
 * @param c      A valid pointer to a shardcache_client_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param data   A valid pointer to the value
 * @param dlen   The length of the value
 * @param expire The number of seconds after which the value should expire
 * @return 0 on success, 1 if the key already exists,
 *         -1 in case of errors and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_add(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire);

/**
 * @brief Set the value for a key
 * @param c      A valid pointer to a shardcache_client_t structure
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param data   A valid pointer to the value
 * @param dlen   The length of the value
 * @param expire The number of seconds after which the value should expire,
 *               0 If the value is persistent and shouldn't expire.
 * @return 0 on success, -1 otherwise and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire);

/**
 * @brief Remove the value for a key
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @return 0 on success, -1 otherwise the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen);

/**
 * @brief Evict (remove from the cache) the value for a key
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param key   A valid pointer to the key
 * @param klen  The length of the key
 * @return 0 on success, -1 otherwise and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen);

/**
 * @brief Get the stats from a shardcache node
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param node_name  The name of the node we want to get stats from
 * @param buf   A reference to the pointer which will be set to point to the memory
 *              holding the retrieved stats
 * @param len If not NULL, the size of memory pointed by *buf is stored in *len
 * @return 0 on success, -1 otherwise and the internal errno is set
 * @note The caller is responsible of releasing the memory eventually pointed by *buf
 *       by using free()
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_stats(shardcache_client_t *c, char *node_name, char **buf, size_t *len);

/**
 * @brief Check the status of a shardcache node
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param node_name  The name of the node we want to get stats from
 * @return 0 success, -1 otherwise and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_check(shardcache_client_t *c, char *node_name);

/**
 * @brief Start a migration
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param nodes           A list of <address:port> strings representing the nodes
 *                        taking part to the shardcache 'cloud'
 * @param num_nodes       The number of nodes present in the nodes list
 * @return 0 success, -1 otherwise and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_migration_begin(shardcache_client_t *c, shardcache_node_t **nodes, int num_nodes);

/**
 * @brief Abort the current migration (if any)
 * @param c     A valid pointer to a shardcache_client_t structure
 * @return 0 success, -1 otherwise and the internal errno is set
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
int shardcache_client_migration_abort(shardcache_client_t *c);


/**
 * @brief Get the index from a shardcache node
 * @param c     A valid pointer to a shardcache_client_t structure
 * @param node_name  The name of the node we want to get stats from
 * @return 0 success, -1 otherwise
 * @note The caller must use shardcache_free_index() to release memory used
 *       by the returned shardcache_storage_index_t pointer
 * @note On success the internal errno will be set to SHARDCACHE_CLIENT_OK
 * @see shardcache_client_errno()
 * @see shardcache_client_errstr()
 */
shardcache_storage_index_t *shardcache_client_index(shardcache_client_t *c, char *node_name);

/**
 * @brief Return the error code for the last operation performed by the shardcache client
 * @param c     A valid pointer to a shardcache_client_t structure
 * @return The errno
 * @see shardcache_client_errno()
 */
int shardcache_client_errno(shardcache_client_t *c);

/**
 * @brief Return the error string for the last operation performed by the shardcache client
 * @param c     A valid pointer to a shardcache_client_t structure
 * @return The error string
 * @see shardcache_client_errno()
 */
char *shardcache_client_errstr(shardcache_client_t *c);

/**
 * @brief Release all the resources used by the shardcache client instance
 * @param c     A valid pointer to a shardcache_client_t structure to release
 */
void shardcache_client_destroy(shardcache_client_t *c);

typedef struct {
    void *key;
    size_t klen;
    void *data;
    size_t dlen;
    int status;
    uint32_t expire;
    uint32_t idx; // reserved for internal use
} shc_multi_item_t;

/**
 * @brief Helper to create item-descriptors provided as parameter
 *        to shardcache_client_get_multi() and shardcache_client_set_multi()
 * @param key    A valid pointer to the key
 * @param klen   The length of the key
 * @param data   A valid pointer to the value
 * @param dlen   The length of the value
 * @return A pointer to newly initialized shc_multi_item_t structure which can be
 *         included in the array provided as parameter to shardcache_client_get_multi()
 *         or shardcache_client_get_multi()
 * @note the caller MUST release the resources allocated for the item descriptor by using
 *       shc_multi_item_destroy() when done with it
 *
 */
shc_multi_item_t *shc_multi_item_create(void  *key,
                                        size_t klen,
                                        void  *data,
                                        size_t dlen);

/**
 * @brief Release all the resources allocad for the shc_multi_item_t structure
 * @param item A valid pointer to an initialized shc_multi_item_t structure
 */
void shc_multi_item_destroy(shc_multi_item_t *item);


/**
 * @brief get multiple keys at once
 *
 * @param c          A valid pointer to a shardcache_client_t structure to release
 * @param items      A NULL-terminated array of shc_multi_item_t structures
 *
 * @note the operation will per parallelized among multiple nodes if possible
 */
int shardcache_client_get_multi(shardcache_client_t *c,
                                shc_multi_item_t **items);
/**
 * @brief get multiple keys at once
 *
 * @param c          A valid pointer to a shardcache_client_t structure to release
 * @param items      A NULL-terminated array of shc_multi_item_t structures
 *
 * @note the operation will per parallelized among multiple nodes if possible
 */
int shardcache_client_set_multi(shardcache_client_t *c,
                                shc_multi_item_t **items);

/**
 * @brief get the node used to fulfil last request
 * @param c          A valid pointer to a shardcache_Client_t structure
 */
shardcache_node_t *shardcache_client_current_node(shardcache_client_t *c);

int shardcache_client_getf(shardcache_client_t *c, void *key, size_t klen);

int shardcache_client_get_multif(shardcache_client_t *c, shc_multi_item_t **items);

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
