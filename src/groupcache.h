#ifndef __GROUPCACHE_H__
#define __GROUPCACHE_H__

/**
 * @file groupcache.h
 * @author Andrea Guzzo
 * @brief groupcache C implementation
 */
#include <sys/types.h>

#define GROUPCACHE_PORT_DEFAULT 9874

#define GROUPCACHE_AUTHKEY_LEN 8

/**
 * @brief Opaque structure representing the actual groupcache instance
 */
typedef struct __groupcache_s groupcache_t;

/**
 * @brief Callback to provide the value for a given key.
 *        The groupcache instance will call this callback
 *        if the value has not been found in the cache
 */
typedef void *(*groupcache_fetch_item_callback_t)(void *key, size_t len, size_t *vlen, void *priv);

/**
 * @brief Callback to store a new value for a given key.
 *        The groupcache instance will call this callback
 *        if a new value needs to be set in the underlying storage
 */
typedef void (*groupcache_store_item_callback_t)(void *key, size_t len, void *value, size_t vlen, void *priv);

/**
 * @brief Callback to remove an existing value for a given key.
 *        The groupcache instance will call this callback
 *        if the value for a given key needs to be removed from the underlying storage
 *        NOTE: the value might still be living in the cache, so only the underlying storage
 *              should be updated accordingly to the removal request. The value pointer
 *              shouldn't still be released at this point.
 *              The groupcache_remove_item_callback_t will be called by the groupcache
 *              instance when the memory associated to the value can be safely released
 */
typedef void (*groupcache_remove_item_callback_t)(void *key, size_t len, void *priv);

/**
 * @brief Callback to release the resources of a previously provided value
 *        The groupcache instance will call this callback
 *        if a value object can now be released since not anymore referenced
 *        by the groupcache instance
 */
typedef void (*groupcache_free_item_callback_t)(void *val);


/**
 * @brief Structure holding all the callback pointers required
 *        by groupcache to interact with underlying storage
 */
typedef struct __groupcache_storage_s {
    groupcache_fetch_item_callback_t  fetch;
    groupcache_store_item_callback_t  store;
    groupcache_remove_item_callback_t remove;
    groupcache_free_item_callback_t   free;
    void *priv;
} groupcache_storage_t;

/**
 * @brief Create a new groupcache instance
 * @arg me : a valid <address:port> null-terminated string representing the new node to be created
 * @arg peers : a list of <address:port> strings representing the peers taking part to the groupcache 'cloud'
 * @arg num_peers : the number of peers present in the peers list
 * @arg storage : a groupcache_storage_t structure holding pointers to the storage callbacks.
 *                NOTE: The newly created instance will copy the pointers to its internal descriptor so
 *                      the resources allocated for the storage structure can be safely released after
 *                      calling groupcache_create()
 * @arg secret : a null-terminated string containing the shared secret used to authenticate incoming messages
 */
groupcache_t *groupcache_create(char *me, 
                        char **peers,
                        int num_peers,
                        groupcache_storage_t *storage,
                        char *secret);

/**
 * @brief Release all the resources used by the groupcache instance
 * @arg cache : the instance to release
 */
void groupcache_destroy(groupcache_t *cache);

/**
 * @brief Get the value for a key
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg vlen : If provided the length of the returned value will be stored at the location pointed by vlen
 * @return A pointer to the stored value if any, NULL otherwise
 */
void *groupcache_get(groupcache_t *cache, void *key, size_t klen, size_t *vlen);

/**
 * @brief Set the value for a key
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg value : A valid pointer to the value
 * @arg vlen : The length of the value
 * @return 0 on success, -1 otherwise
 */
int groupcache_set(groupcache_t *cache, void *key, size_t klen, void *value, size_t vlen);

/**
 * @brief Remove the value for a key
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @return 0 on success, -1 otherwise
 */
int groupcache_del(groupcache_t *cache, void *key, size_t klen);

/**
 * @brief Remove the value from the cache for a key
 *        NOTE: the value will not be removed from the underlying storage
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @return 0 on success, -1 otherwise
 */
int groupcache_evict(groupcache_t *cache, void *key, size_t klen);

/**
 * @brief Get the list of all peers (including this node itself)
 *        taking part to the groupcache 'cloud'
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg num_peers : If provided the number of peers in the returned array will be 
 *                  will be stored at the location pointed by num_peers
 * @return A NULL terminated list containing all the peers <address:port> strings
 */
char **groupcache_get_peers(groupcache_t *cache, int *num_peers);

/**
 * @brief Get the peer owning a specific key
 * @arg cache : A valid pointer to a groupcache_t structure
 * @arg key : A valid pointer to the key
 * @arg klen : The length of the key
 * @arg owner : If provided the pointed pointer will be set to the name
 *              (<address:port>) of the peer owning the key
 * @return 1 if the current node (represented by cache) is the owner of the key, 0 otherwise
 */
int groupcache_test_ownership(groupcache_t *cache, void *key, size_t len, const char **owner);

/**
 * @brief Compute the authentication digest based on the shared secret
 * @arg secret : The shared secret against which to compute the digest
 * @arg auth   : A pointer here to store the computed digest
 *               enough memory must have been allocated to hold the digest 
 *               (GROUPCACHE_AUTHKEY_LEN)
 * @return 0 if the digest has been computed successfully, -1 otherwise
 */
int groupcache_compute_authkey(char *secret, unsigned char *auth);

#endif
