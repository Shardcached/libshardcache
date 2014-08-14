#ifndef __SHARDCACHE_NODE_H__
#define __SHARDCACHE_NODE_H__

/**
 * @brief Structure representing a node taking part in the shard cache
 * @see shardcache_create()
 * @see shardcache_get_nodes()
 */
typedef struct __shardcache_node_s shardcache_node_t;

#include <shardcache.h>

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
 * @brief Create a new shardcache_node_t structure by parsing a full node string
 * @param str A valid node string
 * @return A newly initialized shardcache_node_t structure
 * @note The caller MUST release the resources used to represent the node by calling
 *       shardcache_node_destroy() on the returned pointer
 */
shardcache_node_t *shardcache_node_create_from_string(char *str);

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

#endif

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
