#ifndef __SHARDCACHE_CLIENT_H__
#define __SHARDCACHE_CLIENT_H__

#include <shardcache.h>

typedef struct shardcache_client_s shardcache_client_t;

shardcache_client_t *shardcache_client_create(shardcache_node_t *nodes, int num_nodes, char *auth);

size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data);
int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire);
int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen);
int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen);

void shardcache_client_destroy(shardcache_client_t *c);

#endif
