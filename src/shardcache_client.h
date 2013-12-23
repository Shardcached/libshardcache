#ifndef __SHARDCACHE_CLIENT_H__
#define __SHARDCACHE_CLIENT_H__

#include <shardcache.h>

#define SHARDCACHE_CLIENT_OK             0
#define SHARDCACHE_CLIENT_ERROR_PEER     1
#define SHARDCACHE_CLIENT_ERROR_NETWORK  2

typedef struct shardcache_client_s shardcache_client_t;

shardcache_client_t *shardcache_client_create(shardcache_node_t *nodes, int num_nodes, char *auth);

size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data);
int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire);
int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen);
int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen);
int shardcache_client_stats(shardcache_client_t *c, char *peer, char **buf, size_t *len);
int shardcache_client_check(shardcache_client_t *c, char *peer);

int shardcache_client_errno(shardcache_client_t *c);
char *shardcache_client_errstr(shardcache_client_t *c);

void shardcache_client_destroy(shardcache_client_t *c);

#endif
