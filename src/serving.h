#include "shardcache.h"

typedef struct __shardcache_serving_s {
    shardcache_t *cache;
    int sock;
    const char *auth;
    const char *me;
} shardcache_serving_t;

void *accept_requests(void *priv);
