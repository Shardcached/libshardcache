#include "shardcache.h"

typedef struct __shardcache_serving_s shardcache_serving_t;

void *accept_requests(void *priv);
shardcache_serving_t *start_serving(shardcache_t *cache,
                                    const char *auth,
                                    const char *me,
                                    int num_workers,
                                    shardcache_counters_t *counters);
void stop_serving(shardcache_serving_t *s);
