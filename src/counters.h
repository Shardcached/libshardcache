#ifndef __SHARDCACHE_COUNTERS_H__
#define __SHARDCACHE_COUNTERS_H__

typedef struct __shardcache_counters_s shardcache_counters_t;

shardcache_counters_t *shardcache_init_counters();
void shardcache_release_counters(shardcache_counters_t *counters);

void shardcache_counter_add(shardcache_counters_t *counters, const char *name, const uint32_t *counter_ptr);
int shardcache_get_all_counters(shardcache_counters_t *counters, shardcache_counter_t **out);
void shardcache_counter_remove(shardcache_counters_t *counters, const char *name);

#endif
