#ifndef __SHARDCACHE_COUNTERS_H__
#define __SHARDCACHE_COUNTERS_H__

#include "shardcache.h"

void shardcache_init_counters();
void shardcache_release_counters();

void shardcache_counter_add(const char *name, const uint32_t *counter_ptr);
int shardcache_get_all_counters(shardcache_counter_t **counters);
void shardcache_counter_remove(const char *name);

#endif
