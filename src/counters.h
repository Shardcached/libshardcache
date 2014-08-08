#ifndef __SHARDCACHE_COUNTERS_H__
#define __SHARDCACHE_COUNTERS_H__

typedef struct __shardcache_counters_s shardcache_counters_t;

shardcache_counters_t *shardcache_init_counters();
void shardcache_release_counters(shardcache_counters_t *counters);

void shardcache_counter_add(shardcache_counters_t *counters, const char *name, const uint64_t *counter_ptr);
int shardcache_get_all_counters(shardcache_counters_t *counters, shardcache_counter_t **out);
void shardcache_counter_remove(shardcache_counters_t *counters, const char *name);

int shardcache_counter_value_add(shardcache_counters_t *c, char *name, int value);
int shardcache_counter_value_sub(shardcache_counters_t *c, char *name, int value);
int shardcache_counter_value_set(shardcache_counters_t *c, char *name, int value);

#endif

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
