#include "shardcache.h"
#include "counters.h"
#include <linklist.h>
#include <stdio.h>

#define COUNTERS_ALLOC_CHUNK 128

struct __shardcache_counters_s {
    linked_list_t *lookup;
};

shardcache_counters_t *shardcache_init_counters()
{
    shardcache_counters_t *c = calloc(1, sizeof(shardcache_counters_t));
    c->lookup = list_create();
    return c;
}

void shardcache_release_counters(shardcache_counters_t *c)
{
    list_destroy(c->lookup);
    free(c);
}

void
shardcache_counter_add(shardcache_counters_t *c, const char *name, const uint64_t *counter_ptr)
{
    tagged_value_t *tval = list_create_tagged_value_nocopy((char *)name, (void *)counter_ptr);
    list_push_tagged_value(c->lookup, tval);
}

static int
shardcache_counter_remove_helper(void *item, uint32_t idx, void *user)
{
    char *name = (char *)user;
    tagged_value_t *tval = (tagged_value_t *)item;
    if (strcmp(tval->tag, name) == 0) {
        list_destroy_tagged_value(tval);
        return -2;
    }
    return 1;
}

void
shardcache_counter_remove(shardcache_counters_t *c, const char *name)
{
    list_foreach_value(c->lookup, shardcache_counter_remove_helper, (void *)name);
}

typedef struct {
    shardcache_counter_t *counters;
    size_t size;
    uint64_t index;
} counters_iterator_arg_t;

int
shardcache_get_all_counters(shardcache_counters_t *c, shardcache_counter_t **out_counters)
{
    int i = 0;
    size_t size = sizeof(shardcache_counter_t) * COUNTERS_ALLOC_CHUNK;
    shardcache_counter_t *counters = malloc(sizeof(shardcache_counter_t) * COUNTERS_ALLOC_CHUNK);
    list_lock(c->lookup);
    for (i = 0; i < list_count(c->lookup); i++) {
        tagged_value_t *tval = list_pick_tagged_value(c->lookup, i);
        if (i == (size/sizeof(shardcache_counter_t))-1) {
            size += (sizeof(shardcache_counter_t) * COUNTERS_ALLOC_CHUNK);
            counters = realloc(counters, size);
        }
        shardcache_counter_t *counter = &counters[i];
        snprintf(counter->name, sizeof(counter->name), "%s", tval->tag);
        counter->value = (uint64_t)__sync_fetch_and_add((uint64_t *)tval->value, 0);
 
    }
    list_unlock(c->lookup);
    *out_counters = counters;
    return i;
}

int
shardcache_counter_value_add(shardcache_counters_t *c, char *name, int value)
{
    tagged_value_t *tval = list_get_tagged_value(c->lookup, name);
    if (tval)
        return __sync_fetch_and_add((uint64_t *)tval->value, value);
    return 0;
}

int
shardcache_counter_value_sub(shardcache_counters_t *c, char *name, int value)
{
    tagged_value_t *tval = list_get_tagged_value(c->lookup, name);
    if (tval)
        return __sync_fetch_and_sub((uint64_t *)tval->value, value);
    return 0;
}

int
shardcache_counter_value_set(shardcache_counters_t *c, char *name, int value)
{
    tagged_value_t *tval = list_get_tagged_value(c->lookup, name);
    if (tval) {
        int b = 0;
        int old = __sync_fetch_and_add((uint64_t *)tval->value, 0);
        do {
            b = __sync_bool_compare_and_swap((uint64_t *)tval->value, old, value);
        } while (!b);
        return old;
    }
    return 0;
}

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
