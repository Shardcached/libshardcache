#include "counters.h"
#include <hashtable.h>

#define COUNTERS_ALLOC_CHUNK 64

static hashtable_t *lookup = NULL;

void shardcache_init_counters()
{
    lookup = ht_create(1<<8, 1<<16, NULL);  
}

void shardcache_release_counters()
{
    ht_destroy(lookup);
}

void
shardcache_counter_add(const char *name, const uint32_t *counter_ptr)
{
    ht_set(lookup, (void *)name, strlen(name),
                   (void *)counter_ptr, sizeof(uint32_t));
}

void
shardcache_counter_remove(const char *name)
{
    ht_delete(lookup, (char *)name, strlen(name), NULL, NULL);
}

typedef struct {
    shardcache_counter_t *counters;
    size_t size;
    uint32_t index;
} counters_iterator_arg_t;

static int
collect_counters(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    counters_iterator_arg_t *arg = (counters_iterator_arg_t *)user;
    if (arg->index == arg->size-1) {
        arg->size += COUNTERS_ALLOC_CHUNK;
        arg->counters = realloc(arg->counters, arg->size);
    }
    shardcache_counter_t *counter = &arg->counters[arg->index++];
    size_t namelen = klen;
    if (klen > sizeof(counter->name))
        namelen = sizeof(counter->name);
    memcpy(counter->name, key, namelen);
    counter->name[klen] = 0;
    counter->value = __sync_fetch_and_add((uint32_t *)value, 0);
    return 1;
}

int
shardcache_get_all_counters(shardcache_counter_t **counters)
{
    counters_iterator_arg_t collect = { 
        .counters = calloc(sizeof(shardcache_counter_t), COUNTERS_ALLOC_CHUNK),
        .size = COUNTERS_ALLOC_CHUNK,
        .index = 0
    };
    ht_foreach_pair(lookup, collect_counters, &collect); 
    *counters = collect.counters;
    return collect.index;
}

