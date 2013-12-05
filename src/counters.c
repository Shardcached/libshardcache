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
    c->lookup = create_list();
    return c;
}

void shardcache_release_counters(shardcache_counters_t *c)
{
    destroy_list(c->lookup);
    free(c);
}

void
shardcache_counter_add(shardcache_counters_t *c, const char *name, const uint32_t *counter_ptr)
{
    tagged_value_t *tval = create_tagged_value_nocopy((char *)name, (void *)counter_ptr);
    push_tagged_value(c->lookup, tval);
}

void
shardcache_counter_remove(shardcache_counters_t *c, const char *name)
{
    int i;
    for (i = 0; i < list_count(c->lookup); i++) {
        tagged_value_t *tval = pick_tagged_value(c->lookup, i);
        if (strcmp(tval->tag, name) == 0) {
            tval = fetch_tagged_value(c->lookup, i);
            destroy_tagged_value(tval);
            break;
        }  
    }
}

typedef struct {
    shardcache_counter_t *counters;
    size_t size;
    uint32_t index;
} counters_iterator_arg_t;

int
shardcache_get_all_counters(shardcache_counters_t *c, shardcache_counter_t **out_counters)
{
    int i = 0;
    shardcache_counter_t *counters = calloc(sizeof(shardcache_counter_t), COUNTERS_ALLOC_CHUNK);
    size_t size = COUNTERS_ALLOC_CHUNK;
    for (i = 0; i < list_count(c->lookup); i++) {
        tagged_value_t *tval = pick_tagged_value(c->lookup, i);
        if (i == size-1) {
            size += COUNTERS_ALLOC_CHUNK;
            counters = realloc(counters, size);
        }
        shardcache_counter_t *counter = &counters[i];
        snprintf(counter->name, sizeof(counter->name), "%s", tval->tag);
        counter->value = (uint32_t)__sync_fetch_and_add((uint32_t *)tval->value, 0);
 
    }
    *out_counters = counters;
    return i;
}

