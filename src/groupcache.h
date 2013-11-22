#ifndef __GROUPCACHE_H__
#define __GROUPCACHE_H__

#include <sys/types.h>

#define GROUPCACHE_PORT_DEFAULT 9874

typedef struct __groupcache_s groupcache_t;

typedef void *(*groupcache_fetch_item_callback_t)(void *key, size_t len, size_t *vlen, void *priv);
typedef void (*groupcache_store_item_callback_t)(void *key, size_t len, void *value, size_t vlen, void *priv);
typedef void (*groupcache_remove_item_callback_t)(void *key, size_t len, void *priv);
typedef void (*groupcache_free_item_callback_t)(void *val);

typedef struct __groupcache_storage_s {
    groupcache_fetch_item_callback_t  fetch;
    groupcache_store_item_callback_t  store;
    groupcache_remove_item_callback_t remove;
    groupcache_free_item_callback_t   free;
    void *priv;
} groupcache_storage_t;

groupcache_t *groupcache_create(char *me, 
                        char **peers,
                        int num_peers,
                        groupcache_storage_t *storage);

void groupcache_destroy(groupcache_t *cache);

void *groupcache_get(groupcache_t *cache, void *key, size_t klen, size_t *vlen);

int groupcache_set(groupcache_t *cache, void *key, size_t klen, void *value, size_t vlen);

int groupcache_del(groupcache_t *cache, void *key, size_t klen);

char **groupcache_get_peers(groupcache_t *cache, int *num_peers);

int groupcache_test_ownership(groupcache_t *cache, void *key, size_t len, const char **owner);

#endif
