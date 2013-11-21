#ifndef __GROUPCACHE_H__
#define __GROUPCACHE_H__

#include <sys/types.h>

typedef struct __groupcache_s groupcache_t;

typedef void *(*groupcache_fetch_item_callback_t)(void *key, size_t len, size_t *vlen, void *priv);
typedef void (*groupcache_store_item_callback_t)(void *key, size_t len, void *value, size_t vlen, void *priv);
typedef void (*groupcache_free_item_callback_t)(void *val);

groupcache_t *groupcache_create(char *me, 
                        char **peers,
                        int num_peers,
                        groupcache_fetch_item_callback_t fetch_cb,
                        groupcache_store_item_callback_t store_cb,
                        groupcache_free_item_callback_t free_cb,
                        void *priv);

void groupcache_destroy(groupcache_t *cache);
void *groupcache_get(groupcache_t *gc, void *key, size_t klen, size_t *vlen);
void *groupcache_set(groupcache_t *gc, void *key, size_t klen, void *value, size_t *vlen);

char **groupcache_get_peers(groupcache_t *cache, int *num_peers);

#endif
