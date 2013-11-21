#ifndef __GROUPCACHE_H__
#define __GROUPCACHE_H__

#include <sys/types.h>

typedef struct __groupcache_s groupcache_t;

typedef void *(*groupcache_fetch_item_callback_t)(void *key, size_t len, size_t *vlen, void *priv);
typedef void (*groupcache_free_item_callback_t)(void *val);

groupcache_t *groupcache_create(char *me, 
                        char **peers,
                        int npeers,
                        groupcache_fetch_item_callback_t fetch_cb,
                        groupcache_free_item_callback_t free_cb,
                        void *priv);

void groupcache_destroy(groupcache_t *cache);
void *groupcache_get(groupcache_t *gc, void *key, size_t klen, size_t *vlen);


#endif
