#include <linklist.h>
#include "arc.h"
#include "atomic.h"


/* This is the object we're managing. It has a key
 * and some data. This data will be loaded when ARC instruct
 * us to do so. */
typedef struct {
    arc_t *arc;
    void *key;
    size_t klen;
    void *data;
    size_t dlen;
    struct timeval ts;
    int async;
    linked_list_t *listeners;
    int complete;
    int evict;
    // we want a mutex here because the object might be locked
    // for long time if involved in a fetch or store operation
    pthread_mutex_t lock;
} cache_object_t;

typedef struct {
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_listener_t;

void * arc_ops_create(const void *key, size_t len, int async, void *priv);
size_t arc_ops_fetch(void *item, void * priv);
void arc_ops_evict(void *item, void *priv);
void arc_ops_destroy(void *item, void *priv);

