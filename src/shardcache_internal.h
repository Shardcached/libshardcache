#include <fbuf.h>
#include <linklist.h>
#include <chash.h>
#include <hashtable.h>
#include <iomux.h>

#include "connections_pool.h"
#include "serving.h"
#include "counters.h"

#define DEBUG_DUMP_MAXSIZE 128

#define KEY2STR(__k, __l, __o, __ol) \
{ \
    size_t __s = (__l < __ol) ? __l : __ol; \
    memcpy(__o, __k, __s); \
    __o[__s] = 0; \
}

typedef struct chash_t chash_t;

struct __shardcache_s {
    char *me;

    shardcache_node_t *shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;
    size_t arc_size;

#ifdef __MACH__
    OSSpinLock migration_lock;
#else
    pthread_spinlock_t migration_lock;
#endif
    chash_t *chash;

    chash_t *migration;
    shardcache_node_t *migration_shards;
    int num_migration_shards;
    int migration_done;

    int use_persistent_storage;
    shardcache_storage_t storage;

    hashtable_t *volatile_storage;
    uint32_t next_expire;
#ifdef __MACH__
    OSSpinLock next_expire_lock;
#else
    pthread_spinlock_t next_expire_lock;
#endif
    pthread_t expirer_th;
    int expirer_started;
    int expirer_quit;

    int evict_on_delete;

    int use_persistent_connections;

    shardcache_serving_t *serv;

    const char *auth;

    pthread_t migrate_th;

    pthread_t evictor_th;
    pthread_mutex_t evictor_lock;
    pthread_cond_t evictor_cond;
    linked_list_t *evictor_jobs;
    int evictor_quit;

    shardcache_counters_t *counters;
#define SHARDCACHE_COUNTER_GETS         0
#define SHARDCACHE_COUNTER_SETS         1
#define SHARDCACHE_COUNTER_DELS         2
#define SHARDCACHE_COUNTER_HEADS        3
#define SHARDCACHE_COUNTER_EVICTS       4
#define SHARDCACHE_COUNTER_CACHE_MISSES 5
#define SHARDCACHE_COUNTER_NOT_FOUND    6
#define SHARDCACHE_COUNTER_TABLE_SIZE   7
#define SHARDCACHE_COUNTER_CACHE_SIZE   8
#define SHARDCACHE_NUM_COUNTERS 9
    struct {
        const char *name;
        uint32_t value;
    } cnt[SHARDCACHE_NUM_COUNTERS];

    connections_pool_t *connections_pool;
    int tcp_timeout;
    pthread_t async_io_th;
    iomux_t *async_mux;
    int async_leave;
};

typedef struct {
    void *data;
    size_t dlen;
    uint32_t expire;
} volatile_object_t;

int shardcache_test_migration_ownership(shardcache_t *cache,
        void *key, size_t klen, char *owner, size_t *len);

int shardcache_get_connection_for_peer(shardcache_t *cache, char *peer);

void shardcache_release_connection_for_peer(shardcache_t *cache, char *peer, int fd);

