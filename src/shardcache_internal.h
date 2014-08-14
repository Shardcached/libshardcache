/* shardcache_t Definition and Internals
 * NOTE: this file is not intended to be distributed in the binary package
 * In theory the only source files including this header should be shardcache.c
 * and arc_ops.c . The shardcache_t structure is not intended to be accessed
 * directly outside of the logic residing in these two source files.
 * So, if you are not working on one of those and you are looking here for
 * details, close this header file and forget you've ever seen it
 */

#include <linklist.h>
#include <chash.h>
#include <hashtable.h>
#include <queue.h>
#include <iomux.h>
#include <atomic_defs.h>

#include "connections_pool.h"
#include "arc.h"
#include "serving.h"
#include "counters.h"
#include "shardcache.h"
#include "shardcache_replica.h"

#define DEBUG_DUMP_MAXSIZE 128

#define KEY2STR(__k, __l, __o, __ol) \
{ \
    size_t __s = (__l < __ol) ? __l : __ol; \
    memcpy(__o, __k, __s); \
    __o[__s] = 0; \
}

#define LIKELY(__e) __builtin_expect((__e), 1)
#define UNLIKELY(__e) __builtin_expect((__e), 0)

#define __USE_UNIX98
#include <pthread.h>

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif

#ifndef __MACH__
#define SPIN_INIT(__mutex) pthread_spin_init(__mutex, 0)
#else
#define SPIN_INIT(__mutex)
#endif

#ifndef __MACH__
#define SPIN_DESTROY(__mutex) pthread_spin_destroy(__mutex)
#else
#define SPIN_DESTROY(__mutex)
#endif

#ifndef __MACH__
#define SPIN_LOCK(__mutex) pthread_spin_lock(__mutex)
#else
#define SPIN_LOCK(__mutex) OSSpinLockLock(__mutex)
#endif

#ifndef __MACH__
#define SPIN_UNLOCK(__mutex) pthread_spin_unlock(__mutex)
#else
#define SPIN_UNLOCK(__mutex) OSSpinLockUnlock(__mutex)
#endif

#define MUTEX_INIT(__mutex) pthread_mutex_init(__mutex, NULL)

#define MUTEX_INIT_RECURSIVE(__mutex) {\
    pthread_mutexattr_t __attr; \
    pthread_mutexattr_init(&__attr); \
    pthread_mutexattr_settype(&__attr, PTHREAD_MUTEX_RECURSIVE); \
    pthread_mutex_init(__mutex, &__attr); \
    pthread_mutexattr_destroy(&__attr); \
}

#define MUTEX_DESTROY(__mutex) pthread_mutex_destroy(__mutex)

#define MUTEX_LOCK(__mutex) pthread_mutex_lock(__mutex) 

#define MUTEX_UNLOCK(__mutex) pthread_mutex_unlock(__mutex) 

#define CONDITION_INIT(__cond) pthread_cond_init(__cond, NULL)

#define CONDITION_DESTROY(__cond) pthread_cond_destroy(__cond)

#define CONDITION_WAIT(__c, __m) {\
    MUTEX_LOCK(__m); \
    pthread_cond_wait(__c, __m); \
    MUTEX_UNLOCK(__m); \
}


#define CONDITION_TIMEDWAIT(__c, __m, __t) {\
    MUTEX_LOCK(__m); \
    pthread_cond_timedwait(__c, __m, __t); \
    MUTEX_UNLOCK(__m); \
}

#define CONDITION_WAIT_IF(__c, __m, __e) {\
    MUTEX_LOCK(__m); \
    if ((__e)) \
        pthread_cond_wait(__c, __m); \
    MUTEX_UNLOCK(__m); \
}

#define CONDITION_SIGNAL(__c, __m) {\
    pthread_mutex_lock(__m); \
    pthread_cond_signal(__c); \
    pthread_mutex_unlock(__m); \
}


typedef struct chash_t chash_t;

struct __shardcache_s {
    char *me;   // a copy of the label for this node
                // it won't be changed until destruction
    char *addr; // a copy of the local address used for shardcache communication

    shardcache_replica_t *replica;

    shardcache_node_t **shards; // a copy of the shards array provided
                               // at construction time

    int num_shards;   // the number of shards in the array

    arc_t *arc;       // the internal arc instance
    arc_ops_t ops;    // the structure holding the arc operations callbacks
    size_t arc_size;  // the actual size of the arc cache
                      // NOTE: arc_size is updated using the atomic builtins,
                      // don't access it directly but use ATOMIC_READ() instead
                      // (see deps/libhl/src/atomic_defs.h)

    // lock used internally during the migration procedures
    // and when selecting the node owner for a key
#ifdef __MACH__
    OSSpinLock migration_lock;
#else
    pthread_spinlock_t migration_lock;
#endif

    chash_t *chash;   // the internal chash instance

    chash_t *migration;                  // the migration continuum
    shardcache_node_t **migration_shards; // the new shards array after the migration
    int num_migration_shards;            // the new number of shards in the migration_shards array
    int migration_done;                  // boolean value indicating that the migration is complete
                                         // (to be accessed using ATOMIC_READ())

    int use_persistent_storage;    // boolean flag indicating if a persistent storage should be used  

    shardcache_storage_t storage;  // the structure holding the callbacks for the persistent storage 

    hashtable_t *volatile_storage; // an hashtable used as volatile storage

    hashtable_t *cache_timeouts; // hashtable holding the timeout_id of the expiration timers
                                 // for cached objects
    hashtable_t *volatile_timeouts; // hashtable holding the timeout_id for the expiration timers
                                    // for volatile items

    iomux_t *expirer_mux; // iomux used to handle the expiration timers
    pthread_t expirer_th; // the thread taking care of propagating expiration commands


    int evict_on_delete;  // boolean flag indicating if eviction will be automatically
                          // triggered when deleting an existing key 

    int use_persistent_connections; // boolean flag indicating if connections should be persistent
                                    // instead of being closed after serving/sending one complete message
    
    int lazy_expiration; // determines if lazy expiration is on. If on keys are checked for expiration
                         // only when a get() operation occurs, if off they will be expired asynchronously
                         // by a background thread

    int force_caching; // boolean flag indicating if the items fetched from remote peers should be
                       // always cached instead of applying th 10% chance of being kept

    int expire_time;   // global expire time for cached items, if 0 items in the cache will never
                       // expire and will need to be either explicitly or naturally evicted to be
                       // removed from the cache
    
    int iomux_run_timeout_low;  // timeout passed to iomux_run()
                                // by both the expirer and the listener
    int iomux_run_timeout_high; // timeout passed to iomux_run()
                                // by both the async reader and the serving workers


    int serving_look_ahead;     // amount of pipelined requests to handle in parallel
                                // while the current is being served

    shardcache_serving_t *serv; // the serving-subsystem instance

    const char *auth;     // the secret to use for signing messages
                          // (NULL if messages are expected to be unsigned)

    pthread_t migrate_th; // the migration thread

    pthread_t evictor_th; // the evictor thread

    pthread_cond_t evictor_cond;  // condition variable used by the evictor thread
                                  // when waiting for new jobs (instead of actively
                                  // polling on the linked list used as queue)
    pthread_mutex_t evictor_lock; // mutex to use when accessing the evictor_cond
                                  //condition variable
    hashtable_t *evictor_jobs;    // linked list used as queue for eviction jobs

    shardcache_counters_t *counters; // the internal counters instance

#define SHARDCACHE_COUNTER_GETS         0
#define SHARDCACHE_COUNTER_SETS         1
#define SHARDCACHE_COUNTER_DELS         2
#define SHARDCACHE_COUNTER_HEADS        3
#define SHARDCACHE_COUNTER_EVICTS       4
#define SHARDCACHE_COUNTER_EXPIRES      5
#define SHARDCACHE_COUNTER_CACHE_MISSES 6
#define SHARDCACHE_COUNTER_FETCH_REMOTE 7
#define SHARDCACHE_COUNTER_FETCH_LOCAL  8
#define SHARDCACHE_COUNTER_NOT_FOUND    9
#define SHARDCACHE_COUNTER_TABLE_SIZE   10
#define SHARDCACHE_COUNTER_CACHE_SIZE   11
#define SHARDCACHE_COUNTER_CACHED_ITEMS 12
#define SHARDCACHE_COUNTER_ERRORS       13
#define SHARDCACHE_NUM_COUNTERS 14
    struct {
        const char *name; // the exported label of the counter
        uint64_t value;   // the actual value (accessed using the atomic builtins)
    } cnt[SHARDCACHE_NUM_COUNTERS]; // array holding the storage for the counters
                                    // exported as stats

    connections_pool_t *connections_pool; // the connections_pool instance which
                                          // holds/distribute the available
                                          // filedescriptors // when using persistent
                                          // connections

    int tcp_timeout;        // the tcp timeout to use when setting up new connections

    struct {
        pthread_t io_th; // the thread taking care of spooling the asynchronous
                         // i/o operations
        iomux_t *mux;    // the iomux instance used for the asynchronous i/o;
                         // operations
        queue_t *queue;
    } async_context[2];

    int async_index;
    int async_quit;
    int quit;
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

int shardcache_set_internal(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire,
                            int inx,
                            int replica,
                            shardcache_async_response_callback_t cb,
                            void *priv);

int shardcache_del_internal(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            int replica,
                            shardcache_async_response_callback_t cb,
                            void *priv);

int shardcache_set_migration_continuum(shardcache_t *cache, shardcache_node_t **nodes, int num_nodes);

int shardcache_schedule_expiration(shardcache_t *cache, void *key, size_t klen, time_t expire, int is_volatile);
int shardcache_unschedule_expiration(shardcache_t *cache, void *key, size_t klen, int is_volatile);

void shardcache_queue_async_read_wrk(shardcache_t *cache, async_read_wrk_t *wrk);

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
