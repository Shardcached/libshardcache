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
#include <iomux.h>

#include "connections_pool.h"
#include "arc.h"
#include "serving.h"
#include "counters.h"
#include "atomic.h"
#include "shardcache.h"
#include "shardcache_replica.h"

#define DEBUG_DUMP_MAXSIZE 128

#define KEY2STR(__k, __l, __o, __ol) \
{ \
    size_t __s = (__l < __ol) ? __l : __ol; \
    memcpy(__o, __k, __s); \
    __o[__s] = 0; \
}

typedef struct chash_t chash_t;

struct __shardcache_node_s {
    char *label;
    char **address;
    int num_replicas;
    char *string;
}; 

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
                      // (see atomic.h)

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

    uint32_t next_expire; // the expiration time of the volatile item (if any).
                          // in seconds (epoch)
    pthread_t expirer_th; // the thread taking care of propagating expiration commands
    int expirer_started;  // flag indicating the the expirer thread has started

    int evict_on_delete;  // boolean flag indicating if eviction will be automatically
                          // triggered when deleting an existing key 

    int use_persistent_connections; // boolean flag indicating if connections should be persistent
                                    // instead of being closed after serving/sending one complete message

    shardcache_serving_t *serv;     // the serving-subsystem instance

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
#define SHARDCACHE_COUNTER_CACHE_MISSES 5
#define SHARDCACHE_COUNTER_FETCH_REMOTE 6
#define SHARDCACHE_COUNTER_FETCH_LOCAL  7
#define SHARDCACHE_COUNTER_NOT_FOUND    8
#define SHARDCACHE_COUNTER_TABLE_SIZE   9
#define SHARDCACHE_COUNTER_CACHE_SIZE   10
#define SHARDCACHE_NUM_COUNTERS 11
    struct {
        const char *name; // the exported label of the counter
        uint32_t value;   // the actual value (accessed using the atomic builtins)
    } cnt[SHARDCACHE_NUM_COUNTERS]; // array holding the storage for the counters
                                    // exported as stats

    connections_pool_t *connections_pool; // the connections_pool instance which
                                          // holds/distribute the available
                                          // filedescriptors // when using persistent
                                          // connections

    int tcp_timeout;        // the tcp timeout to use when setting up new connections
    pthread_t async_io_th;  // the thread taking care of spooling the asynchronous
                            // i/o operations
    iomux_t *async_mux;     // the iomux instance used for the asynchronous i/o;
                            // operations
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
                            shardcache_set_async_callback_t cb,
                            void *priv);

int shardcache_del_internal(shardcache_t *cache, void *key, size_t klen, int replica);

int shardcache_set_migration_continuum(shardcache_t *cache, shardcache_node_t **nodes, int num_nodes);

