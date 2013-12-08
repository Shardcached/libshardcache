#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <signal.h>
#include <fcntl.h>
#include <iomux.h>
#include <limits.h>
#include <fbuf.h>
#include <linklist.h>
#include <siphash.h>
#include <chash.h>

#include "shardcache.h"
#include "arc.h"
#include "connections.h"
#include "messaging.h"
#include "serving.h"
#include "counters.h"


#define HEXDUMP_DATA(__d, __l) {\
    int __i;\
    char *__datap = __d;\
    size_t __datalen = __l;\
    if (__datalen > 256)\
        __datalen = 256;\
    for (__i = 0; __i < __datalen; __i++) {\
        fprintf(stderr, "%02x", __datap[__i]);\
    }\
    if (__datalen < __l)\
        fprintf(stderr, "...");\
}

typedef struct chash_t chash_t;

#define SHARDCACHE_COUNTER_GETS         0
#define SHARDCACHE_COUNTER_SETS         1
#define SHARDCACHE_COUNTER_DELS         2
#define SHARDCACHE_COUNTER_EVICTS       3
#define SHARDCACHE_COUNTER_CACHE_MISSES 4
#define SHARDCACHE_COUNTER_NOT_FOUND    5
#define SHARDCACHE_NUM_COUNTERS 6
static struct {
    const char *name;
    uint32_t value;
} internal_counters[] = {
    { "gets", 0 },
    { "sets", 0 },
    { "dels", 0 },
    { "evicts", 0 },
    { "cache_misses", 0 },
    { "not_found", 0 },
};

struct __shardcache_s {
    char *me;

    shardcache_node_t *shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

    pthread_mutex_t migration_lock;
    chash_t *chash;

    chash_t *migration;
    shardcache_node_t *migration_shards;
    int num_migration_shards;
    int migration_done;

    shardcache_storage_t storage;

    int evict_on_delete;

    shardcache_serving_t *serv;

    const char auth[16];

    pthread_t migrator_th;

    pthread_t evictor_th;
    pthread_mutex_t evictor_lock;
    pthread_cond_t evictor_cond;
    linked_list_t *evictor_jobs;

    shardcache_counters_t *counters;
};

/* This is the object we're managing. It has a key
 * and some data. This data will be loaded when ARC instruct
 * us to do so. */
typedef struct {
    void *key;
    size_t len;
    void *data;
    size_t dlen;
    pthread_mutex_t lock;
} cache_object_t;

/**
 * * Here are the operations implemented
 * */

static void *__op_create(const void *key, size_t len, void *priv)
{
    cache_object_t *obj = malloc(sizeof(cache_object_t));

    obj->len = len;
    obj->key = malloc(len);
    memcpy(obj->key, key, len);
    obj->data = NULL;
    pthread_mutex_init(&obj->lock, NULL);

    return obj;
}

static char *shardcache_get_node_address(shardcache_t *cache, char *label) {
    char *addr = NULL;
    int i;
    for (i = 0; i < cache->num_shards; i++ ){
        if (strcmp(cache->shards[i].label, label) == 0) {
            addr = cache->shards[i].address;
            break;
        }
    }
    pthread_mutex_lock(&cache->migration_lock);
    if (cache->migration && !addr) {
        for (i = 0; i < cache->num_migration_shards; i++) {
            if (strcmp(cache->migration_shards[i].label, label) == 0) {
                addr = cache->migration_shards[i].address;
                break;
            }
        }
    }
    pthread_mutex_unlock(&cache->migration_lock);
    return addr;
}

static int _shardcache_test_ownership(shardcache_t *cache,
                                      void *key,
                                      size_t klen,
                                      char *owner,
                                      size_t *len,
                                      int  migration)
{
    const char *node_name;
    size_t name_len = 0;
    pthread_mutex_lock(&cache->migration_lock);
    chash_t *continuum = cache->chash;

    if (cache->migration && cache->migration_done) { 
        shardcache_migration_end(cache);
    }

    if (migration && cache->migration) {
        continuum = cache->migration;
    } else {
        if (len)
            *len = 0;
        pthread_mutex_unlock(&cache->migration_lock);
        return -1;
    }

    chash_lookup(continuum, key, klen, &node_name, &name_len);
    if (owner) {
        strncpy(owner, node_name,len ? *len :  name_len+1);
    }
    if (len)
        *len = name_len;

    pthread_mutex_unlock(&cache->migration_lock);
    return (strcmp(owner, cache->me) == 0);
}

static int shardcache_test_migration_ownership(shardcache_t *cache,
                                               void *key,
                                               size_t klen,
                                               char *owner,
                                               size_t *len)
{
    int ret = _shardcache_test_ownership(cache, key, klen, owner, len, 1);
    return ret;
}

int shardcache_test_ownership(shardcache_t *cache,
                              void *key,
                              size_t klen,
                              char *owner,
                              size_t *len)
{
    return _shardcache_test_ownership(cache, key, klen, owner, len, 0);
}

static int __op_fetch_from_peer(shardcache_t *cache, cache_object_t *obj, char *peer)
{

#ifdef SHARDCACHE_DEBUG
    char keystr[1024];
    memcpy(keystr, obj->key, obj->len < 1024 ? obj->len : 1024);
    keystr[obj->len] = 0;
    fprintf(stderr, "Fetching data for key %s from peer %s\n", keystr, peer); 
#endif

    char *peer_addr = shardcache_get_node_address(cache, peer);
    if (!peer_addr) {
        // TODO - Error Messages
        return -1;
    }

    // another peer is responsible for this item, let's get the value from there
    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = fetch_from_peer(peer_addr, (char *)cache->auth, obj->key, obj->len, &value);
    if (rc == 0 && fbuf_used(&value)) {
        obj->data = fbuf_data(&value);
        obj->dlen = fbuf_used(&value);
        __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
        return 0;
    } else {
        fbuf_destroy(&value);
        return -1;
    }
}

static int __op_fetch(void *item, void * priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    pthread_mutex_lock(&obj->lock);
    if (obj->data) { // the value is already loaded, we don't need to fetch
        pthread_mutex_unlock(&obj->lock);
        return 0;
    }

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    // if we are not the owner try asking our peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->len, node_name, &node_len))
    {
        int done = 1;
        node_name[node_len] = 0;
        int ret = __op_fetch_from_peer(cache, obj, node_name);
        if (ret == -1) {
            node_len = sizeof(node_name);
            int check = shardcache_test_migration_ownership(cache,
                                                            obj->key,
                                                            obj->len,
                                                            node_name,
                                                            &node_len);
            if (check == 0) {
                done = 0;
            } else if (node_len) {
                node_name[node_len] = 0;
                ret = __op_fetch_from_peer(cache, obj, node_name);
            }
        }
        if (done) {
            pthread_mutex_unlock(&obj->lock);
            return ret;
        }
    }

#ifdef SHARDCACHE_DEBUG
    char keystr[1024];
    memcpy(keystr, obj->key, obj->len < 1024 ? obj->len : 1024);
    keystr[obj->len] = 0;
#endif
    // we are responsible for this item ... so let's fetch it
    if (cache->storage.fetch) {
        void *v = cache->storage.fetch(obj->key, obj->len, &obj->dlen, cache->storage.priv);
#ifdef SHARDCACHE_DEBUG
        if (v && obj->dlen) {
            fprintf(stderr, "Fetch storage callback returned value ");
            
            HEXDUMP_DATA(v, obj->dlen);

            fprintf(stderr, " (%lu) for key %s\n", (unsigned long)obj->dlen, keystr); 
        } else {
            fprintf(stderr, "Fetch storage callback returned an empty value for key %s\n", keystr);
        }
#endif
        if (v && obj->dlen) {
            obj->data = v;
        }
    }

    if (!obj->data) {
        pthread_mutex_unlock(&obj->lock);
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Item not found for key %s\n", keystr);
#endif
        __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_NOT_FOUND].value, 1);
        return -1;
    }
    pthread_mutex_unlock(&obj->lock);
    __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
    return 0;
}

static void __op_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    //shardcache_t *cache = (shardcache_t *)priv;
    pthread_mutex_lock(&obj->lock);
    if (obj->data) {
        free(obj->data);
        obj->data = NULL;
        obj->dlen = 0;
        __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_EVICTS].value, 1);
    }
    pthread_mutex_unlock(&obj->lock);
}

static void __op_destroy(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    //shardcache_t *cache = (shardcache_t *)priv;

    // no lock is necessary here ... if we are here
    // nobody is referencing us anymore
    if (obj->data) {
        free(obj->data);
    }
    free(obj->key);
    pthread_mutex_destroy(&obj->lock);
    free(obj);
}

static void shardcache_do_nothing(int sig)
{
    // do_nothing
}

typedef struct {
    void *key;
    size_t klen;
} shardcache_evictor_job_t;

static void destroy_evictor_job(shardcache_evictor_job_t *job)
{
    free(job->key);
    free(job);
}

static shardcache_evictor_job_t *create_evictor_job(void *key, size_t klen)
{
    shardcache_evictor_job_t *job = malloc(sizeof(shardcache_evictor_job_t)); 
    job->key = malloc(klen);
    memcpy(job->key, key, klen);
    job->klen = klen; 
    return job;
}

static void *evictor(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    linked_list_t *jobs = cache->evictor_jobs;
    for (;;) {
        pthread_testcancel();
        shardcache_evictor_job_t *job = (shardcache_evictor_job_t *)shift_value(jobs);
        while (job) {
#ifdef SHARDCACHE_DEBUG
            char keystr[1024];
            memcpy(keystr, job->key, job->klen < 1024 ? job->klen : 1024);
            keystr[job->klen] = 0;
            fprintf(stderr, "Eviction job for key '%s' started\n", keystr);
#endif
            int i;
            for (i = 0; i < cache->num_shards; i++) {
                char *peer = cache->shards[i].label;
                if (strcmp(peer, cache->me) != 0) {
                    delete_from_peer(cache->shards[i].address, (char *)cache->auth, job->key, job->klen, 0);
                }
            }
            destroy_evictor_job(job);
#ifdef SHARDCACHE_DEBUG
            fprintf(stderr, "Eviction job for key '%s' completed\n", keystr);
#endif
            job = (shardcache_evictor_job_t *)shift_value(jobs);
        }
        pthread_testcancel();
        struct timeval now;
        int rc = 0;
        rc = gettimeofday(&now, NULL);
        if (rc == 0) {
            struct timespec abstime = { now.tv_sec + 1, now.tv_usec * 1000 };
            pthread_mutex_lock(&cache->evictor_lock);
            pthread_cond_timedwait(&cache->evictor_cond, &cache->evictor_lock, &abstime);
            pthread_mutex_unlock(&cache->evictor_lock);
        } else {
            // TODO - Error messsages
        }
    }
    return NULL;
}

shardcache_t *shardcache_create(char *me,
                                shardcache_node_t *nodes,
                                int nnodes,
                                shardcache_storage_t *st,
                                char *secret,
                                int num_workers,
                                int evict_on_delete)
{
    int i;
    size_t shard_lens[nnodes];
    char *shard_names[nnodes];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = evict_on_delete;


    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&cache->migration_lock, &attr); 
    pthread_mutexattr_destroy(&attr);

    if (!st) {
        fprintf(stderr, "No storage defined\n");
        free(cache);
        return NULL;
    }

    memcpy(&cache->storage, st, sizeof(cache->storage));

    cache->me = strdup(me);

    cache->ops.create  = __op_create;
    cache->ops.fetch   = __op_fetch;
    cache->ops.evict   = __op_evict;
    cache->ops.destroy = __op_destroy;

    cache->ops.priv = cache;
    cache->shards = malloc(sizeof(shardcache_node_t) * nnodes);
    memcpy(cache->shards, nodes, sizeof(shardcache_node_t) * nnodes);
    for (i = 0; i < nnodes; i++) {
        shard_names[i] = cache->shards[i].label;
        shard_lens[i] = strlen(shard_names[i]);
    }

    cache->num_shards = nnodes;

    cache->chash = chash_create((const char **)shard_names, shard_lens, cache->num_shards, 200);

    cache->arc = arc_create(&cache->ops, 300);

    // check if there is already signal handler registered on SIGPIPE
    struct sigaction sa;
    if (sigaction(SIGPIPE, NULL, &sa) != 0) {
        fprintf(stderr, "Can't check signal handlers: %s\n", strerror(errno)); 
        shardcache_destroy(cache);
        return NULL;
    }

    // if not we need to register one to handle writes/reads to disconnected sockets
    if (sa.sa_handler == NULL)
        signal(SIGPIPE, shardcache_do_nothing);

    strncpy((char *)cache->auth, secret, sizeof(cache->auth));

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "AUTH KEY (secret: %s): ", secret);
    for (i = 0; i < SHARDCACHE_MSG_SIG_LEN; i++) {
        fprintf(stderr, "%02x", (unsigned char)cache->auth[i]); 
    }
    fprintf(stderr, "\n");
#endif

    cache->counters = shardcache_init_counters();
    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        shardcache_counter_add(cache->counters, internal_counters[i].name, &internal_counters[i].value); 
    }

    if (cache->evict_on_delete) {
        pthread_mutex_init(&cache->evictor_lock, NULL);
        pthread_cond_init(&cache->evictor_cond, NULL);
        cache->evictor_jobs = create_list();
        set_free_value_callback(cache->evictor_jobs, (free_value_callback_t)destroy_evictor_job);
        pthread_create(&cache->evictor_th, NULL, evictor, cache);
    }

    char *addr = shardcache_get_node_address(cache, cache->me);
    if (!addr) {
        fprintf(stderr, "Can't find my address (%s) among the configured nodes\n", cache->me);
        shardcache_destroy(cache);
        return NULL;
    }
    cache->serv = start_serving(cache, cache->auth, addr, num_workers, cache->counters); 
    return cache;
}

void shardcache_destroy(shardcache_t *cache) {
    int i;

    if (cache->serv)
        stop_serving(cache->serv);

    pthread_mutex_lock(&cache->migration_lock);
    if (cache->migration) {
        shardcache_migration_abort(cache);    
    }
    pthread_mutex_unlock(&cache->migration_lock);
    pthread_mutex_destroy(&cache->migration_lock);

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        shardcache_counter_remove(cache->counters, internal_counters[i].name);
    }

    if (cache->arc)
        arc_destroy(cache->arc);

    if (cache->chash)
        chash_free(cache->chash);

    free(cache->me);
    free(cache->shards);

    if (cache->evict_on_delete) {
        pthread_cancel(cache->evictor_th);
        pthread_join(cache->evictor_th, NULL);
        pthread_mutex_destroy(&cache->evictor_lock);
        pthread_cond_destroy(&cache->evictor_cond);
        destroy_list(cache->evictor_jobs);
    }

    shardcache_release_counters(cache->counters);

    free(cache);
}

void *shardcache_get(shardcache_t *cache, void *key, size_t len, size_t *vlen) {
    if (!key)
        return NULL;

    __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_GETS].value, 1);

    char *value = NULL;
    cache_object_t *obj = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, len, (void **)&obj);
    if (!res)
        return NULL;

    if (obj) {
        pthread_mutex_lock(&obj->lock);
        if (obj->data) {
            value = malloc(obj->dlen);
            if (value) {
                memcpy(value, obj->data, obj->dlen);
                if (vlen)
                    *vlen = obj->dlen;
            } else {
                fprintf(stderr, "malloc failed for %zu bytes: %s\n", obj->dlen, strerror(errno));
            }
        }
        pthread_mutex_unlock(&obj->lock);
    }
    arc_release_resource(cache->arc, res);
    return value;
}

int shardcache_set(shardcache_t *cache, void *key, size_t klen, void *value, size_t vlen) {
    // if we are not the owner try propagating the command to the responsible peer
    
    if (!key || !value)
        return -1;

    __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_SETS].value, 1);

    arc_remove(cache->arc, (const void *)key, klen);

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    if (shardcache_test_ownership(cache, key, klen, node_name, &node_len)) {
#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;

        fprintf(stderr, "Storing value ");

        HEXDUMP_DATA(value, vlen);

        fprintf(stderr, " (%d) for key %s\n", (int)vlen, keystr);
#endif
        if (cache->storage.store)
            cache->storage.store(key, klen, value, vlen, cache->storage.priv);
        return 0;
    } else if (node_len) {
#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;

        fprintf(stderr, "Forwarding set command %s => ", keystr);
        
        HEXDUMP_DATA(value, vlen);

        fprintf(stderr, " (%d) to %s\n", (int)vlen, node_name);
#endif
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            // TODO - Error Messages
            return -1;
        }
        return send_to_peer(peer, (char *)cache->auth, key, klen, value, vlen);
    }

    return -1;
}

int shardcache_del(shardcache_t *cache, void *key, size_t klen) {

    if (!key)
        return -1;

    __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_DELS].value, 1);

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    if (shardcache_test_ownership(cache, key, klen, node_name, &node_len))
    {
        node_name[node_len] = 0;
        if (cache->storage.remove)
            cache->storage.remove(key, klen, cache->storage.priv);

        if (cache->evict_on_delete)
        {
            arc_remove(cache->arc, (const void *)key, klen);
            shardcache_evictor_job_t *job = create_evictor_job(key, klen);
            push_value(cache->evictor_jobs, job);
            pthread_mutex_lock(&cache->evictor_lock);
            pthread_cond_signal(&cache->evictor_cond);
            pthread_mutex_unlock(&cache->evictor_lock);
        }
        return 0;
    } else {
        node_name[node_len] = 0;
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            // TODO - Error Messages
            return -1;
        }
        return delete_from_peer(peer, (char *)cache->auth, key, klen, 1);
    }

    return -1;
}

int shardcache_evict(shardcache_t *cache, void *key, size_t klen) {
    if (!key || !klen)
        return -1;

    arc_remove(cache->arc, (const void *)key, klen);
    __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_EVICTS].value, 1);
    return 0;
}

const shardcache_node_t *shardcache_get_nodes(shardcache_t *cache, int *num_nodes) {
    if (num_nodes)
        *num_nodes = cache->num_shards;
    return cache->shards;
}

int shardcache_get_counters(shardcache_t *cache, shardcache_counter_t **counters) {
    return shardcache_get_all_counters(cache->counters, counters); 
}

static void reset_stat_figure(uint32_t *figure_ptr) {
    int done = 0;
    do {
        uint32_t old_val = __sync_fetch_and_add(figure_ptr, 0);
        done = __sync_bool_compare_and_swap(figure_ptr, old_val, 0);
    } while (!done);
}

void shardcache_clear_counters(shardcache_t *cache) {
    if (cache) { }
    int i;
    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i++) {
        reset_stat_figure(&internal_counters[i].value);
    }
}

shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache)
{
    size_t isize = 65535;
    if (cache->storage.count)
        isize = cache->storage.count(cache->storage.priv);

    ssize_t count = 0;
    shardcache_storage_index_item_t *items = NULL;
    if (cache->storage.index) {
        items = calloc(sizeof(shardcache_storage_index_item_t), isize);
        count = cache->storage.index(items, isize, cache->storage.priv);
    }
    shardcache_storage_index_t *index = calloc(1, sizeof(shardcache_storage_index_t));
    index->items = items;
    index->size = count; 
    return index;
}

void shardcache_free_index(shardcache_storage_index_t *index)
{
    if (index->items) {
        int i;
        for (i = 0; i < index->size; i++) {
            if (index->items[i].key)
                free(index->items[i].key);
        }
        free(index->items);
    }
    free(index);
}

void *migrator(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;

    shardcache_storage_index_t *index = shardcache_get_index(cache);

    uint32_t migrated_items = 0;
    uint32_t scanned_items = 0;
    uint32_t errors = 0;
    uint32_t total_items = index->size;

    shardcache_counter_add(cache->counters, "migrated_items", &migrated_items);
    shardcache_counter_add(cache->counters, "scanned_items", &scanned_items);
    shardcache_counter_add(cache->counters, "total_items", &total_items);
    shardcache_counter_add(cache->counters, "migration_errors", &errors);

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Migrator starting (%d items to precess)\n", total_items);
#endif

    int aborted = 0;
    linked_list_t *to_delete = create_list();
    int i;
    for (i = 0; i < index->size; i++) {
        size_t klen = index->items[i].klen;
        void *key = index->items[i].key;

        char node_name[1024];
        size_t node_len = sizeof(node_name);
        memset(node_name, 0, node_len);

        char keystr[1024];
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;

#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Migrator processign key %s\n", keystr);
#endif

        pthread_mutex_lock(&cache->migration_lock);
        if (!cache->migration) {
            fprintf(stderr, "Migrator running while no migration continuum present ... aborting\n");
            __sync_add_and_fetch(&errors, 1);
            pthread_mutex_unlock(&cache->migration_lock);
            aborted = 1;
            break;
        }
        pthread_mutex_unlock(&cache->migration_lock);

        int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
        
        // if we are not the owner try asking our peer responsible for this data
        if (!is_mine)
        {
            void *value = NULL;
            size_t vlen = 0;
            if (cache->storage.fetch) {
                value = cache->storage.fetch(key, klen, &vlen, cache->storage.priv);
            }
            if (value) {
                char *peer = shardcache_get_node_address(cache, (char *)node_name);
                if (peer) {
#ifdef SHARDCACHE_DEBUG
                    fprintf(stderr, "Migrator copying %s to peer %s (%s)\n", keystr, node_name, peer);
#endif
                    int rc = send_to_peer(peer, (char *)cache->auth, key, klen, value, vlen);
                    if (rc == 0) {
                        __sync_add_and_fetch(&migrated_items, 1);
                        push_value(to_delete, &index->items[i]);
                    } else {
                        fprintf(stderr, "Errors copying %s to peer %s (%s)\n", keystr, node_name, peer);
                        __sync_add_and_fetch(&errors, 1);
                    }
                } else {
                    fprintf(stderr, "Can't find address for peer %s\n", node_name);
                    __sync_add_and_fetch(&errors, 1);
                }
            }
        }
        __sync_add_and_fetch(&scanned_items, 1);
    }

    shardcache_counter_remove(cache->counters, "migrated_items");
    shardcache_counter_remove(cache->counters, "scanned_items");
    shardcache_counter_remove(cache->counters, "total_items");
    shardcache_counter_remove(cache->counters, "migration_errors");

    if (!aborted) {
#ifdef SHARDCACHE_DEBUG
            fprintf(stderr, "Migration completed, now removing not-owned  items\n");
#endif
        shardcache_storage_index_item_t *item = shift_value(to_delete);
        while (item) {
            if (cache->storage.remove)
                cache->storage.remove(item->key, item->klen, cache->storage.priv);
#ifdef SHARDCACHE_DEBUG
            char ikeystr[1024];
            memcpy(ikeystr, item->key, item->klen < 1024 ? item->klen : 1024);
            ikeystr[item->klen] = 0;
            fprintf(stderr, "removed item %s\n", ikeystr);
#endif
            item = shift_value(to_delete);
        }
    }
    destroy_list(to_delete);


    pthread_mutex_lock(&cache->migration_lock);
    cache->migration_done = 1;
    pthread_mutex_unlock(&cache->migration_lock);
#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Migrator ended: processed %d items, migrated %d, errors %d\n",
            total_items, migrated_items, errors);
#endif

    if (index)
        free(index);

    return NULL;
}

int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t *nodes,
                               int num_nodes,
                               int forward)
{
    int i, n;
    fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

    pthread_mutex_lock(&cache->migration_lock);

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Starting migration\n");
#endif
    cache->migration_done = 0;
    cache->migration_shards = malloc(sizeof(shardcache_node_t) * num_nodes);
    memcpy(cache->migration_shards, nodes, sizeof(shardcache_node_t) * num_nodes);
    cache->num_migration_shards = num_nodes;

    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    for (i = 0; i < num_nodes; i++) {
        shard_names[i] = nodes[i].label;
        shard_lens[i] = strlen(shard_names[i]);
        if (i > 0) 
            fbuf_add(&mgb_message, ",");
        fbuf_printf(&mgb_message, "%s:%s", nodes[i].label, nodes[i].address);
    }

    if (num_nodes == cache->num_shards) {
        int differ = 0;
        for (i = 0 ; i < num_nodes; i++) {
            int found = 0;
            for (n = 0; n < num_nodes; n++) {
                if (strcmp(nodes[i].label, cache->shards[n].label) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                differ = 1;
                break;
            }
        }
        if (!differ) {
            free(cache->migration_shards);
            cache->migration_shards = NULL;
            fbuf_destroy(&mgb_message);
            fprintf(stderr,
                    "The migration continuum contains exactly the same nodes"
                    "of the actual continuum... ignoring migration\n");
            pthread_mutex_unlock(&cache->migration_lock);
            return -1;
        }
    }
    cache->migration = chash_create((const char **)shard_names,
                                    shard_lens,
                                    num_nodes,
                                    200);

    pthread_create(&cache->migrator_th, NULL, migrator, cache);
    pthread_mutex_unlock(&cache->migration_lock);
    if (forward) {
        for (i = 0; i < num_nodes; i++) {
            if (strcmp(nodes[i].label, cache->me) != 0) {
                int rc = migrate_peer(nodes[i].address,
                                      (char *)cache->auth,
                                      fbuf_data(&mgb_message),
                                      fbuf_used(&mgb_message));
                if (rc != 0) {
                    // TODO - handle errors
                }
            }
        }
    }

    return 0;
}

int shardcache_migration_abort(shardcache_t *cache)
{
    int ret = -1;
    pthread_mutex_lock(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->migration);
        free(cache->migration_shards);
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Migration aborted\n");
#endif
        ret = 0;
    }
    cache->migration = NULL;
    cache->migration_shards = NULL;

    pthread_mutex_unlock(&cache->migration_lock);
    pthread_join(cache->migrator_th, NULL);
    return ret;
}

int shardcache_migration_end(shardcache_t *cache)
{
    int ret = -1;
    pthread_mutex_lock(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->chash);
        free(cache->shards);
        cache->chash = cache->migration;
        cache->shards = cache->migration_shards;
        cache->migration = NULL;
        cache->migration_shards = NULL;
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Migration ended\n");
#endif
        ret = 0;
    }
    cache->migration_done = 0;
    pthread_mutex_unlock(&cache->migration_lock);
    pthread_join(cache->migrator_th, NULL);
    return ret;
}
