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

    chash_t *chash;

    shardcache_storage_t storage;

    int evict_on_delete;

    shardcache_serving_t *serv;

    const char auth[16];

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
    return addr;
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
    memset(node_name, 0, sizeof(node_name));
    size_t node_len = 0;
    // if we are not the owner try asking our peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->len, node_name, &node_len)) {
#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, obj->key, obj->len < 1024 ? obj->len : 1024);
        keystr[obj->len] = 0;
        fprintf(stderr, "Fetching data for key %s from peer %s\n", keystr, node_name); 
#endif
        node_name[node_len] = 0;
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            // TODO - Error Messages
            pthread_mutex_unlock(&obj->lock);
            return -1;
        }
        // another peer is responsible for this item, let's get the value from there
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        int rc = fetch_from_peer(peer, (char *)cache->auth, obj->key, obj->len, &value);
        if (rc == 0 && fbuf_used(&value)) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            pthread_mutex_unlock(&obj->lock);
            __sync_add_and_fetch(&internal_counters[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
            return 0;
        } else {
            pthread_mutex_unlock(&obj->lock);
            fbuf_destroy(&value);
            return -1;
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
    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        shardcache_counter_remove(cache->counters, internal_counters[i].name);
    }

    if (cache->serv)
        stop_serving(cache->serv);

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
    memset(node_name, 0, sizeof(node_name));
    size_t node_len = 0;
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
        node_name[node_len] = 0;
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
    memset(node_name, 0, sizeof(node_name));
    size_t node_len = 0;
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

int shardcache_test_ownership(shardcache_t *cache, void *key, size_t klen, char *owner, size_t *len)
{
    const char *node_name;
    size_t name_len = 0;
    chash_lookup(cache->chash, key, klen, &node_name, &name_len);
    if (owner) {
        strncpy(owner, node_name, name_len);
    }
    if (len)
        *len = name_len;
    return (strcmp(owner, cache->me) == 0);
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
