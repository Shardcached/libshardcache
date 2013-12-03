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
#include <signal.h>
#include <fcntl.h>
#include <iomux.h>
#include <fbuf.h>
#include <siphash.h>

#include "shardcache.h"
#include "arc.h"
#include "connections.h"
#include "messaging.h"
#include "serving.h"

#include <chash.h>

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

struct __shardcache_s {
    char *me;

    char **shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

    chash_t *chash;

    shardcache_storage_t storage;

    int evict_on_delete;

    shardcache_serving_t *serv;
    shardcache_stats_t stats;

    const char auth[16];
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
        // another peer is responsible for this item, let's get the value from there
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        int rc = fetch_from_peer((char *)node_name, (char *)cache->auth, obj->key, obj->len, &value);
        if (rc == 0 && fbuf_used(&value)) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            pthread_mutex_unlock(&obj->lock);
            __sync_add_and_fetch(&cache->stats.ncache_misses, 1);
            return 0;
        }
    }

    // we are responsible for this item ... so let's fetch it
    if (cache->storage.fetch) {
        void *v = cache->storage.fetch(obj->key, obj->len, &obj->dlen, cache->storage.priv);
#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, obj->key, obj->len < 1024 ? obj->len : 1024);
        keystr[obj->len] = 0;

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
        __sync_add_and_fetch(&cache->stats.nnot_found, 1);
        return -1;
    }
    pthread_mutex_unlock(&obj->lock);
    __sync_add_and_fetch(&cache->stats.ncache_misses, 1);
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

shardcache_t *shardcache_create(char *me,
                                char **peers,
                                int npeers,
                                shardcache_storage_t *st,
                                char *secret,
                                int num_workers)
{
    int i;
    size_t shard_lens[npeers + 1];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = 1;

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
    // shards will contain all the peers (including me) plus a
    // trailing NULL pointer (thus npeers + 2)
    cache->shards = malloc(sizeof(char *) * (npeers + 2));
    for (i = 0; i < npeers; i++) {
        cache->shards[i] = strdup(peers[i]);
        shard_lens[i] = strlen(cache->shards[i]);
    }
    cache->shards[npeers] = cache->me;
    shard_lens[npeers] = strlen(me);

    cache->num_shards = npeers + 1;
    cache->shards[cache->num_shards] = NULL;

    cache->chash = chash_create((const char **)cache->shards, shard_lens, cache->num_shards, 200);

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

    cache->serv = start_serving(cache, cache->auth, cache->me, num_workers);

    return cache;
}

void shardcache_destroy(shardcache_t *cache) {
    int i;
    if (cache->serv)
        stop_serving(cache->serv);

    if (cache->arc)
        arc_destroy(cache->arc);

    if (cache->chash)
        chash_free(cache->chash);
    //free(cache->me);
    for (i = 0; i < cache->num_shards; i++)
        free(cache->shards[i]);
    free(cache->shards);
    free(cache);
}

void *shardcache_get(shardcache_t *cache, void *key, size_t len, size_t *vlen) {
    if (!key)
        return NULL;

    __sync_add_and_fetch(&cache->stats.ngets, 1);

    char *value = NULL;
    cache_object_t *obj = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, len, (void **)&obj);
    if (!res)
        return NULL;

    if (obj) {
        pthread_mutex_lock(&obj->lock);
        if (obj->data) {
            value = malloc(obj->dlen);
            memcpy(value, obj->data, obj->dlen);
            if (vlen)
                *vlen = obj->dlen;
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

    __sync_add_and_fetch(&cache->stats.nsets, 1);

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
        node_name[node_len] = 0;
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
        return send_to_peer((char *)node_name, (char *)cache->auth, key, klen, value, vlen);
    }

    return -1;
}

int shardcache_del(shardcache_t *cache, void *key, size_t klen) {

    if (!key)
        return -1;

    __sync_add_and_fetch(&cache->stats.ndels, 1);

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
            /* TODO - we might want to use a backgroud thread to propagate the eviction requests
             *        to all our peers. This would mean that the deleted value might still be found
             *        in the cache of some other node when we return from this function.
             *        But on the other hand we wouldn't block the caller until all the connections
             *        have been handled ... involving eventual timeouts and slowdowns
             */
            int i;
            for (i = 0; i < cache->num_shards; i++) {
                char *peer = cache->shards[i];
                if (strcmp(peer, cache->me) != 0) {
                    delete_from_peer(peer, (char *)cache->auth, key, klen, 0);
                }
            }
        }
        return 0;
    } else {
        return delete_from_peer((char *)node_name, (char *)cache->auth, key, klen, 1);
    }

    return -1;
}

int shardcache_evict(shardcache_t *cache, void *key, size_t klen) {
    if (!key || !klen)
        return -1;

    arc_remove(cache->arc, (const void *)key, klen);
    return 0;
}

char **shardcache_get_peers(shardcache_t *cache, int *num_peers) {
    if (num_peers)
        *num_peers = cache->num_shards;
    return cache->shards;
}

void shardcache_get_stats(shardcache_t *cache, shardcache_stats_t *stats) {
    stats->ngets = __sync_fetch_and_add(&cache->stats.ngets, 0);
    stats->nsets = __sync_fetch_and_add(&cache->stats.nsets, 0);
    stats->ndels = __sync_fetch_and_add(&cache->stats.ndels, 0);
    stats->ncache_misses = __sync_fetch_and_add(&cache->stats.ncache_misses, 0);
    stats->nnot_found = __sync_fetch_and_add(&cache->stats.nnot_found, 0);
}

static void reset_stat_figure(uint32_t *figure_ptr) {
    int done = 0;
    do {
        uint32_t old_val = __sync_fetch_and_add(figure_ptr, 0);
        done = __sync_bool_compare_and_swap(figure_ptr, old_val, 0);
    } while (!done);
}

void shardcache_clear_stats(shardcache_t *cache) {
    reset_stat_figure(&cache->stats.ngets);
    reset_stat_figure(&cache->stats.nsets);
    reset_stat_figure(&cache->stats.ndels);
    reset_stat_figure(&cache->stats.ncache_misses);
    reset_stat_figure(&cache->stats.nnot_found);
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
