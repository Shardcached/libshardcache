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
#include <limits.h>
#include <fbuf.h>
#include <linklist.h>
#include <siphash.h>
#include <chash.h>
#include <hashtable.h>
#include <iomux.h>

#include "shardcache.h"
#include "arc.h"
#include "connections.h"
#include "connections_pool.h"
#include "messaging.h"
#include "serving.h"
#include "counters.h"

#ifdef __MACH__
#include <libkern/OSAtomic.h>
#endif

#ifdef __MACH__
#define SPIN_LOCK(__mutex) OSSpinLockLock(__mutex)
#define SPIN_UNLOCK(__mutex) OSSpinLockUnlock(__mutex)
#else
#define SPIN_LOCK(__mutex) pthread_spin_lock(__mutex)
#define SPIN_UNLOCK(__mutex) pthread_spin_unlock(__mutex)
#endif

#define DEBUG_DUMP_MAXSIZE 128

#define KEY2STR(__k, __l, __o, __ol) \
{ \
    size_t __s = (__l < __ol) ? __l : __ol; \
    memcpy(__o, __k, __s); \
    __o[__s] = 0; \
}

typedef struct chash_t chash_t;

extern int shardcache_log_initialized;

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
    pthread_mutex_t async_lock;
    int async_leave;
};

typedef struct {
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_listener_t;

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
    // we want a mutex here because the object might be locked
    // for long time if involved in a fetch or store operation
    pthread_mutex_t lock;
} cache_object_t;

typedef struct {
    void *data;
    size_t dlen;
    uint32_t expire;
} volatile_object_t;

/**
 * * Here are the operations implemented
 * */

static void *__op_create(const void *key, size_t len, int async, void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    cache_object_t *obj = calloc(1, sizeof(cache_object_t));

    obj->klen = len;
    obj->key = malloc(len);
    memcpy(obj->key, key, len);
    obj->data = NULL;
    obj->complete = 0;
    if (async) {
        obj->async = async;
        obj->listeners = create_list();
        set_free_value_callback(obj->listeners, free);
    }
    pthread_mutex_init(&obj->lock, NULL);
    obj->arc = cache->arc;

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
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration && !addr) {
        for (i = 0; i < cache->num_migration_shards; i++) {
            if (strcmp(cache->migration_shards[i].label, label) == 0) {
                addr = cache->migration_shards[i].address;
                break;
            }
        }
    }
    SPIN_UNLOCK(&cache->migration_lock);
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

    if (len && *len == 0)
        return -1;

    if (cache->num_shards == 1)
        return 1;

    SPIN_LOCK(&cache->migration_lock);

    chash_t *continuum = NULL;
    if (cache->migration && cache->migration_done) { 
        shardcache_migration_end(cache);
    } 

    if (migration) {
        if (cache->migration) {
            continuum = cache->migration;
        } else {
            SPIN_UNLOCK(&cache->migration_lock);
            return -1;
        }
    } else {
        continuum = cache->chash;
    }

    chash_lookup(continuum, key, klen, &node_name, &name_len);
    if (owner) {
        if (len && name_len + 1 > *len)
            name_len = *len - 1;
        memcpy(owner, node_name, name_len);
        owner[name_len] = 0;
    }
    if (len)
        *len = name_len;

    SPIN_UNLOCK(&cache->migration_lock);
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

int shardcache_get_connection_for_peer(shardcache_t *cache, char *peer)
{
    if (!cache->use_persistent_connections)
        return connect_to_peer(peer, cache->tcp_timeout);

    // this will reuse an available filedescriptor already connected to peer
    // or create a new connection if there isn't any available
    return connections_pool_get(cache->connections_pool, peer);
}

void shardcache_release_connection_for_peer(shardcache_t *cache, char *peer, int fd)
{
    if (fd < 0)
        return;

    if (!cache->use_persistent_connections) {
        close(fd);
        return;
    }
    // put back the fildescriptor into the connection cache
    connections_pool_add(cache->connections_pool, peer, fd);
}

typedef struct {
    cache_object_t *obj;
    void *data;
    size_t len;
} shardcache_fetch_from_peer_notify_arg;

static int
shardcache_fetch_from_peer_notify_listener (void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    shardcache_fetch_from_peer_notify_arg *arg = (shardcache_fetch_from_peer_notify_arg *)user;
    cache_object_t *obj = arg->obj;
    int rc = listener->cb(obj->key, obj->klen, arg->data, arg->len, 0, NULL, listener->priv);
    return (rc == 0) ? 1 : -1;
}

static int
shardcache_fetch_from_peer_notify_listener_complete(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cache_object_t *obj = (cache_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, obj->dlen, &obj->ts, listener->priv);
    return -1;
}

static int
shardcache_fetch_from_peer_notify_listener_error(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cache_object_t *obj = (cache_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, 0, NULL, listener->priv);
    return -1;
}

static int
shardcache_fetch_from_peer_async_cb(char *peer,
                                    void *key,
                                    size_t klen,
                                    void *data,
                                    size_t len,
                                    int error,
                                    void *priv)
{
    cache_object_t *obj = (cache_object_t *)priv;
    pthread_mutex_lock(&obj->lock);
    if (error) {
        foreach_list_value(obj->listeners, shardcache_fetch_from_peer_notify_listener_error, obj);
        arc_remove(obj->arc, obj->key, obj->klen);
    } else if (len) {
        obj->data = realloc(obj->data, obj->dlen + len);
        memcpy(obj->data + obj->dlen, data, len);
        obj->dlen += len;
        shardcache_fetch_from_peer_notify_arg arg = {
            .obj = obj,
            .data = data,
            .len = len
        };
        foreach_list_value(obj->listeners, shardcache_fetch_from_peer_notify_listener, &arg);
    } else {
        foreach_list_value(obj->listeners, shardcache_fetch_from_peer_notify_listener_complete, obj);
        obj->complete = 1;
    }
    pthread_mutex_unlock(&obj->lock);
    return !error ? 0 : -1;
}

static int __op_fetch_from_peer(shardcache_t *cache, cache_object_t *obj, char *peer)
{
    int rc = -1;
    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));
        SHC_DEBUG("Fetching data for key %s from peer %s", keystr, peer); 
    }

    char *peer_addr = shardcache_get_node_address(cache, peer);
    if (!peer_addr) {
        SHC_ERROR("Can't find address for node %s\n", peer);
        return rc;
    }

    // another peer is responsible for this item, let's get the value from there
    fbuf_t value = FBUF_STATIC_INITIALIZER;

    int fd = shardcache_get_connection_for_peer(cache, peer_addr);
    if (obj->async) {
        pthread_mutex_lock(&cache->async_lock);
        rc = fetch_from_peer_async(peer_addr,
                                   (char *)cache->auth,
                                   SHC_HDR_CSIGNATURE_SIP,
                                   obj->key,
                                   obj->klen,
                                   shardcache_fetch_from_peer_async_cb,
                                   obj,
                                   fd,
                                   cache->async_mux);
        pthread_mutex_unlock(&cache->async_lock);
        if (rc != 0) {
            foreach_list_value(obj->listeners, shardcache_fetch_from_peer_notify_listener_error, obj);
            arc_remove(cache->arc, obj->key, obj->klen);
        }
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
    } else { 
        rc = fetch_from_peer(peer_addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, obj->key, obj->klen, &value, fd);
        if (rc == 0 && fbuf_used(&value)) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
            obj->complete = 1;
        }
    }
    shardcache_release_connection_for_peer(cache, peer_addr, fd);

    fbuf_destroy(&value);
    return rc;
}

static void * copy_volatile_object_cb(void *ptr, size_t len)
{
    volatile_object_t *item = (volatile_object_t *)ptr;
    volatile_object_t *copy = malloc(sizeof(volatile_object_t));
    copy->data = malloc(item->dlen);
    memcpy(copy->data, item->data, item->dlen);
    copy->dlen = item->dlen;
    copy->expire = item->expire;
    return copy;
}

static size_t __op_fetch(void *item, void * priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    pthread_mutex_lock(&obj->lock);
    if (obj->data) { // the value is already loaded, we don't need to fetch
        pthread_mutex_unlock(&obj->lock);
        return obj->dlen;
    }

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    // if we are not the owner try asking our peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->klen, node_name, &node_len))
    {
        int done = 1;
        int ret = __op_fetch_from_peer(cache, obj, node_name);
        if (ret == -1) {
            node_len = sizeof(node_name);
            int check = shardcache_test_migration_ownership(cache,
                                                            obj->key,
                                                            obj->klen,
                                                            node_name,
                                                            &node_len);
            if (check == 0)
                ret = __op_fetch_from_peer(cache, obj, node_name);
            else if (check == 1 || cache->storage.shared)
                done = 0;
        }
        if (done) {
            if (ret == 0) {
                gettimeofday(&obj->ts, NULL);
                size_t dlen = obj->dlen;
                pthread_mutex_unlock(&obj->lock);
                return dlen;
            }
            return 0;
        }
    }

    char keystr[1024];
    if (shardcache_log_level() >= LOG_DEBUG)
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));

    // we are responsible for this item ... 
    // let's first check if it's among the volatile keys otherwise
    // fetch it from the storage
    volatile_object_t *vobj = ht_get_deep_copy(cache->volatile_storage,
                                               obj->key,
                                               obj->klen,
                                               NULL,
                                               copy_volatile_object_cb);
    if (vobj) {
        obj->data = vobj->data; 
        obj->dlen = vobj->dlen;
        free(vobj);
        if (shardcache_log_level() >= LOG_DEBUG) {
            if (obj->data && obj->dlen) {
                SHC_DEBUG2("Found volatile value %s (%lu) for key %s",
                       shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE),
                       (unsigned long)obj->dlen, keystr);
            }
        }
    } else if (cache->use_persistent_storage && cache->storage.fetch) {
        obj->data = cache->storage.fetch(obj->key, obj->klen, &obj->dlen, cache->storage.priv);

        if (shardcache_log_level() >= LOG_DEBUG) {
            if (obj->data && obj->dlen) {
                SHC_DEBUG2("Fetch storage callback returned value %s (%lu) for key %s",
                       shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE),
                       (unsigned long)obj->dlen, keystr);
            } else {
                SHC_DEBUG2("Fetch storage callback returned an empty value for key %s", keystr);
            }
        }
    }

    if (!obj->data) {
        pthread_mutex_unlock(&obj->lock);
        if (shardcache_log_level() >= LOG_DEBUG)
            SHC_DEBUG("Item not found for key %s", keystr);
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_NOT_FOUND].value, 1);
        return 0;
    }
    gettimeofday(&obj->ts, NULL);
    pthread_mutex_unlock(&obj->lock);
    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);

    obj->complete = 1;

    if (obj->async)
        foreach_list_value(obj->listeners, shardcache_fetch_from_peer_notify_listener_complete, obj);

    return obj->dlen;
}

static void __op_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;
    pthread_mutex_lock(&obj->lock);
    if (obj->data) {
        free(obj->data);
        obj->data = NULL;
        obj->dlen = 0;
        obj->complete = 0;
        obj->async = 0;
        clear_list(obj->listeners);
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_EVICTS].value, 1);
    }
    pthread_mutex_unlock(&obj->lock);
}

static void __op_destroy(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;

    // no lock is necessary here ... if we are here
    // nobody is referencing us anymore
    if (obj->data) {
        free(obj->data);
    }
    if (obj->key)
        free(obj->key);

    if (obj->listeners)
        destroy_list(obj->listeners);

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
} shardcache_key_t;

typedef shardcache_key_t shardcache_evictor_job_t;

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

    while (!__sync_fetch_and_add(&cache->evictor_quit, 0))
    {
        shardcache_evictor_job_t *job = (shardcache_evictor_job_t *)shift_value(jobs);
        while (job) {
            char keystr[1024];
            KEY2STR(job->key, job->klen, keystr, sizeof(keystr));
            SHC_DEBUG2("Eviction job for key '%s' started", keystr);

            int i;
            for (i = 0; i < cache->num_shards; i++) {
                char *peer = cache->shards[i].label;
                if (strcmp(peer, cache->me) != 0) {
                    SHC_DEBUG3("Sending Eviction command to %s", peer);
                    int fd = shardcache_get_connection_for_peer(cache, cache->shards[i].address);
                    int rc = evict_from_peer(cache->shards[i].address, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, job->key, job->klen, fd);
                    if (rc != 0)
                        SHC_WARNING("evict_from_peer return %d for peer %s", rc, peer);
                    shardcache_release_connection_for_peer(cache, cache->shards[i].address, fd);
                }
            }
            destroy_evictor_job(job);

            SHC_DEBUG2("Eviction job for key '%s' completed", keystr);

            job = (shardcache_evictor_job_t *)shift_value(jobs);
        }
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

static void destroy_volatile(volatile_object_t *obj)
{
    if (obj->data)
        free(obj->data);
    free(obj);
}

typedef struct {
    linked_list_t *list;
    time_t now;
    uint32_t next;
} expire_volatile_arg_t;

typedef shardcache_key_t expire_volatile_item_t;

static int expire_volatile(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    expire_volatile_arg_t *arg = (expire_volatile_arg_t *)user;
    volatile_object_t *v = (volatile_object_t *)value;
    if (v->expire && v->expire < arg->now) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("Key %s expired", keystr);
        expire_volatile_item_t *item = malloc(sizeof(expire_volatile_item_t));
        item->key = malloc(klen);
        memcpy(item->key, key, klen);
        item->klen = klen;
        push_value(arg->list, item);
    } else if (v->expire && (!arg->next || v->expire < arg->next)) {
        arg->next = v->expire;
    }
    return 1;
}

void *shardcache_expire_volatile_keys(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    while (!__sync_fetch_and_add(&cache->expirer_quit, 0))
    {
        time_t now = time(NULL);

        SPIN_LOCK(&cache->next_expire_lock);

        if (cache->next_expire && now >= cache->next_expire && ht_count(cache->volatile_storage)) {
            int prev_expire = cache->next_expire;
            SPIN_UNLOCK(&cache->next_expire_lock);

            expire_volatile_arg_t arg = {
                .list = create_list(),
                .now = time(NULL),
                .next = 0
            };

            ht_foreach_pair(cache->volatile_storage, expire_volatile, &arg);

            SPIN_LOCK(&cache->next_expire_lock);
            if (cache->next_expire == prev_expire) // nobody advanced the next_expire yet
                cache->next_expire = arg.next;
            else if (cache->next_expire && arg.next)
                cache->next_expire = cache->next_expire < arg.next ? cache->next_expire : arg.next;
            SPIN_UNLOCK(&cache->next_expire_lock);

            expire_volatile_item_t *item = shift_value(arg.list);
            while (item) {
                volatile_object_t *prev = NULL;
                ht_delete(cache->volatile_storage, item->key, item->klen, (void **)&prev, NULL);
                if (prev) {
                    __sync_sub_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                                         prev->dlen);
                    destroy_volatile(prev);
                }
                arc_remove(cache->arc, (const void *)item->key, item->klen);
                free(item->key);
                free(item);
                item = shift_value(arg.list);
            }

            destroy_list(arg.list);

        } else {
            SPIN_UNLOCK(&cache->next_expire_lock);
        }

        sleep(1);
    }
    return NULL;
}

void *shardcache_run_async(void *priv)
{
    struct timeval timeout = { 0, 20000 };
    shardcache_t *cache = (shardcache_t *)priv;
    while (!__sync_fetch_and_add(&cache->async_leave, 0)) {
        pthread_mutex_lock(&cache->async_lock);
        iomux_run(cache->async_mux, &timeout);
        pthread_mutex_unlock(&cache->async_lock);
    }
    return NULL;
}

shardcache_t *shardcache_create(char *me,
                                shardcache_node_t *nodes,
                                int nnodes,
                                shardcache_storage_t *st,
                                char *secret,
                                int num_workers,
                                size_t cache_size)
{
    int i;
    size_t shard_lens[nnodes];
    char *shard_names[nnodes];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = 1;
    cache->use_persistent_connections = 1;
    cache->tcp_timeout = SHARDCACHE_TCP_TIMEOUT_DEFAULT;

#ifndef __MACH__
    pthread_spin_init(&cache->migration_lock, 0);
#endif

    if (st) {
        memcpy(&cache->storage, st, sizeof(cache->storage));
        cache->use_persistent_storage = 1;
    } else {
        SHC_NOTICE("No storage callbacks provided,"
                   "using only the internal volatile storage");
        cache->use_persistent_storage = 0;
    }


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

    cache->arc = arc_create(&cache->ops, cache_size);
    cache->arc_size = cache_size;

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

    if (secret && *secret) {
        cache->auth = calloc(1, 16);
        strncpy((char *)cache->auth, secret, 16);
    } 

    if (secret && *secret) {
        SHC_DEBUG("AUTH KEY (secret: %s) : %s\n", secret,
                  shardcache_hex_escape(cache->auth, SHARDCACHE_MSG_SIG_LEN, DEBUG_DUMP_MAXSIZE));
    }

    const char *counters_names[SHARDCACHE_NUM_COUNTERS] =
        { "gets", "sets", "dels", "heads", "evicts", "cache_misses",
          "not_found", "volatile_table_size", "cache_size" };

    cache->counters = shardcache_init_counters();

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        cache->cnt[i].name = counters_names[i];
        shardcache_counter_add(cache->counters, cache->cnt[i].name, &cache->cnt[i].value); 
    }

    if (__sync_fetch_and_add(&cache->evict_on_delete, 0)) {
        pthread_mutex_init(&cache->evictor_lock, NULL);
        pthread_cond_init(&cache->evictor_cond, NULL);
        cache->evictor_jobs = create_list();
        set_free_value_callback(cache->evictor_jobs,
                                (free_value_callback_t)destroy_evictor_job);
        pthread_create(&cache->evictor_th, NULL, evictor, cache);
    }

    char *addr = shardcache_get_node_address(cache, cache->me);
    if (!addr) {
        fprintf(stderr, "Can't find my address (%s) among the configured nodes\n", cache->me);
        shardcache_destroy(cache);
        return NULL;
    }

    cache->volatile_storage = ht_create(1<<16, 1<<20, (ht_free_item_callback_t)destroy_volatile);

    cache->connections_pool = connections_pool_create(cache->tcp_timeout);

    cache->async_mux = iomux_create();
    pthread_mutex_init(&cache->async_lock, NULL);
    if (pthread_create(&cache->async_io_th, NULL, shardcache_run_async, cache) != 0) {
        fprintf(stderr, "Can't create the async i/o thread: %s\n", strerror(errno));
        shardcache_destroy(cache);
        return NULL;
    }

    cache->serv = start_serving(cache, cache->auth, addr, num_workers, cache->counters); 
    if (!cache->serv) {
        fprintf(stderr, "Can't start the communication engine\n");
        shardcache_destroy(cache);
        return NULL;
    }

#ifndef __MACH__
    pthread_spin_init(&cache->next_expire_lock, 0);
#endif
    pthread_create(&cache->expirer_th, NULL, shardcache_expire_volatile_keys, cache);
    cache->expirer_started = 1;

    if (!shardcache_log_initialized)
        shardcache_log_init("libshardcache", LOG_WARNING);

    return cache;
}

void shardcache_destroy(shardcache_t *cache)
{
    int i;

    if (cache->serv)
        stop_serving(cache->serv);

    __sync_add_and_fetch(&cache->async_leave, 1);
    pthread_join(cache->async_io_th, NULL);
    iomux_destroy(cache->async_mux);

    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        shardcache_migration_abort(cache);    
    }
    SPIN_UNLOCK(&cache->migration_lock);
#ifndef __MACH__
    pthread_spin_destroy(&cache->migration_lock);
#endif

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        shardcache_counter_remove(cache->counters, cache->cnt[i].name);
    }

    if (__sync_fetch_and_add(&cache->evict_on_delete, 0)) {
        __sync_add_and_fetch(&cache->evictor_quit, 1);
        pthread_join(cache->evictor_th, NULL);
        pthread_mutex_destroy(&cache->evictor_lock);
        pthread_cond_destroy(&cache->evictor_cond);
        destroy_list(cache->evictor_jobs);
    }

    shardcache_release_counters(cache->counters);

    if (cache->expirer_started) {
        __sync_add_and_fetch(&cache->expirer_quit, 1);
        pthread_join(cache->expirer_th, NULL);
    }

    ht_destroy(cache->volatile_storage);
#ifndef __MACH__
    pthread_spin_destroy(&cache->next_expire_lock);
#endif

    if (cache->auth)
        free((void *)cache->auth);

    if (cache->arc)
        arc_destroy(cache->arc);

    if (cache->chash)
        chash_free(cache->chash);

    free(cache->me);
    free(cache->shards);


    connections_pool_destroy(cache->connections_pool);

    free(cache);
}

size_t
shardcache_get_offset(shardcache_t *cache,
                      void *key,
                      size_t klen,
                      void *data,
                      size_t *dlen,
                      size_t offset,
                      struct timeval *timestamp)
{
    size_t vlen = 0;
    size_t copied = 0;
    if (!key)
        return 0;

    if (offset == 0)
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_GETS].value, 1);

    cache_object_t *obj = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, (void **)&obj, 0);
    if (!res)
        return 0;

    if (obj) {
        pthread_mutex_lock(&obj->lock);
        if (obj->data) {
            if (dlen && data) {
                if (offset < obj->dlen) {
                    int size = obj->dlen - offset;
                    copied = size < *dlen ? size : *dlen;
                    memcpy(data, obj->data + offset, copied);
                    *dlen = copied;
                }
            }
            if (timestamp)
                memcpy(timestamp, &obj->ts, sizeof(struct timeval));
        }
        vlen = obj->dlen;
        pthread_mutex_unlock(&obj->lock);
    }
    arc_release_resource(cache->arc, res);
    uint32_t size = (uint32_t)arc_size(cache->arc);
    uint32_t prev = __sync_fetch_and_add(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value, 0);
    __sync_bool_compare_and_swap(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
                                 prev,
                                 size);
    return (offset < vlen + copied) ? (vlen - offset - copied) : 0;
}


typedef struct {
    int stat;
    size_t dlen;
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_async_helper_arg_t;

static int
shardcache_get_async_helper(void *key,
                            size_t klen,
                            void *data,
                            size_t dlen,
                            size_t total_size,
                            struct timeval *timestamp,
                            void *priv)
{
    shardcache_get_async_helper_arg_t *arg = (shardcache_get_async_helper_arg_t *)priv;

    int rc = arg->cb(key, klen, data, dlen, total_size, timestamp, arg->priv);
    if (rc != 0 || (!dlen && !total_size)) { // error
        arg->stat = -1;
        free(arg);
        return -1;
    }

    arg->dlen += dlen;

    if (total_size) {
        free(arg);
    }

    return 0;
}

int
shardcache_get_async(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     shardcache_get_async_callback_t cb,
                     void *priv)
{
    if (!key)
        return -1;

    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_GETS].value, 1);

    cache_object_t *obj = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, (void **)&obj, 1);

    if (!res)
        return -1;

    if (!obj) {
        arc_release_resource(cache->arc, res);
        return -1;
    }

    pthread_mutex_lock(&obj->lock);
    if (obj->complete) {
        cb(key, klen, obj->data, obj->dlen, obj->dlen, &obj->ts, priv);
    } else {
        if (obj->dlen) // let's send what we have so far
            cb(key, klen, obj->data, obj->dlen, 0, NULL, priv);

        shardcache_get_async_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_async_helper_arg_t));
        arg->cb = cb;
        arg->priv = priv;

        shardcache_get_listener_t *listener = malloc(sizeof(shardcache_get_listener_t));
        listener->cb = shardcache_get_async_helper;
        listener->priv = arg;
        push_value(obj->listeners, listener);
   }

    pthread_mutex_unlock(&obj->lock);
    arc_release_resource(cache->arc, res);


    uint32_t size = (uint32_t)arc_size(cache->arc);
    uint32_t prev = __sync_fetch_and_add(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value, 0);
    __sync_bool_compare_and_swap(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
                                 prev,
                                 size);

    return 0;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    fbuf_t data;
    size_t stat;
    struct timeval ts;
    int complete;
} shardcache_get_helper_arg_t;

static int
shardcache_get_helper(void *key,
                      size_t klen,
                      void *data,
                      size_t dlen,
                      size_t total_size,
                      struct timeval *timestamp,
                      void *priv)
{
    shardcache_get_helper_arg_t *arg = (shardcache_get_helper_arg_t *)priv;
    pthread_mutex_lock(&arg->lock);
    if (dlen) {
        fbuf_add_binary(&arg->data, data, dlen);
    } else if (!total_size) {
        // error notified (dlen == 0 && total_size == 0)
        arg->stat = -1;
        pthread_cond_signal(&arg->cond);
        pthread_mutex_unlock(&arg->lock);
        return -1;
    }

    if (total_size) {
        arg->complete = 1;
        if (timestamp)
            memcpy(&arg->ts, timestamp, sizeof(struct timeval));
        if (total_size != fbuf_used(&arg->data)) {
            arg->stat = -1;
        }
        pthread_cond_signal(&arg->cond);
    }
    pthread_mutex_unlock(&arg->lock);
    return 0;
}

void *shardcache_get(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     size_t *vlen,
                     struct timeval *timestamp)
{
    if (!key)
        return NULL;

    shardcache_get_helper_arg_t arg = {

        .lock = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .data = FBUF_STATIC_INITIALIZER,
        .stat = 0,
        .ts = { 0, 0 },
        .complete = 0
    };

    char keystr[1024];
    if (shardcache_log_level() >= LOG_DEBUG)
        KEY2STR(key, klen, keystr, sizeof(keystr));

    SHC_DEBUG("Getting value for key: %s", keystr);

    int rc = shardcache_get_async(cache, key, klen, shardcache_get_helper, &arg);

    if (rc == 0) {
        pthread_mutex_lock(&arg.lock);
        if (!arg.complete)
            pthread_cond_wait(&arg.cond, &arg.lock);
        pthread_mutex_unlock(&arg.lock);

        if (arg.stat != 0) {
            fbuf_destroy(&arg.data);
            SHC_DEBUG("Error trying to get key: %s", keystr);
            return NULL;
        }

        char *value = fbuf_data(&arg.data);
        if (vlen)
            *vlen = fbuf_used(&arg.data);

        if (timestamp)
            memcpy(timestamp, &arg.ts, sizeof(struct timeval));

        if (!value)
            SHC_DEBUG("No value for key: %s", keystr);

        return value;
    }
    fbuf_destroy(&arg.data);
    return NULL;
}

size_t
shardcache_head(shardcache_t *cache,
                void *key,
                size_t len,
                void *head,
                size_t hlen,
                struct timeval *timestamp)
{
    if (!key)
        return 0;

    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_HEADS].value, 1);

    size_t rlen = hlen;
    size_t remainder =  shardcache_get_offset(cache, key, len, head, &rlen, 0, timestamp);
    return remainder + rlen;
}

int
shardcache_exists(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        if (!ht_exists(cache->volatile_storage, key, klen)) {
            if (cache->use_persistent_storage && cache->storage.exist) {
                if (!cache->storage.exist(key, klen, cache->storage.priv))
                    return 0;
            } else {
                return 0;
            }
        }
        return 1;
    } else {
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        int fd = shardcache_get_connection_for_peer(cache, peer);
        int rc = exists_on_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, peer, fd);
        return rc;
    }

    return -1;
}

int
shardcache_touch(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        cache_object_t *obj = NULL;
        arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, (void **)&obj, 0);
        if (res) {
            pthread_mutex_lock(&obj->lock);
            gettimeofday(&obj->ts, NULL);
            pthread_mutex_unlock(&obj->lock);
            arc_release_resource(cache->arc, res);
            return obj ? 0 : -1;
        }
    } else {
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        int fd = shardcache_get_connection_for_peer(cache, peer);
        int rc = touch_on_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, peer, fd);
        return rc;
    }

    return -1;
}


static void
_shardcache_commence_eviction(shardcache_t *cache, void *key, size_t klen)
{
    shardcache_evictor_job_t *job = create_evictor_job(key, klen);

    char keystr[1024];
    KEY2STR(key, klen, keystr, sizeof(keystr));
    SHC_INFO("Adding evictor job for key %s", keystr);

    push_value(cache->evictor_jobs, job);
    pthread_mutex_lock(&cache->evictor_lock);
    pthread_cond_signal(&cache->evictor_cond);
    pthread_mutex_unlock(&cache->evictor_lock);
}


static int
_shardcache_set_internal(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         void *value,
                         size_t vlen,
                         time_t expire,
                         int inx)
{
    // if we are not the owner try propagating the command to the responsible peer
    
    if (!key || !value)
        return -1;

    char keystr[1024];
    KEY2STR(key, klen, keystr, sizeof(keystr));
    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_SETS].value, 1);

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        int rc = -1;
        SHC_DEBUG("Storing value %s (%d) for key %s",
                  shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE),
                  (int)vlen, keystr);

        volatile_object_t *prev = NULL;
        uint32_t *counter = &cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value;
        if (!cache->use_persistent_storage || expire)
        {
            // ensure removing this key from the persistent storage (if present)
            // since it's now going to be a volatile item
            if (cache->use_persistent_storage && cache->storage.remove)
                cache->storage.remove(key, klen, cache->storage.priv);

            if (inx && ht_exists(cache->volatile_storage, key, klen)) {
                SHC_DEBUG("A volatile value already exists for key %s", keystr);
                return 1;
            }

            volatile_object_t *obj = malloc(sizeof(volatile_object_t));
            obj->data = malloc(vlen);
            memcpy(obj->data, value, vlen);
            obj->dlen = vlen;
            obj->expire = expire ? time(NULL) + expire : 0;

            SHC_DEBUG("Setting volatile item %s to expire %d (now: %d)", 
                keystr, obj->expire, (int)time(NULL));

            if (inx) {
                rc = ht_set(cache->volatile_storage, key, klen,
                             obj, sizeof(volatile_object_t));
            } else {
                rc = ht_get_and_set(cache->volatile_storage, key, klen,
                                     obj, sizeof(volatile_object_t),
                                     (void **)&prev, NULL);
            }

            if (prev) {
                if (vlen > prev->dlen) {
                    __sync_add_and_fetch(counter, vlen - prev->dlen);
                } else {
                    __sync_sub_and_fetch(counter, prev->dlen - vlen);
                }
                destroy_volatile(prev); 
                arc_remove(cache->arc, (const void *)key, klen);
                _shardcache_commence_eviction(cache, key, klen);
            } else {
                __sync_add_and_fetch(counter, vlen);
            }

            SPIN_LOCK(&cache->next_expire_lock);
            if (obj->expire && (!cache->next_expire || obj->expire < cache->next_expire))
                cache->next_expire = obj->expire;

            SPIN_UNLOCK(&cache->next_expire_lock);

        }
        else if (cache->use_persistent_storage && cache->storage.store)
        {
            if (inx) {
                if (ht_exists(cache->volatile_storage, key, klen) == 1) {
                    SHC_DEBUG("A volatile value already exists for key %s", keystr);
                    return 1;
                }
            } else {
                // remove this key from the volatile storage (if present)
                // it's going to be eventually persistent now (depending on the storage type)
                ht_delete(cache->volatile_storage, key, klen, (void **)&prev, NULL);
            }

            if (prev) {
                __sync_sub_and_fetch(counter, prev->dlen);
                destroy_volatile(prev);
            }

            if (inx && cache->storage.exist &&
                    cache->storage.exist(key, klen, cache->storage.priv) == 1)
            {
                SHC_DEBUG("A value already exists for key %s", keystr);
                return 1;
            }

            rc = cache->storage.store(key, klen, value, vlen, cache->storage.priv);

            arc_remove(cache->arc, (const void *)key, klen);
            _shardcache_commence_eviction(cache, key, klen);
        }
        return rc;
    }
    else if (node_len)
    {
        SHC_DEBUG("Forwarding set command %s => %s (%d) to %s",
                keystr, shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE),
                (int)vlen, node_name);

        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }

        int fd = shardcache_get_connection_for_peer(cache, peer);

        int rc = -1;
        if (inx)
            rc = add_to_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, expire, fd);
        else
            rc = send_to_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, expire, fd);

        shardcache_release_connection_for_peer(cache, peer, fd);

        if (rc == 0) {
            arc_remove(cache->arc, (const void *)key, klen);

            _shardcache_commence_eviction(cache, key, klen);
        }
        return rc;
    }

    return -1;
}

int shardcache_set_volatile(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire)
{
    return _shardcache_set_internal(cache, key, klen, value, vlen, expire, 0);
}

int shardcache_add_volatile(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            void *value,
                            size_t vlen,
                            time_t expire)
{
    return _shardcache_set_internal(cache, key, klen, value, vlen, expire, 1);
}

int shardcache_set(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen)
{
    return _shardcache_set_internal(cache, key, klen, value, vlen, 0, 0);
}

int shardcache_add(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen)
{
    return _shardcache_set_internal(cache, key, klen, value, vlen, 0, 1);
}

int shardcache_del(shardcache_t *cache, void *key, size_t klen) {

    if (!key)
        return -1;

    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_DELS].value, 1);

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        volatile_object_t *prev_item = NULL;
        int rc = ht_delete(cache->volatile_storage, key, klen, (void **)&prev_item, NULL);

        if (rc != 0) {
            if (cache->use_persistent_storage && cache->storage.remove)
                cache->storage.remove(key, klen, cache->storage.priv);
        } else if (prev_item) {
            __sync_sub_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                                 prev_item->dlen);
            destroy_volatile(prev_item);
        }

        if (__sync_fetch_and_add(&cache->evict_on_delete, 0))
        {
            arc_remove(cache->arc, (const void *)key, klen);

            _shardcache_commence_eviction(cache, key, klen);
        }

        uint32_t size = (uint32_t)arc_size(cache->arc);
        uint32_t prev = __sync_fetch_and_add(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value, 0);
        __sync_bool_compare_and_swap(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
                                     prev,
                                     size);

        return 0;
    } else {
        char *peer = shardcache_get_node_address(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        int fd = shardcache_get_connection_for_peer(cache, peer);
        int rc = delete_from_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, peer, fd);
        return rc;
    }

    return -1;
}

int shardcache_evict(shardcache_t *cache, void *key, size_t klen) {
    if (!key || !klen)
        return -1;

    arc_remove(cache->arc, (const void *)key, klen);
    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_EVICTS].value, 1);

    uint32_t size = (uint32_t)arc_size(cache->arc);
    uint32_t *counter = &cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value;
    uint32_t prev = __sync_fetch_and_add(counter, 0);
    __sync_bool_compare_and_swap(counter, prev, size);

    return 0;
}

shardcache_node_t *shardcache_get_nodes(shardcache_t *cache, int *num_nodes) {
    int i;
    int num = 0;
    SPIN_LOCK(&cache->migration_lock);
    num = cache->num_shards;
    if (num_nodes)
        *num_nodes = num;
    shardcache_node_t *list = malloc(sizeof(shardcache_node_t) * num);
    for (i = 0; i < num; i++) {
        memcpy(&list[i], &cache->shards[i], sizeof(shardcache_node_t));
    }
    SPIN_UNLOCK(&cache->migration_lock);
    return list;
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
        reset_stat_figure(&cache->cnt[i].value);
    }
}

shardcache_storage_index_t *shardcache_get_index(shardcache_t *cache)
{
    shardcache_storage_index_t *index = NULL;

    if (cache->use_persistent_storage) {
        size_t isize = 65535;
        if (cache->storage.count)
            isize = cache->storage.count(cache->storage.priv);

        ssize_t count = 0;
        shardcache_storage_index_item_t *items = NULL;
        if (cache->storage.index) {
            items = calloc(sizeof(shardcache_storage_index_item_t), isize);
            count = cache->storage.index(items, isize, cache->storage.priv);
        }
        index = calloc(1, sizeof(shardcache_storage_index_t));
        index->items = items;
        index->size = count; 
    }
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

static int expire_migrated(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    shardcache_t *cache = (shardcache_t *)user;
    volatile_object_t *v = (volatile_object_t *)value;

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1) {
        fprintf(stderr, "expire_migrated running while no migration continuum present ... aborting\n");
        return 0;
    } else if (!is_mine) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("Forcing Key %s to expire because not owned anymore", keystr);

        v->expire = 0;
    }
    return 1;
}


void *migrate(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;

    shardcache_storage_index_t *index = shardcache_get_index(cache);
    int aborted = 0;
    linked_list_t *to_delete = create_list();

    uint32_t migrated_items = 0;
    uint32_t scanned_items = 0;
    uint32_t errors = 0;
    uint32_t total_items = 0;

    if (index) {
        total_items = index->size;

        shardcache_counter_add(cache->counters, "migrated_items", &migrated_items);
        shardcache_counter_add(cache->counters, "scanned_items", &scanned_items);
        shardcache_counter_add(cache->counters, "total_items", &total_items);
        shardcache_counter_add(cache->counters, "migration_errors", &errors);

        SHC_INFO("Migrator starting (%d items to precess)", total_items);

        int i;
        for (i = 0; i < index->size; i++) {
            size_t klen = index->items[i].klen;
            void *key = index->items[i].key;

            char node_name[1024];
            size_t node_len = sizeof(node_name);
            memset(node_name, 0, node_len);

            char keystr[1024];
            KEY2STR(key, klen, keystr, sizeof(keystr));

            SHC_DEBUG("Migrator processign key %s", keystr);

            int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
            
            if (is_mine == -1) {
                fprintf(stderr, "Migrator running while no migration continuum present ... aborting\n");
                __sync_add_and_fetch(&errors, 1);
                aborted = 1;
                break;
            } else if (!is_mine) {
                // if we are not the owner try asking our peer responsible for this data
                void *value = NULL;
                size_t vlen = 0;
                if (cache->storage.fetch) {
                    value = cache->storage.fetch(key, klen, &vlen, cache->storage.priv);
                }
                if (value) {
                    char *peer = shardcache_get_node_address(cache, (char *)node_name);
                    if (peer) {
                        SHC_DEBUG("Migrator copying %s to peer %s (%s)", keystr, node_name, peer);
                        int fd = shardcache_get_connection_for_peer(cache, peer);
                        int rc = send_to_peer(peer, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, 0, fd);
                        shardcache_release_connection_for_peer(cache, peer, fd);
                        if (rc == 0) {
                            __sync_add_and_fetch(&migrated_items, 1);
                            push_value(to_delete, &index->items[i]);
                        } else {
                            fprintf(stderr, "Errors copying %s to peer %s (%s)\n", keystr, node_name, peer);
                            __sync_add_and_fetch(&errors, 1);
                        }
                    } else {
                        fprintf(stderr, "Can't find address for peer %s (me : %s)\n", node_name, cache->me);
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
    }

    if (!aborted) {
            SHC_INFO("Migration completed, now removing not-owned  items");
        shardcache_storage_index_item_t *item = shift_value(to_delete);
        while (item) {
            if (cache->storage.remove)
                cache->storage.remove(item->key, item->klen, cache->storage.priv);

            char ikeystr[1024];
            KEY2STR(item->key, item->klen, ikeystr, sizeof(ikeystr));
            SHC_DEBUG("removed item %s", ikeystr);

            item = shift_value(to_delete);
        }

        // and now let's expire all the volatile keys that don't belong to us anymore
        ht_foreach_pair(cache->volatile_storage, expire_migrated, cache);
        SPIN_LOCK(&cache->next_expire_lock);
        cache->next_expire = 0;
        SPIN_UNLOCK(&cache->next_expire_lock);

    }

    destroy_list(to_delete);

    SPIN_LOCK(&cache->migration_lock);
    cache->migration_done = 1;
    SPIN_UNLOCK(&cache->migration_lock);
    if (index) {
        SHC_INFO("Migrator ended: processed %d items, migrated %d, errors %d",
                total_items, migrated_items, errors);
    }

    if (index)
        shardcache_free_index(index);

    return NULL;
}

int shardcache_migration_begin(shardcache_t *cache,
                               shardcache_node_t *nodes,
                               int num_nodes,
                               int forward)
{
    int i, n;

    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        // already in a migration, ignore this command
        SPIN_UNLOCK(&cache->migration_lock);
        return -1;
    }

    SHC_NOTICE("Starting migration");

    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

    for (i = 0; i < num_nodes; i++) {
        shard_names[i] = nodes[i].label;
        shard_lens[i] = strlen(shard_names[i]);
        if (i > 0) 
            fbuf_add(&mgb_message, ",");
        fbuf_printf(&mgb_message, "%s:%s", nodes[i].label, nodes[i].address);
    }


    int ignore = 0;

    if (num_nodes == cache->num_shards) {
        // let's assume the lists are the same, if not
        // ignore will be set again to 0
        ignore = 1;
        for (i = 0 ; i < num_nodes; i++) {
            int found = 0;
            for (n = 0; n < num_nodes; n++) {
                if (strcmp(nodes[i].label, cache->shards[n].label) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                // the lists differ, we don't want to ignore the request
                ignore = 0;
                break;
            }
        }
    }

    if (!ignore) {
        cache->migration_done = 0;
        cache->migration_shards = malloc(sizeof(shardcache_node_t) * num_nodes);
        memcpy(cache->migration_shards, nodes, sizeof(shardcache_node_t) * num_nodes);
        cache->num_migration_shards = num_nodes;
        cache->migration = chash_create((const char **)shard_names,
                                        shard_lens,
                                        num_nodes,
                                        200);

        pthread_create(&cache->migrate_th, NULL, migrate, cache);
    }

    SPIN_UNLOCK(&cache->migration_lock);

    if (forward) {
        for (i = 0; i < cache->num_shards; i++) {
            if (strcmp(cache->shards[i].label, cache->me) != 0) {
                int fd = shardcache_get_connection_for_peer(cache, cache->shards[i].address);
                int rc = migrate_peer(cache->shards[i].address,
                                      (char *)cache->auth,
                                      SHC_HDR_SIGNATURE_SIP,
                                      fbuf_data(&mgb_message),
                                      fbuf_used(&mgb_message), fd);
                shardcache_release_connection_for_peer(cache, cache->shards[i].address, fd);
                if (rc != 0) {
                    fprintf(stderr, "Node %s (%s) didn't aknowledge the migration\n",
                            cache->shards[i].label, cache->shards[i].address);
                }
            }
        }
    }
    fbuf_destroy(&mgb_message);

    return 0;
}

int shardcache_migration_abort(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->migration);
        free(cache->migration_shards);
        SHC_NOTICE("Migration aborted");
        ret = 0;
    }
    cache->migration = NULL;
    cache->migration_shards = NULL;
    cache->num_migration_shards = 0;

    SPIN_UNLOCK(&cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int shardcache_migration_end(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->chash);
        free(cache->shards);
        cache->chash = cache->migration;
        cache->shards = cache->migration_shards;
        cache->num_shards = cache->num_migration_shards;
        cache->migration = NULL;
        cache->migration_shards = NULL;
        cache->num_migration_shards = 0;
        SHC_NOTICE("Migration ended");
        ret = 0;
    }
    cache->migration_done = 0;
    SPIN_UNLOCK(&cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int shardcache_use_persistent_connections(shardcache_t *cache, int new_value) {
    int old_value = 0;
    do {
        old_value = __sync_fetch_and_add(&cache->use_persistent_connections, 0);
    } while (!__sync_bool_compare_and_swap(&cache->use_persistent_connections, old_value, new_value));
    return old_value;
}

int shardcache_evict_on_delete(shardcache_t *cache, int new_value) {
    int old_value = 0;
    do {
        old_value = __sync_fetch_and_add(&cache->evict_on_delete, 0);
    } while (!__sync_bool_compare_and_swap(&cache->evict_on_delete, old_value, new_value));
    return old_value;
}

int shardcache_tcp_timeout(shardcache_t *cache, int new_value) {
    return connections_pool_tcp_timeout(cache->connections_pool, new_value);
}

