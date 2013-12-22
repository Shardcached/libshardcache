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
#include <hashtable.h>

#include "shardcache.h"
#include "arc.h"
#include "connections.h"
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
#define SHARDCACHE_COUNTER_TABLE_SIZE   6
#define SHARDCACHE_COUNTER_CACHE_SIZE   7
#define SHARDCACHE_NUM_COUNTERS 8
struct __shardcache_s {
    char *me;

    shardcache_node_t *shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

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

    int evict_on_delete;

    shardcache_serving_t *serv;

    const char *auth;

    pthread_t migrate_th;

    pthread_t evictor_th;
    pthread_mutex_t evictor_lock;
    pthread_cond_t evictor_cond;
    linked_list_t *evictor_jobs;

    shardcache_counters_t *counters;
    struct {
        const char *name;
        uint32_t value;
    } cnt[SHARDCACHE_NUM_COUNTERS];
};

/* This is the object we're managing. It has a key
 * and some data. This data will be loaded when ARC instruct
 * us to do so. */
typedef struct {
    void *key;
    size_t klen;
    void *data;
    size_t dlen;
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

static void *__op_create(const void *key, size_t len, void *priv)
{
    cache_object_t *obj = calloc(1, sizeof(cache_object_t));

    obj->klen = len;
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

static int __op_fetch_from_peer(shardcache_t *cache, cache_object_t *obj, char *peer)
{

#ifdef SHARDCACHE_DEBUG
    char keystr[1024];
    memcpy(keystr, obj->key, obj->klen < 1024 ? obj->klen : 1024);
    keystr[obj->klen] = 0;
    fprintf(stderr, "Fetching data for key %s from peer %s\n", keystr, peer); 
#endif

    char *peer_addr = shardcache_get_node_address(cache, peer);
    if (!peer_addr) {
        // TODO - Error Messages
        return -1;
    }

    // another peer is responsible for this item, let's get the value from there
    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = fetch_from_peer(peer_addr, (char *)cache->auth, obj->key, obj->klen, &value, -1);
    if (rc == 0 && fbuf_used(&value)) {
        obj->data = fbuf_data(&value);
        obj->dlen = fbuf_used(&value);
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
        return 0;
    } else {
        fbuf_destroy(&value);
        return -1;
    }
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
            pthread_mutex_unlock(&obj->lock);
            return obj->dlen;
        }
    }

#ifdef SHARDCACHE_DEBUG
    char keystr[1024];
    memcpy(keystr, obj->key, obj->klen < 1024 ? obj->klen : 1024);
    keystr[obj->klen] = 0;
#endif

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
    } else if (cache->use_persistent_storage && cache->storage.fetch) {
        obj->data = cache->storage.fetch(obj->key, obj->klen, &obj->dlen, cache->storage.priv);

#ifdef SHARDCACHE_DEBUG
        if (obj->data && obj->dlen) {
            fprintf(stderr, "Fetch storage callback returned value ");
            
            HEXDUMP_DATA(obj->data, obj->dlen);

            fprintf(stderr, " (%lu) for key %s\n", (unsigned long)obj->dlen, keystr); 
        } else {
            fprintf(stderr, "Fetch storage callback returned an empty value for key %s\n", keystr);
        }
#endif
    }

    if (!obj->data) {
        pthread_mutex_unlock(&obj->lock);
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Item not found for key %s\n", keystr);
#endif
        __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_NOT_FOUND].value, 1);
        return 0;
    }
    pthread_mutex_unlock(&obj->lock);
    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value, 1);
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
                    delete_from_peer(cache->shards[i].address, (char *)cache->auth, job->key, job->klen, 0, -1);
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
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;
        fprintf(stderr, "Key %s expired\n", keystr);
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
    for (;;) {
        time_t now = time(NULL);

        pthread_testcancel();

        SPIN_LOCK(&cache->next_expire_lock);

        if (cache->next_expire && now >= cache->next_expire && ht_count(cache->volatile_storage)) {
            int prev_expire = cache->next_expire;
            SPIN_UNLOCK(&cache->next_expire_lock);

            pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

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

            pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        } else {
            SPIN_UNLOCK(&cache->next_expire_lock);
        }

        pthread_testcancel();

        sleep(1);
    }
    return NULL;
}

shardcache_t *shardcache_create(char *me,
                                shardcache_node_t *nodes,
                                int nnodes,
                                shardcache_storage_t *st,
                                char *secret,
                                int num_workers,
                                size_t cache_size,
                                int evict_on_delete)
{
    int i;
    size_t shard_lens[nnodes];
    char *shard_names[nnodes];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = evict_on_delete;


#ifndef __MACH__
    pthread_spin_init(&cache->migration_lock, 0);
#endif

    if (st) {
        memcpy(&cache->storage, st, sizeof(cache->storage));
        cache->use_persistent_storage = 1;
    } else {
        fprintf(stderr, "No storage callbacks provided, "
                        "using only the internal volatile storage\n");
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

    if (secret && strlen(secret)) {
        cache->auth = calloc(1, 16);
        strncpy((char *)cache->auth, secret, 16);
    } 

#ifdef SHARDCACHE_DEBUG
    if (secret && strlen(secret)) {
        fprintf(stderr, "AUTH KEY (secret: %s): ", secret);
        for (i = 0; i < SHARDCACHE_MSG_SIG_LEN; i++) {
            fprintf(stderr, "%02x", (unsigned char)cache->auth[i]); 
        }
        fprintf(stderr, "\n");
    }
#endif

    const char *counters_names[SHARDCACHE_NUM_COUNTERS] =
        { "gets", "sets", "dels", "evicts", "cache_misses", "not_found", "volatile_table_size", "cache_size" };

    cache->counters = shardcache_init_counters();

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        cache->cnt[i].name = counters_names[i];
        shardcache_counter_add(cache->counters, cache->cnt[i].name, &cache->cnt[i].value); 
    }

    if (cache->evict_on_delete) {
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

    cache->volatile_storage = ht_create(1<<20, 10<<20, (ht_free_item_callback_t)destroy_volatile);

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

    return cache;
}

void shardcache_destroy(shardcache_t *cache)
{
    int i;

    if (cache->serv)
        stop_serving(cache->serv);

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

    if (cache->expirer_started) {
        pthread_cancel(cache->expirer_th);
        pthread_join(cache->expirer_th, NULL);
    }

    ht_destroy(cache->volatile_storage);
#ifndef __MACH__
    pthread_spin_destroy(&cache->next_expire_lock);
#endif

    if (cache->auth)
        free((void *)cache->auth);

    free(cache);
}

void *shardcache_get(shardcache_t *cache, void *key, size_t len, size_t *vlen) {
    if (!key)
        return NULL;

    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_GETS].value, 1);

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
    uint32_t size = (uint32_t)arc_size(cache->arc);
    uint32_t prev = __sync_fetch_and_add(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value, 0);
    __sync_bool_compare_and_swap(&cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
                                 prev,
                                 size);
    return value;
}

static int
_shardcache_set_internal(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         void *value,
                         size_t vlen,
                         time_t expire)
{
    // if we are not the owner try propagating the command to the responsible peer
    
    if (!key || !value)
        return -1;

    __sync_add_and_fetch(&cache->cnt[SHARDCACHE_COUNTER_SETS].value, 1);

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1) {

#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;

        fprintf(stderr, "Storing value ");

        HEXDUMP_DATA(value, vlen);

        fprintf(stderr, " (%d) for key %s\n", (int)vlen, keystr);
#endif
        volatile_object_t *prev = NULL;
        uint32_t *counter = &cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value;
        if (!cache->use_persistent_storage || expire) {
            // ensure removing this key from the persistent storage (if present)
            // since it's now going to be a volatile item
            if (cache->use_persistent_storage && cache->storage.remove)
                cache->storage.remove(key, klen, cache->storage.priv);
            volatile_object_t *obj = malloc(sizeof(volatile_object_t));
            obj->data = malloc(vlen);
            memcpy(obj->data, value, vlen);
            obj->dlen = vlen;
            obj->expire = expire ? time(NULL) + expire : 0;
#ifdef SHARDCACHE_DEBUG
            fprintf(stderr, "Setting volatile item %s to expire %d (now: %d)\n", 
                keystr, obj->expire, (int)time(NULL));
#endif
            ht_get_and_set(cache->volatile_storage, key, klen,
                obj, sizeof(volatile_object_t), (void **)&prev, NULL);

            if (prev) {
                if (vlen > prev->dlen) {
                    __sync_add_and_fetch(counter, vlen - prev->dlen);
                } else {
                    __sync_sub_and_fetch(counter, prev->dlen - vlen);
                }
                destroy_volatile(prev); 
            } else {
                __sync_add_and_fetch(counter, vlen);
            }

            SPIN_LOCK(&cache->next_expire_lock);
            if (obj->expire && obj->expire < cache->next_expire)
                cache->next_expire = obj->expire;

            SPIN_UNLOCK(&cache->next_expire_lock);

        } else if (cache->use_persistent_storage && cache->storage.store) {
            // remove this key from the volatile storage (if present)
            // it's going to be eventually persistent now (depending on the storage type)
            ht_delete(cache->volatile_storage, key, klen, (void **)&prev, NULL);
            if (prev) {
                __sync_sub_and_fetch(counter, prev->dlen);
                destroy_volatile(prev);
            }
            cache->storage.store(key, klen, value, vlen, cache->storage.priv);
        }
        arc_remove(cache->arc, (const void *)key, klen);
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
        int rc = send_to_peer(peer, (char *)cache->auth, key, klen, value, vlen, expire, -1);
        if (rc == 0)
            arc_remove(cache->arc, (const void *)key, klen);
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
    return _shardcache_set_internal(cache, key, klen, value, vlen, expire);
}

int shardcache_set(shardcache_t *cache,
                   void *key,
                   size_t klen,
                   void *value,
                   size_t vlen)
{
    return _shardcache_set_internal(cache, key, klen, value, vlen, 0);
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

        if (cache->evict_on_delete)
        {
            arc_remove(cache->arc, (const void *)key, klen);
            shardcache_evictor_job_t *job = create_evictor_job(key, klen);
            push_value(cache->evictor_jobs, job);
            pthread_mutex_lock(&cache->evictor_lock);
            pthread_cond_signal(&cache->evictor_cond);
            pthread_mutex_unlock(&cache->evictor_lock);
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
            // TODO - Error Messages
            return -1;
        }
        return delete_from_peer(peer, (char *)cache->auth, key, klen, 1, -1);
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
#ifdef SHARDCACHE_DEBUG
        char keystr[1024];
        memcpy(keystr, key, klen < 1024 ? klen : 1024);
        keystr[klen] = 0;
        fprintf(stderr, "Forcing Key %s to expire because not owned anymore\n", keystr);
#endif
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

    if (index) {
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
#ifdef SHARDCACHE_DEBUG
                        fprintf(stderr, "Migrator copying %s to peer %s (%s)\n", keystr, node_name, peer);
#endif
                        int rc = send_to_peer(peer, (char *)cache->auth, key, klen, value, vlen, 0, -1);
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
#ifdef SHARDCACHE_DEBUG
    if (index) {
        fprintf(stderr, "Migrator ended: processed %d items, migrated %d, errors %d\n",
                total_items, migrated_items, errors);
    }
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

    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        // already in a migration, ignore this command
        SPIN_UNLOCK(&cache->migration_lock);
        return -1;
    }

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Starting migration\n");
#endif

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
                int rc = migrate_peer(cache->shards[i].address,
                                      (char *)cache->auth,
                                      fbuf_data(&mgb_message),
                                      fbuf_used(&mgb_message), -1);
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
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Migration aborted\n");
#endif
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
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Migration ended\n");
#endif
        ret = 0;
    }
    cache->migration_done = 0;
    SPIN_UNLOCK(&cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}


