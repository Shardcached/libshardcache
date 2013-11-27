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

typedef struct chash_t chash_t;

struct __shardcache_s {
    char *me;

    shardcache_serving_t serv;

    char **shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

    chash_t *chash;

    shardcache_storage_t storage;

    int sock;
    pthread_t listener;

    int evict_on_delete;

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

    return obj;
}

static int __op_fetch(void *item, void * priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    if (obj->data) // the value is already loaded, we don't need to fetch
        return 0;

    const char *node_name;
    // if we are not the owner try asking our peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->len, &node_name)) {
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Fetching data for key %s from peer %s\n", (char *)obj->key, node_name); 
#endif
        // another peer is responsible for this item, let's get the value from there
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        int rc = fetch_from_peer((char *)node_name, (char *)cache->auth, obj->key, obj->len, &value);
        if (rc == 0 && fbuf_used(&value)) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            return 0;
        }
    }

    // we are responsible for this item ... so let's fetch it
    if (cache->storage.fetch) {
        void *v = cache->storage.fetch(obj->key, obj->len, &obj->dlen, cache->storage.priv);
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Fetch storage callback returned value %s (%lu) for key %s\n",
                v, (unsigned long)obj->dlen, (char *)obj->key); 
#endif
        if (v && obj->dlen) {
            obj->data = malloc(obj->dlen);
            memcpy(obj->data, v, obj->dlen);
        }
    }

    if (!obj->data) {
        return -1;
    }
    return 0;
}

static void __op_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;
    if (obj->data && cache->storage.free) {
        cache->storage.free(obj->data);
        obj->data = NULL;
        obj->dlen = 0;
    }
}

static void __op_destroy(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    if (obj->data && cache->storage.free) {
        cache->storage.free(obj->data);
    }
    free(obj->key);
    free(obj);
}

static void shardcache_do_nothing(int sig)
{
    // do_nothing
}

shardcache_t *shardcache_create(char *me, char **peers, int npeers,
                                shardcache_storage_t *st, char *secret)
{
    int i;
    size_t shard_lens[npeers + 1];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = 1;

    cache->me = strdup(me);

    if (st)
        memcpy(&cache->storage, st, sizeof(cache->storage));;

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

    // open the listening socket
    char *brkt = NULL;
    char *addr = strdup(cache->me); // we need a temporary copy to be used by strtok
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : SHARDCACHE_PORT_DEFAULT;

    cache->sock = open_socket(host, port);
    if (cache->sock == -1) {
        fprintf(stderr, "Can't open listening socket: %s\n",strerror(errno));
        shardcache_destroy(cache);
        free(addr);
        return NULL;
    }

    free(addr); // we don't need it anymore
    
    /*
    int rc = shardcache_compute_authkey(secret, cache->auth);
    if (rc != 0) {
        fprintf(stderr, "ERROR-- could not compute message digest\n");
        shardcache_destroy(cache);
        return NULL;
    } 
    */

    strncpy((char *)cache->auth, secret, sizeof(cache->auth));
    cache->serv.auth = cache->auth;
    cache->serv.me = cache->me;

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "AUTH KEY (secret: %s): ", secret);
    for (i = 0; i < shardCACHE_MSG_SIG_LEN; i++) {
        fprintf(stderr, "%02x", (unsigned char)cache->serv.auth[i]); 
    }
    fprintf(stderr, "\n");
#endif

    cache->serv.cache = cache;
    cache->serv.sock = cache->sock;

    // and start a background thread to handle incoming connections
    int rc = pthread_create(&cache->listener, NULL, accept_requests, &cache->serv);
    if (rc != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(errno));
        shardcache_destroy(cache);
        return NULL;
    }
    return cache;
}

void shardcache_destroy(shardcache_t *cache) {
    int i;
    pthread_cancel(cache->listener);
    pthread_join(cache->listener, NULL);
    arc_destroy(cache->arc);
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

    cache_object_t *obj = arc_lookup(cache->arc, (const void *)key, len);

    if (!obj)
        return NULL;

    if (vlen)
        *vlen = obj->dlen;

    return obj->data;
}

int shardcache_set(shardcache_t *cache, void *key, size_t klen, void *value, size_t vlen) {
    // if we are not the owner try propagating the command to the responsible peer
    
    if (!key || !value)
        return -1;

    arc_remove(cache->arc, (const void *)key, klen);

    const char *node_name;
    if (shardcache_test_ownership(cache, key, klen, &node_name)) {
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Storing value %s for key %s\n", (char *)value, (char *)key);
#endif
        if (cache->storage.store)
            cache->storage.store(key, klen, value, vlen, cache->storage.priv);
        return 0;
    } else {
#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Forwarding set command %s => %s to %s\n", (char *)key, (char *)value, node_name);
#endif
        return send_to_peer((char *)node_name, (char *)cache->auth, key, klen, value, vlen);
    }

    return -1;
}

int shardcache_del(shardcache_t *cache, void *key, size_t klen) {

    if (!key)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    const char *node_name;
    if (shardcache_test_ownership(cache, key, klen, &node_name))
    {
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

int shardcache_test_ownership(shardcache_t *cache, void *key, size_t len, const char **owner)
{
    const char *node_name = NULL;
    size_t name_len = 0;
    chash_lookup(cache->chash, key, len, &node_name, &name_len);
    if (owner)
        *owner = node_name;
    return (strcmp(node_name, cache->me) == 0);
}

uint64_t shardcache_compute_signature(char *secret, uint8_t *msg, size_t len) {
    return sip_hash24(secret, msg, len);    
}
