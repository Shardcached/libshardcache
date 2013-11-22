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
#include <fcntl.h>
#include <iomux.h>
#include <fbuf.h>

#include "groupcache.h"
#include "arc.h"
#include "connections.h"
#include "messaging.h"

#include <chash.h>


typedef struct chash_t chash_t;

struct __groupcache_s {
    char *me;

    char **shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

    chash_t *chash;

    groupcache_storage_t storage;

    void *priv;

    int sock;
    pthread_t listener;
    int num_workers;
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
    groupcache_t *cache = (groupcache_t *)priv;

    const char *node_name;
    // if we are not the owner try asking our peer responsible for this data
    if (!groupcache_test_ownership(cache, obj->key, obj->len, &node_name)) {
        // another peer is responsible for this item, let's get the value from there
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        int rc = fetch_from_peer((char *)node_name, obj->key, obj->len, &value);
        if (rc == 0) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            return 0;
        }
    }

    // we are responsible for this item ... so let's fetch it
    if (cache->storage.fetch)
        obj->data = cache->storage.fetch(obj->key, obj->len, &obj->dlen, cache->priv);

    if (!obj->data)
        return -1;
    return 0;
}

static void __op_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    groupcache_t *cache = (groupcache_t *)priv;
    if (obj->data && cache->storage.free) {
        cache->storage.free(obj->data);
        obj->data = NULL;
        obj->dlen = 0;
    }
}

static void __op_destroy(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    free(obj->key);
    free(obj);
}

typedef struct {
    fbuf_t *input;
    fbuf_t *output;
    int fd;
    groupcache_t *cache;
    groupcache_hdr_t hdr;
    fbuf_t *key;
    fbuf_t *value;
    int    key_found;
    int    value_found;
} groupcache_worker_context_t;

static groupcache_worker_context_t *groupcache_create_connection_context(groupcache_t *cache) {
    groupcache_worker_context_t *context = calloc(1, sizeof(groupcache_worker_context_t));

    context->input = fbuf_create(0);
    context->output = fbuf_create(0);
    context->key = fbuf_create(0);
    context->value = fbuf_create(0);
    context->cache = cache;
    return context;
}

static void groupcache_destroy_connection_context(groupcache_worker_context_t *ctx) {
    fbuf_free(ctx->input);
    fbuf_free(ctx->output);
    fbuf_free(ctx->key);
    fbuf_free(ctx->value);
    free(ctx);
}

static void *serve_request(void *priv) {
    groupcache_worker_context_t *ctx = (groupcache_worker_context_t *)priv;
    groupcache_t *cache = ctx->cache;
    int fd = ctx->fd;

    int opts = fcntl(fd, F_GETFL);
    if (opts >= 0) {
        int err = fcntl(fd, F_SETFL, opts & (~O_NONBLOCK));
        if (err != 0) {
            fprintf(stderr, "Can't set fd %d to non blocking mode: %s\n", fd, strerror(errno));
        }
    } else {
        fprintf(stderr, "Can't get flags for fd %d: %s\n", fd, strerror(errno));
    }

    int rc = 0;
    switch(ctx->hdr) {
        case GROUPCACHE_HDR_GET:
        {
            size_t vlen = 0;
            void *v = groupcache_get(cache, fbuf_data(ctx->key), fbuf_used(ctx->key), &vlen);
            write_message(fd, GROUPCACHE_HDR_RES, v, vlen);
            break;
        }
        case GROUPCACHE_HDR_SET:
        {
            /*
            if (cache->storage.store)
                cache->storage.store(fbuf_data(ctx->key), fbuf_used(ctx->key),
                        fbuf_data(ctx->value), fbuf_used(ctx->value), cache->priv);
            */
            rc = groupcache_set(cache, fbuf_data(ctx->key), fbuf_used(ctx->key),
                    fbuf_data(ctx->value), fbuf_used(ctx->value));
            break;
        }
        case GROUPCACHE_HDR_DEL:
        {
            rc = groupcache_del(cache, fbuf_data(ctx->key), fbuf_used(ctx->key));
            break;
        }
        default:
            fprintf(stderr, "Unknown command: 0x%02x\n", (char)ctx->hdr);
            break;
    }

    if (rc != 0) {
        fprintf(stderr, "groupcache: Error running command %d (key %s)\n", ctx->hdr, fbuf_data(ctx->key));
        write_message(fd, GROUPCACHE_HDR_RES, "ERR", 3);
    } else {
        write_message(fd, GROUPCACHE_HDR_RES, "OK", 2);
    }

    close(fd);
    groupcache_destroy_connection_context(ctx);
    return NULL;
}

static void groupcache_input_handler(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    groupcache_worker_context_t *ctx = (groupcache_worker_context_t *)priv;
    if (!ctx)
        return;

    fbuf_add_binary(ctx->input, data, len);

    // the smallest possible valid (complete) request is 6 bytes
    if (fbuf_used(ctx->input) < 6)
        return;

    char *chunk = fbuf_data(ctx->input);
    char hdr = *chunk;
    chunk++;

    if (hdr != GROUPCACHE_HDR_GET &&
        hdr != GROUPCACHE_HDR_SET&&
        hdr != GROUPCACHE_HDR_DEL &&
        hdr != GROUPCACHE_HDR_RES)
    {
        // BAD REQUEST
        iomux_close(iomux, fd);
        return;
    }

    ctx->hdr = hdr;

    int terminator_found = 0;
    int separator_found = 0;
    unsigned int ahead = 1; 

    int klen = fbuf_used(ctx->key);
    int vlen = fbuf_used(ctx->value);
       
    while (!terminator_found) {
        uint16_t nlen;
        memcpy(&nlen, chunk, 2);
        uint16_t clen = ntohs(nlen);
        chunk += 2;
        ahead += 2;
        if (clen > 0) {
            ahead += clen;
            if (fbuf_used(ctx->input) >= ahead) {
                if (!ctx->key_found) {
                    if (klen > 0)
                        klen -= clen;
                    else
                        fbuf_add_binary(ctx->key, chunk, clen);
                } else if (!ctx->value_found) {
                    if (vlen > 0)
                        vlen -= clen;
                    else
                        fbuf_add_binary(ctx->value, chunk, clen);
                } else {
                    // Bogus request, ignore
                }
                chunk += clen;
            } else {
                break;
            }
        } else if (separator_found) {
            if (hdr == GROUPCACHE_HDR_SET) {
                ctx->value_found = 1;
            } else {
                // Bogus request, ignore
            }
            terminator_found = 1;
        } else {
            ctx->key_found = 1;
            separator_found = 1;
            if (hdr != GROUPCACHE_HDR_SET)
                terminator_found = 1;
        }
    }

    if (terminator_found) {
        // we have a complete request so we can now start 
        // background worker to handle it
        pthread_t worker_thread;
        ctx->fd = fd;
        // let the worker take care of the fd from now on
        iomux_remove(iomux, fd);
        pthread_create(&worker_thread, NULL, serve_request, ctx);
        pthread_detach(worker_thread);
    }
}

static void groupcache_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    groupcache_worker_context_t *ctx = (groupcache_worker_context_t *)priv;
    close(fd);
    if (ctx) {
        groupcache_destroy_connection_context(ctx);
    }
    //DEBUG1("Connection to %d closed", fd);
}

static void groupcache_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    groupcache_t *cache = (groupcache_t *)priv;

    // create and initialize the context for the new connection
    groupcache_worker_context_t *ctx = groupcache_create_connection_context(cache);

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = NULL,
        .mux_input = groupcache_input_handler,
        .mux_eof = groupcache_eof_handler,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = ctx
    };

    // and wait for input data
    iomux_add(iomux, fd, &connection_callbacks);
}


void *accept_requests(void *priv) {
    groupcache_t *cache = (groupcache_t *)priv;

    char *brkt;
    char *addr = strdup(cache->me);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;

    iomux_callbacks_t groupcache_callbacks = {
        .mux_connection = groupcache_connection_handler,
        .mux_input = NULL,
        .mux_eof = NULL,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = cache
    };

    iomux_t *iomux = iomux_create();
    cache->sock = open_socket(host, port);
    if (cache->sock == -1) {
        fprintf(stderr, "Can't open listening socket: %s\n",strerror(errno));
        groupcache_destroy(cache);
        return NULL;
    }
    iomux_add(iomux, cache->sock, &groupcache_callbacks);
    iomux_listen(iomux, cache->sock);

    iomux_loop(iomux, 0);
    
    iomux_destroy(iomux);
    return NULL;
}

groupcache_t *groupcache_create(char *me, char **peers, int npeers, groupcache_storage_t *st)
{
    int i;
    size_t shard_lens[npeers + 1];

    groupcache_t *cache = calloc(1, sizeof(groupcache_t));
    cache->me = strdup(me);

    if (st)
        memcpy(&cache->storage, st, sizeof(cache->storage));;

    cache->ops.create  = __op_create;
    cache->ops.fetch   = __op_fetch;
    cache->ops.evict   = __op_evict;
    cache->ops.destroy = __op_destroy;

    cache->ops.priv = cache;
    cache->shards = malloc(sizeof(char *) * (npeers + 1));
    memcpy(cache->shards, peers, sizeof(char *) * npeers);
    cache->shards[npeers] = me;
    for (i = 0; i < npeers; i++) {
        shard_lens[i] = strlen(cache->shards[i]);
    }
    shard_lens[npeers] = strlen(me);

    cache->num_shards = npeers + 1;
    cache->chash = chash_create((const char **)cache->shards, shard_lens, cache->num_shards, 200);

    cache->arc = arc_create(&cache->ops, 300);

    int rc = pthread_create(&cache->listener, NULL, accept_requests, cache);
    if (rc != 0) {
        groupcache_destroy(cache);
        return NULL;
    }
    return cache;
}

void groupcache_destroy(groupcache_t *cache) {
    pthread_cancel(cache->listener);
    pthread_join(cache->listener, NULL);
    arc_destroy(cache->arc);
    chash_free(cache->chash);
    free(cache->me);
    free(cache);
}

void *groupcache_get(groupcache_t *cache, void *key, size_t len, size_t *vlen) {
    cache_object_t *obj = arc_lookup(cache->arc, (const void *)key, len);

    if (!obj)
        return NULL;

    if (vlen)
        *vlen = obj->dlen;

    return obj->data;
}

int groupcache_set(groupcache_t *cache, void *key, size_t klen, void *value, size_t vlen) {
    // if we are not the owner try propagating the command to the responsible peer
    
    const char *node_name;
    if (groupcache_test_ownership(cache, key, klen, &node_name)) {
        if (cache->storage.store)
            cache->storage.store(key, klen, value, vlen, cache->priv);
        return 0;
    } else {
        return send_to_peer((char *)node_name, key, klen, value, vlen);
    }

    return -1;
}

int groupcache_del(groupcache_t *cache, void *key, size_t klen) {
    // if we are not the owner try propagating the command to the responsible peer
    const char *node_name;
    arc_remove(cache->arc, (const void *)key, klen);
    if (groupcache_test_ownership(cache, key, klen, &node_name)) {
        if (cache->storage.remove)
            cache->storage.remove(key, klen, cache->priv);
        return 0;
    } else {
        return delete_from_peer((char *)node_name, key, klen);
    }

    return -1;
}

char **groupcache_get_peers(groupcache_t *cache, int *num_peers) {
    if (num_peers)
        *num_peers = cache->num_shards;
    return cache->shards;
}

int groupcache_test_ownership(groupcache_t *cache, void *key, size_t len, const char **owner)
{
    const char *node_name = NULL;
    size_t name_len = 0;
    chash_lookup(cache->chash, key, len, &node_name, &name_len);
    if (owner)
        *owner = node_name;
    return (strcmp(node_name, cache->me) == 0);
}

