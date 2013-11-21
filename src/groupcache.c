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
#include <iomux.h>
#include <fcntl.h>

#include "groupcache.h"
#include "arc.h"
#include "connections.h"
#include <fbuf.h>

#include <chash.h>


#define GROUPCACHE_PORT_DEFAULT 9874

typedef enum {
    GROUPCACHE_CMD_GET          = 0x01,
    GROUPCACHE_CMD_GET_RESPONSE = 0x02,
    GROUPCACHE_CMD_SET          = 0x03,
    GROUPCACHE_CMD_SET_RESPONSE = 0x04
} groupcache_cmd_t;

typedef struct chash_t chash_t;

struct __groupcache_s {
    char *me;

    char **shards;
    int num_shards;

    arc_t *arc;
    arc_ops_t ops;

    chash_t *chash;

    groupcache_free_item_callback_t free_item_cb; 
    groupcache_fetch_item_callback_t fetch_item_cb; 
    groupcache_store_item_callback_t store_item_cb; 
    void *priv;

    int sock;
    pthread_t listener;
    int       num_workers;
};

/* This is the object we're managing. It has a key
 * * and some data. This data will be loaded when ARC instruct
 * * us to do so. */
typedef struct {
    void *key;
    size_t len;
    size_t dlen;
    void *data;
} cache_object_t;

int read_message(int fd, fbuf_t *out, groupcache_cmd_t *cmd) {
    uint16_t chunk_len;
    int reading_message = 0;
    for(;;) {
        int rb;

        if (reading_message == 0 && cmd) {
            rb = read_socket(fd, (char *)cmd, 1);
            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                return -1;
            }
            if (*cmd != GROUPCACHE_CMD_GET &&
                *cmd != GROUPCACHE_CMD_GET_RESPONSE &&
                *cmd != GROUPCACHE_CMD_SET&&
                *cmd != GROUPCACHE_CMD_SET_RESPONSE)
            {
                return -1;
            }
            reading_message = 1;
        }

        rb = read_socket(fd, (char *)&chunk_len, 2);
        if (rb == 2) {
            chunk_len = ntohs(chunk_len);
            if (chunk_len == 0) {
                return fbuf_used(out);
            }

            int initial_len = chunk_len;
            while (chunk_len != 0) {
                char buf[chunk_len];
                rb = read_socket(fd, buf, chunk_len);
                if (rb == -1) {
                    if (errno != EINTR && errno != EAGAIN) {
                        // ERROR 
                        fbuf_set_used(out, fbuf_used(out) - (initial_len - chunk_len));
                        return -1;
                    }
                } else if (rb == 0) {
                    fbuf_set_used(out, fbuf_used(out) - (initial_len - chunk_len));
                    return -1;
                }
                chunk_len -= rb;
                fbuf_add_binary(out, buf, rb);
            }
        } else if (rb == -1 && errno != EINTR && errno != EAGAIN) {
            // ERROR 
            break;
        } else if (rb == 0) {
            break;
        } 
    }
    return -1;
}

static int write_message(int fd, char cmd, void *v, size_t vlen)  {
    // TODO - split if vlen is bigger than MAXUINT16
    //        the protocol supports chunks of at most MAXUINT16 bytes
    //        so we need to provide multiple chunks in case the transmitted
    //        value is bigger and doesn't fit in one chunk

    int wb;

    if (cmd > 0) {
        wb = write_socket(fd, (char *)&cmd, 1);
        if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
            return -1;
        }
    }

    uint16_t size = htons(vlen);
    wb = write_socket(fd, (char *)&size, 2);
    if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
        return -1;
    } else if (wb == 2) {
        int wrote = 0;
        while (wrote != vlen) {
            wb = write_socket(fd, v, vlen);
            if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
                return -1;
            }
            wrote += wb;
        }
    }
    uint16_t terminator = 0;
    wb = write_socket(fd, (char *)&terminator, 2);
    if (wb == 2)
        return 0;
    return -1;
}

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

static int __send_to_peer(char *peer, void *key, size_t klen, void *value, size_t vlen) {
    char *brkt;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, GROUPCACHE_CMD_SET, key, klen);
        if (rc != 0) {
            close(fd);
            return -1;
        }

        groupcache_cmd_t cmd = 0;
        rc = write_message(fd, 0, value, vlen);

        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            int rb = read_message(fd, &resp, &cmd);
            if (cmd == GROUPCACHE_CMD_SET_RESPONSE && rb > 0) {
                printf("GOT: %s\n", fbuf_data(&resp));
                close(fd);
                fbuf_destroy(&resp);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}

static int __fetch_from_peer(char *peer, void *key, size_t len, fbuf_t *out) {
    char *brkt;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, GROUPCACHE_CMD_GET, key, len);
        if (rc == 0) {
            groupcache_cmd_t cmd = 0;
            int rb = read_message(fd, out, &cmd);
            if (cmd == GROUPCACHE_CMD_GET_RESPONSE && rb > 0) {
                printf("HEYHEY GOT : %s\n", fbuf_data(out));
                close(fd);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}

static int __op_fetch(void *item, void * priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    groupcache_t *cache = (groupcache_t *)priv;

    const char *node_name = NULL;
    size_t name_len = 0;
    chash_lookup(cache->chash, obj->key, obj->len, &node_name, &name_len);
    // if we are not the owner try asking our peer responsible for this data
    if (strcmp(node_name, cache->me) != 0) {
        // another peer is responsible for this item, let's get the value from there
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        int rc = __fetch_from_peer((char *)node_name, obj->key, obj->len, &value);
        if (rc == 0) {
            obj->data = fbuf_data(&value);
            obj->dlen = fbuf_used(&value);
            return 0;
        }
    }

    // we are responsible for this item ... so let's fetch it
    if (cache->fetch_item_cb)
        obj->data = cache->fetch_item_cb(obj->key, obj->len, &obj->dlen, cache->priv);
    if (!obj->data)
        return -1;
    return 0;
}

static void __op_evict(void *item, void *priv)
{
    cache_object_t *obj = (cache_object_t *)item;
    groupcache_t *cache = (groupcache_t *)priv;
    if (obj->data && cache->free_item_cb) {
        cache->free_item_cb(obj->data);
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
    groupcache_cmd_t cmd;
    void *key;
    int klen;
    void *value;
    int vlen;
} groupcache_connection_context;


static void *serve_request(void *priv) {
    groupcache_connection_context *ctx = (groupcache_connection_context *)priv;
    groupcache_t *cache = ctx->cache;
    int fd = ctx->fd;

    int opts = fcntl(fd, F_GETFL);
    if (opts >= 0) {
        int err = fcntl(fd, F_SETFL, opts & (~O_NONBLOCK));
        if (err != 0) {
            // TODO - Warning Messages
        }
    } else {
        // TODO - Warning Messages
    }

    switch(ctx->cmd) {
        case GROUPCACHE_CMD_GET:
        {
            size_t vlen = 0;
            void *v = groupcache_get(cache, ctx->key, ctx->klen, &vlen);
            if (v && vlen > 0) {
                write_message(fd, GROUPCACHE_CMD_GET_RESPONSE, v, vlen);
            }
            break;
        }
        case GROUPCACHE_CMD_SET:
        {
            if (cache->store_item_cb)
                cache->store_item_cb(ctx->key, ctx->klen, ctx->value, ctx->vlen, cache->priv);
            write_message(fd, GROUPCACHE_CMD_SET_RESPONSE, "OK", 2);
            break;
        }
        default:
            // TODO - Error Messages
            break;
    }

    close(fd);
    fbuf_destroy(ctx->input);
    fbuf_destroy(ctx->output);
    free(ctx);
    return NULL;
}

static void groupcache_input_handler(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    groupcache_connection_context *ctx = (groupcache_connection_context *)priv;
    if (!ctx)
        return;

    fbuf_add_binary(ctx->input, data, len);

    // the smallest possible valid (complete) request is 6 bytes
    if (fbuf_used(ctx->input) < 6)
        return;

    char *chunk = fbuf_data(ctx->input);
    char cmd = *chunk;
    chunk++;

    if (cmd != GROUPCACHE_CMD_GET &&
        cmd != GROUPCACHE_CMD_GET_RESPONSE &&
        cmd != GROUPCACHE_CMD_SET&&
        cmd != GROUPCACHE_CMD_SET_RESPONSE)
    {
        // BAD REQUEST
        fbuf_free(ctx->input);
        fbuf_free(ctx->output);
        free(ctx);
        iomux_close(iomux, fd);
        return;
    }

    ctx->cmd = cmd;

    int terminator_found = 0;
    int separator_found = 0;
    unsigned int ahead = 1; 

    if (ctx->key) {
        ahead += (2 + ctx->klen);
        chunk += 2 + ctx->klen;
    }
       
    while (!terminator_found) {
        uint16_t nlen;
        memcpy(&nlen, chunk, 2);
        uint16_t clen = ntohs(nlen);
        chunk += 2;
        ahead += 2;
        if (clen > 0) {
            ahead += clen;
            if (fbuf_used(ctx->input) >= ahead) {
                if (ctx->key) {
                    if (cmd == GROUPCACHE_CMD_SET) {
                        ctx->value = chunk;
                        ctx->vlen = clen;
                    } else {
                        // TODO - Error Messages
                    }
                } else {
                    ctx->key = chunk;
                    ctx->klen = clen;
                }
                chunk += clen;
            } else {
                break;
            }
        } else if (separator_found ||
                   (cmd != GROUPCACHE_CMD_SET && ctx->key))
        {
            terminator_found = 1;
        } else {
            separator_found = 1;
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
    //DEBUG1("Connection to %d closed", fd);
    close(fd);
}

static void groupcache_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    groupcache_t *cache = (groupcache_t *)priv;

    // create and initialize the context for the new connection
    groupcache_connection_context *context = calloc(1, sizeof(groupcache_connection_context));

    context->input = fbuf_create(0);
    context->output = fbuf_create(0);
    context->cache = cache;

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = NULL,
        .mux_input = groupcache_input_handler,
        .mux_eof = groupcache_eof_handler,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = context
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
        .mux_eof = groupcache_eof_handler,
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

groupcache_t *groupcache_create(char *me,
                        char **peers,
                        int npeers,
                        groupcache_fetch_item_callback_t fetch_cb, 
                        groupcache_store_item_callback_t store_cb, 
                        groupcache_free_item_callback_t free_cb,
                        void *priv)
{
    int i;
    size_t shard_lens[npeers + 1];

    groupcache_t *cache = calloc(1, sizeof(groupcache_t));
    cache->me = strdup(me);

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
    cache->free_item_cb = free_cb;
    cache->fetch_item_cb = fetch_cb;
    cache->store_item_cb = store_cb;
    cache->priv = priv;


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
    const char *node_name = NULL;
    size_t name_len = 0;
    chash_lookup(cache->chash, key, klen, &node_name, &name_len);
    // if we are not the owner try asking our peer responsible for this data
    if (strcmp(node_name, cache->me) == 0) {
        if (cache->store_item_cb)
            cache->store_item_cb(key, klen, value, vlen, cache->priv);
    } else {
        __send_to_peer((char *)node_name, key, klen, value, vlen);
    }

    return 0;
}

char **groupcache_get_peers(groupcache_t *cache, int *num_peers) {
    if (num_peers)
        *num_peers = cache->num_shards;
    return cache->shards;
}
