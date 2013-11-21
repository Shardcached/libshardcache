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

#include "groupcache.h"
#include "arc.h"
#include "connections.h"
#include "fbuf.h"
#include "log.h"

#include <chash.h>


#define GROUPCACHE_PORT_DEFAULT 9874

typedef enum {
    GROUPCACHE_CMD_GET = 0x01,
    GROUPCACHE_CMD_GET_RESPONSE = 0x02,
    GROUPCACHE_CMD_SET = 0x03,
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
    pthread_t *workers;
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

            if (cmd)
                DEBUG2("Received new message type %d , len %d", *cmd, chunk_len);

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

    int wb = write_socket(fd, (char *)&cmd, 1);
    if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
        return -1;
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


static void serve_request(groupcache_t *cache, int fd) {
    fbuf_t buf = FBUF_STATIC_INITIALIZER; 
    groupcache_cmd_t cmd = 0;
    int rb = read_message(fd, &buf, &cmd);
    while (cmd > 0 && rb > 0) {
        switch(cmd) {
            case GROUPCACHE_CMD_GET:
                if (fbuf_used(&buf)) {
                    size_t vlen = 0;
                    void *v = groupcache_get(cache, fbuf_data(&buf), fbuf_used(&buf), &vlen);
                    if (v && vlen > 0) {
                        write_message(fd, GROUPCACHE_CMD_GET_RESPONSE, v, vlen);
                    }
                }
                break;
            case GROUPCACHE_CMD_SET:
                {
                    int klen = fbuf_used(&buf);
                    void *key = malloc(klen);
                    memcpy(key, fbuf_data(&buf), klen);
                    fbuf_clear(&buf);
                    rb = read_message(fd, &buf, NULL);
                    if (rb > 0) {
                        if (cache->store_item_cb)
                            cache->store_item_cb(key, klen, fbuf_data(&buf), fbuf_used(&buf), cache->priv);
                        write_message(fd, GROUPCACHE_CMD_SET_RESPONSE, "OK", 2);
                    }
                    free(key);
                }
                break;
            default:
                close(fd);
                fbuf_destroy(&buf);
                return;
        }
        fbuf_clear(&buf);
        rb = read_message(fd, &buf, &cmd);
    }
    close(fd);
    fbuf_destroy(&buf);
}

pthread_mutex_t accept_lock = PTHREAD_MUTEX_INITIALIZER;

void *accept_requests(void *priv) {
    groupcache_t *cache = (groupcache_t *)priv;
    for(;;) {
        struct sockaddr peer_addr;
        socklen_t addr_len;
        pthread_mutex_lock(&accept_lock);
        int fd = accept(cache->sock, &peer_addr, &addr_len);
        pthread_mutex_unlock(&accept_lock);
        if (fd >= 0) {
            serve_request(cache, fd);
        }
    }
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
    memcpy(cache->shards, peers, npeers);
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

    char *brkt;
    char *addr = strdup(me);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    cache->sock = open_socket(host, port);
    if (cache->sock == -1) {
        fprintf(stderr, "Can't open listening socket: %s\n",strerror(errno));
        groupcache_destroy(cache);
        return NULL;
    }
    cache->num_workers = 10;
    cache->workers = calloc(sizeof(pthread_t), cache->num_workers);
    for (i = 0; i < cache->num_workers; i++) {
        pthread_create(&cache->workers[i], NULL, accept_requests, cache);
    }
    return cache;
}

void groupcache_destroy(groupcache_t *cache) {
    int i;
    for (i = 0; i < cache->num_workers; i++) {
        pthread_cancel(cache->workers[i]);
        pthread_join(cache->workers[i], NULL);
    }
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

void *groupcache_set(groupcache_t *gc, void *key, size_t klen, void *value, size_t *vlen) {
    return NULL;
}

char **groupcache_get_peers(groupcache_t *cache, int *num_peers) {
    if (num_peers)
        *num_peers = cache->num_shards;
    return cache->shards;
}
