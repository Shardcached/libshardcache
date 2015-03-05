#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <time.h>
#include <limits.h>
#include <chash.h>
#include <fbuf.h>
#include <rbuf.h>
#include <linklist.h>
#include <queue.h>
#include <iomux.h>

#include <pthread.h>

#include "connections.h"
#include "messaging.h"
#include "connections_pool.h"
#include "shardcache_internal.h"
#include "shardcache_client.h"

#define SHC_PIPELINE_MAX_DEFAULT SHARDCACHE_SERVING_LOOK_AHEAD_DEFAULT

struct shardcache_client_s {
    chash_t *chash;
    shardcache_node_t **shards;
    connections_pool_t *connections;
    int num_shards;
    int use_random_node;
    shardcache_node_t *current_node;
    int pipeline_max;
    int errno;
    int multi_command_max_wait;
    char errstr[1024];
    queue_t *async_jobs;
    pthread_t thread;
    pthread_cond_t wakeup_cond;
    pthread_mutex_t wakeup_lock;
    int quit;
};

int
shardcache_client_tcp_timeout(shardcache_client_t *c, int new_value)
{
    return connections_pool_tcp_timeout(c->connections, new_value);
}

int shardcache_client_check_connection_timeout(shardcache_client_t *c, int new_value)
{
    return connections_pool_expire_time(c->connections, new_value);
}

int
shardcache_client_use_random_node(shardcache_client_t *c, int new_value)
{
    int old_value = c->use_random_node;
    c->use_random_node = new_value;
    return old_value;
}

int
shardcache_client_multi_command_max_wait(shardcache_client_t *c, int new_value)
{
    int old_value = c->multi_command_max_wait;
    c->multi_command_max_wait = new_value;
    return old_value;
}

int
shardcache_client_pipeline_max(shardcache_client_t *c, int new_value)
{
    int old_value = c->pipeline_max;
    if (new_value >= 0)
        c->pipeline_max = new_value;
    return old_value;
}

shardcache_client_t *
shardcache_client_create(shardcache_node_t **nodes, int num_nodes)
{
    int i;
    if (!num_nodes) {
        SHC_ERROR("Can't create a shardcache client with no nodes");
        return NULL;
    }
    shardcache_client_t *c = calloc(1, sizeof(shardcache_client_t));
    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    c->shards = malloc(sizeof(shardcache_node_t *) * num_nodes);
    c->connections = connections_pool_create(SHARDCACHE_TCP_TIMEOUT_DEFAULT,
                                             SHARDCACHE_CONNECTION_EXPIRE_DEFAULT,
                                             1);
    for (i = 0; i < num_nodes; i++) {
        c->shards[i] = shardcache_node_copy(nodes[i]);

        shard_names[i] = shardcache_node_get_label(c->shards[i]);
        shard_lens[i] = strlen(shard_names[i]);
    }

    c->num_shards = num_nodes;

    c->chash = chash_create((const char **)shard_names, shard_lens, c->num_shards, 200);

    struct timeval tv;
    gettimeofday(&tv, NULL);
    srandom((unsigned)tv.tv_usec);

    c->pipeline_max = SHC_PIPELINE_MAX_DEFAULT;

    c->async_jobs = queue_create();

    return c;
}

static inline char *
select_other_node(shardcache_client_t *c, char *addr)
{
    if (c->num_shards == 1)
        return addr;

    shardcache_node_t *node = NULL;
    char *new_addr = NULL;
    do {
        node = c->shards[random()%c->num_shards];
        new_addr = shardcache_node_get_address(node);
    } while (strcmp(addr, new_addr) == 0);
    c->current_node = node;

    return addr;
}

static inline char *
select_node(shardcache_client_t *c, void *key, size_t klen, int *fd)
{
    const char *node_name;
    size_t name_len = 0;
    int i;
    char *addr = NULL;
    shardcache_node_t *node = NULL;

    if (c->num_shards == 1) {
        node = c->shards[0];
    } else if (c->use_random_node) {
        node = c->current_node;
        if (!node) {
            node = c->shards[random()%c->num_shards];
            c->current_node = node;
        }
    } else {
        chash_lookup(c->chash, key, klen, &node_name, &name_len);

        for (i = 0; i < c->num_shards; i++) {
            if (strncmp(node_name, shardcache_node_get_label(c->shards[i]), name_len) == 0)
            {
                node = c->shards[i];
                c->current_node = node;
                break;
            }
        }
    }

    if (node) {
        addr = shardcache_node_get_address(node);
        if (fd) {
            int retries = 3;
            do {
                *fd = connections_pool_get(c->connections, addr);
                if (*fd < 0) {
                    char *other_addr = select_other_node(c, addr);
                    if (other_addr == addr)
                        break;
                    addr = other_addr;
                }
            } while (*fd < 0 && retries--);
        }
    }

    return addr;
}

size_t
shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);

    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return 0;
    }

    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = fetch_from_peer(addr, key, klen, &value, fd);
    if (rc == 0) {
        size_t size = fbuf_used(&value);
        if (data)
            *data = fbuf_data(&value);
        else
            fbuf_destroy(&value);

        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;

        connections_pool_add(c->connections, addr, fd);
        return size;
    } else {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't fetch data from node '%s'", addr);
        return 0;
    }
    return 0;
}

size_t
shardcache_client_offset(shardcache_client_t *c, void *key, size_t klen, uint32_t offset, void *data, uint32_t dlen)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return 0;
    }

    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = offset_from_peer(addr, key, klen, offset, dlen, &value, fd);
    if (rc == 0) {
        uint32_t to_copy = dlen > fbuf_used(&value) ? fbuf_used(&value) : dlen;
        if (data)
            memcpy(data, fbuf_data(&value), to_copy);

        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;

        connections_pool_add(c->connections, addr, fd);
        fbuf_destroy(&value);
        return to_copy;
    } else {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't fetch data from node '%s'", addr);
    }
    fbuf_destroy(&value);
    return 0;
}

int
shardcache_client_exists(shardcache_client_t *c, void *key, size_t klen)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }
    int rc = exists_on_peer(addr, key, klen, fd, 1);
    if (rc == -1) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr),
                "Can't check existance of data on node '%s'", addr);
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

int
shardcache_client_touch(shardcache_client_t *c, void *key, size_t klen)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }
    int rc = touch_on_peer(addr, key, klen, fd);
    if (rc == -1) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr),
                 "Can't touch key '%s' on node '%s'", (char *)key, addr);
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

static inline int64_t
shardcache_client_set_internal(shardcache_client_t *c,
                               void *key,
                               size_t klen,
                               void *data,
                               size_t dlen,
                               void *data2,
                               size_t dlen2,
                               uint32_t expire,
                               int mode) // 0 == SET, 1 == ADD, 2 == CAS, 3 == INCR, 4 == DECR
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }

    int64_t rc = -1;
    switch(mode) {
        case 0:
            rc = send_to_peer(addr, key, klen, data, dlen, expire, 0, fd, 1);
            break;
        case 1:
            rc = add_to_peer(addr, key, klen, data, dlen, expire, 0, fd, 1);
            break;
        case 2:
            rc = cas_on_peer(addr, key, klen, data, dlen, data2, dlen2, expire, 0, fd, 1);
            break;
        case 3:
            rc = increment_on_peer(addr, key, klen, *((int64_t *)data), *((int64_t *)data2), expire, 0, fd, 1);
            break;
        case 4:
            rc = decrement_on_peer(addr, key, klen, *((int64_t *)data), *((int64_t *)data2), expire, 0, fd, 1);
            break;
        default:
            // TODO - Error messages
            return -1;
    }

    if (rc == -1) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't set new data on node '%s'", addr);
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

int
shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire)
{
     return shardcache_client_set_internal(c, key, klen, data, dlen, NULL, 0, expire, 0);
}

int
shardcache_client_add(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire)
{
     return shardcache_client_set_internal(c, key, klen, data, dlen, NULL, 0, expire, 1);
}

int64_t
shardcache_client_increment(shardcache_client_t *c,
                            void *key,
                            size_t klen,
                            int64_t amount,
                            int64_t initial,
                            uint32_t expire)
{
    return shardcache_client_set_internal(c,
                                          key, klen,
                                          (void *)&amount, sizeof(amount),
                                          (void *)&initial, sizeof(initial),
                                          expire,
                                          3);
}

int64_t
shardcache_client_decrement(shardcache_client_t *c,
                            void *key,
                            size_t klen,
                            int64_t amount,
                            int64_t initial,
                            uint32_t expire)
{
    return shardcache_client_set_internal(c,
                                          key, klen,
                                          (void *)&amount, sizeof(amount),
                                          (void *)&initial, sizeof(initial),
                                          expire,
                                          4);
}



int
shardcache_client_del(shardcache_client_t *c, void *key, size_t klen)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }
    int rc = delete_from_peer(addr, key, klen, fd, 1);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't delete data from node '%s'", addr);
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

int
shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }

    int rc = evict_from_peer(addr, key, klen, fd, 1);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't evict data from node '%s'", addr);
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return rc;
}

static inline shardcache_node_t *
shardcache_get_node(shardcache_client_t *c, char *node_name)
{
    shardcache_node_t *node = NULL;

    int i;
    for (i = 0; i < c->num_shards; i++) {
        if (strcmp(node_name, shardcache_node_get_label(c->shards[i])) == 0) {
            node = c->shards[i];
            break;
        }
    }

    if (!node) {
        c->errno = SHARDCACHE_CLIENT_ERROR_ARGS;
        snprintf(c->errstr, sizeof(c->errstr), "Unknown node '%s'", node_name);
        return NULL;
    }

    return node;
}

int
shardcache_client_stats(shardcache_client_t *c, char *node_name, char **buf, size_t *len)
{

    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return -1;

    char *addr = shardcache_node_get_address(node);
    int fd = connections_pool_get(c->connections, addr);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }

    int rc = stats_from_peer(addr, buf, len, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr),
                "Can't get stats from node '%s'", shardcache_node_get_label(node));
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return rc;
}

int
shardcache_client_check(shardcache_client_t *c, char *node_name) {
    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return -1;

    char *addr = shardcache_node_get_address(node);
    int fd = connections_pool_get(c->connections, addr);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }

    int rc = check_peer(addr, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr),
                "Can't check node '%s'", shardcache_node_get_label(node));
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

void
shardcache_client_destroy(shardcache_client_t *c)
{
    if (c->thread) {
        __sync_fetch_and_add(&c->quit, 1);
        pthread_join(c->thread, NULL);
        CONDITION_SIGNAL(c->wakeup_cond, c->wakeup_lock);
        CONDITION_DESTROY(c->wakeup_cond);
        MUTEX_DESTROY(c->wakeup_lock);
    }
    queue_destroy(c->async_jobs);
    chash_free(c->chash);
    shardcache_free_nodes(c->shards, c->num_shards);
    connections_pool_destroy(c->connections);
    free(c);
}

int
shardcache_client_errno(shardcache_client_t *c)
{
    return c->errno;
}

char *
shardcache_client_errstr(shardcache_client_t *c)
{
    return c->errstr;
}

shardcache_storage_index_t *
shardcache_client_index(shardcache_client_t *c, char *node_name)
{
    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return NULL;

    char *addr = shardcache_node_get_address(node);
    int fd = connections_pool_get(c->connections, addr);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return NULL;
    }

    shardcache_storage_index_t *index = index_from_peer(addr, fd);
    if (!index) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr),
                "Can't get index from node '%s'", shardcache_node_get_label(node));
    } else {
        connections_pool_add(c->connections, addr, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return index;
}

int
shardcache_client_migration_begin(shardcache_client_t *c, shardcache_node_t **nodes, int num_nodes)
{
    fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

    int i;
    for (i = 0; i < num_nodes; i++) {
        if (i > 0)
            fbuf_add(&mgb_message, ",");
        fbuf_printf(&mgb_message, "%s:%s", shardcache_node_get_string(nodes[i]));
    }

    for (i = 0; i < c->num_shards; i++) {
        char *addr = shardcache_node_get_address(c->shards[i]);
        int fd = connections_pool_get(c->connections, addr);
        if (fd < 0) {
            c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
            snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
            fbuf_destroy(&mgb_message);
            return -1;
        }

        int rc = migrate_peer(addr,
                              fbuf_data(&mgb_message),
                              fbuf_used(&mgb_message), fd);
        if (rc != 0) {
            close(fd);
            c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
            snprintf(c->errstr, sizeof(c->errstr), "Node '%s' (%s) didn't aknowledge the migration\n",
                    shardcache_node_get_label(c->shards[i]), addr);
            fbuf_destroy(&mgb_message);
            // XXX - should we abort migration on peers that have been notified (if any)?
            return -1;
        }
        connections_pool_add(c->connections, addr, fd);
    }
    fbuf_destroy(&mgb_message);

    c->errno = SHARDCACHE_CLIENT_OK;
    c->errstr[0] = 0;

    return 0;
}

int
shardcache_client_migration_abort(shardcache_client_t *c)
{
    int i;
    for (i = 0; i < c->num_shards; i++) {
        char *addr = shardcache_node_get_address(c->shards[i]);
        char *label = shardcache_node_get_label(c->shards[i]);

        int fd = connections_pool_get(c->connections, addr);
        if (fd < 0) {
            c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
            snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
            return -1;
        }

        int rc =  abort_migrate_peer(label, fd);

        if (rc != 0) {
            close(fd);
            c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
            snprintf(c->errstr, sizeof(c->errstr),
                     "Can't abort migration from node '%s'", label);
            return -1;
        }
        connections_pool_add(c->connections, addr, fd);
    }

    c->errno = SHARDCACHE_CLIENT_OK;
    c->errstr[0] = 0;

    return 0;
}

typedef struct {
    shardcache_client_get_aync_data_cb cb;
    void *priv;
    int fd;
    connections_pool_t *connections;
} shardcache_client_get_async_data_arg_t;

static int
shardcache_client_get_async_data_helper(char *node,
                                        void *key,
                                        size_t klen,
                                        void *data,
                                        size_t dlen,
                                        int idx,  // >= 0 OK, -1 DONE, -2 ERR -3 CLOSE
                                        size_t total_len,
                                        void *priv)
{
    shardcache_client_get_async_data_arg_t *arg = (shardcache_client_get_async_data_arg_t *)priv;
    int rc = 0;
    switch(idx) {
        case -1:
            rc = arg->cb(node, key, klen, NULL, 0, total_len, 0, arg->priv);
            break;
        case -2:
            arg->cb(node, key, klen, data, dlen, total_len, 1, arg->priv);
            close(arg->fd);
            rc = -1;
            break;
        case -3:
            connections_pool_add(arg->connections, node, arg->fd);
            free(arg);
            break;
        case 0:
            rc = arg->cb(node, key, klen, data, dlen, total_len, 0, arg->priv);
            break;
    }

    return rc;
}

int
shardcache_client_get_async(shardcache_client_t *c,
                            void *key,
                            size_t klen,
                            shardcache_client_get_aync_data_cb data_cb,
                            void *priv)
{
    int fd = -1;
    char *addr = select_node(c, key, klen, &fd);
    if (fd < 0) {
        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
        return -1;
    }

    shardcache_client_get_async_data_arg_t *arg = malloc(sizeof(shardcache_client_get_async_data_arg_t));
    arg->connections = c->connections;
    arg->fd = fd;
    arg->cb = data_cb;
    arg->priv = priv;
    return fetch_from_peer_async(addr,
                                 key,
                                 klen,
                                 0,
                                 0,
                                 shardcache_client_get_async_data_helper,
                                 arg,
                                 fd,
                                 NULL);
}

shc_multi_item_t *
shc_multi_item_create(void  *key,
                      size_t klen,
                      void  *data,
                      size_t dlen)
{
    shc_multi_item_t *item = calloc(1, sizeof(shc_multi_item_t));
    item->key = malloc(klen);
    memcpy(item->key, key, klen);
    item->klen = klen;

    if (dlen) {
        item->data = malloc(dlen);
        memcpy(item->data, data, dlen);
        item->dlen = dlen;
    }

    return item;
}

void
shc_multi_item_destroy(shc_multi_item_t *item)
{
    free(item->key);
    free(item->data);
}

static linked_list_t *
shc_split_buckets(shardcache_client_t *c, shc_multi_item_t **items, int *num_items)
{
    linked_list_t *pools = list_create();
    int i;
    for(i = 0; items[i]; i++) {
        shc_multi_item_t *item = items[i];
        item->idx = i;

        char *addr = select_node(c, item->key, item->klen, NULL);

        tagged_value_t *tval = list_get_tagged_value(pools, addr);
        if (!tval || list_count((linked_list_t *)tval->value) > c->pipeline_max)
        {
            linked_list_t *sublist = list_create();
            tval = list_create_tagged_sublist(addr, sublist);
            // put the new sublist at the beggining of the main tagged list
            // so that in case of multiple lists for the same node this will
            // be the one returned by list_get_tagged_value()
            list_unshift_tagged_value(pools, tval);
        }

        list_push_value((linked_list_t *)tval->value, item);

    }

    if (num_items)
        *num_items = i;

    return pools;
}


typedef struct shc_multi_ctx_s shc_multi_ctx_t;
struct shc_multi_ctx_s {
    shardcache_client_t *client;
    char *peer;
    fbuf_t *commands;
    shc_multi_item_t **items;
    async_read_ctx_t *reader;
    int response_index;
    int num_requests;
    shardcache_hdr_t cmd;
    uint32_t *total_count;
    struct timeval last_update;
    int fd;
    int (*cb)(shc_multi_ctx_t *, int);
    void *priv;
};

static int
shc_multi_collect_data(void *data, size_t len, int idx, size_t total_len, void *priv)
{
    if (idx != 0) // XXX - HC (should use the record 1 to check the actual status)
        return 0;

    shc_multi_ctx_t *ctx = (shc_multi_ctx_t *)priv;

    if (ctx->response_index >= ctx->num_requests) {
        ctx->client->errno = SHARDCACHE_CLIENT_ERROR_PROTOCOL;
        snprintf(ctx->client->errstr, sizeof(ctx->client->errstr),
                "Unexpected response (response_index: %d, expected_requests: %d)",
                ctx->response_index, ctx->num_requests);
        return -1;
    }

    shc_multi_item_t *item = ctx->items[ctx->response_index];
    if (len) {
        if (ctx->cmd == SHC_HDR_GET) {
            item->data = realloc(item->data, item->dlen + len);
            memcpy(item->data + item->dlen, data, len);
            item->dlen += len;
        } else {
            if (len == 1) {
                item->status = (int)*((char *)data);
            }
        }
    }
    return 0;
}

static inline void
shc_multi_context_destroy(shc_multi_ctx_t *ctx)
{
    async_read_context_destroy(ctx->reader);
    fbuf_free(ctx->commands);
    free(ctx->items);

    if (ctx->fd >= 0) {
        if (ctx->response_index == ctx->num_requests)
            connections_pool_add(ctx->client->connections, ctx->peer, ctx->fd);
        else
            close(ctx->fd);
    }

    free(ctx);
}

static inline shc_multi_ctx_t *
shc_multi_context_create(shardcache_client_t *c,
                         shardcache_hdr_t cmd,
                         char *peer,
                         linked_list_t *items,
                         uint32_t *total_count,
                         int (*cb)(shc_multi_ctx_t *, int ),
                         void *priv)
{
    shc_multi_ctx_t *ctx = calloc(1, sizeof(shc_multi_ctx_t));
    ctx->client = c;
    ctx->commands = fbuf_create(0);
    ctx->num_requests = list_count(items);
    ctx->items = calloc(1, sizeof(shc_multi_item_t *) * (ctx->num_requests+1));
    ctx->reader = async_read_context_create(shc_multi_collect_data, ctx);
    ctx->cmd = cmd;
    ctx->peer = peer;
    ctx->total_count = total_count;
    ctx->cb = cb;
    ctx->priv = priv;
    int n;
    for (n = 0; n < ctx->num_requests; n++) {
        shc_multi_item_t *item = list_pick_value(items, n);
        ctx->items[n] = item;

        shardcache_record_t record[3] = {
            {
                .v = item->key,
                .l = item->klen
            },
            {
                .v = NULL,
                .l = 0
            },
            {
                .v = NULL,
                .l = 0
            }
        };
        int num_records = 1;

        if (cmd == SHC_HDR_SET) {
            record[1].v = item->data;
            record[1].l = item->dlen;
            num_records = 2;
            if (item->expire) {
                uint32_t expire_nbo = ntohl(item->expire);
                record[2].v = &expire_nbo;
                record[2].l = sizeof(uint32_t);
                num_records = 3;
            }
        }

        if (build_message(cmd, record, num_records, ctx->commands, SHC_PROTOCOL_VERSION) != 0) {
            c->errno = SHARDCACHE_CLIENT_ERROR_INTERNAL;
            snprintf(c->errstr, sizeof(c->errstr), "Can't create new command!");
            fbuf_free(ctx->commands);
            free(ctx->items);
            async_read_context_destroy(ctx->reader);
            free(ctx);
            return NULL;
        }
    }
    gettimeofday(&ctx->last_update, NULL);
    return ctx;
}

static void
shc_multi_eof(iomux_t *iomux, int fd, void *priv)
{
    shc_multi_ctx_t *ctx = (shc_multi_ctx_t *)priv;
    if (ctx->cb) {
        ctx->cb(ctx, 1);
    } else if (ctx->fd != fd) {
            // TODO - Warning message for unexpected condition
            close(fd);
    }
}

static int
shc_multi_fetch_response(iomux_t *iomux, int fd, unsigned char *data, int len, void *priv)
{
    shc_multi_ctx_t *ctx = (shc_multi_ctx_t *)priv;
    int processed = 0;

    gettimeofday(&ctx->last_update, NULL);

    SHC_DEBUG3("received %d\n", len);
    async_read_context_state_t state = async_read_context_input_data(ctx->reader, data, len, &processed);
    while (state == SHC_STATE_READING_DONE) {
        if (ctx->cb && ctx->cb(ctx, 0) != 0) {
            iomux_close(iomux, fd);
            return processed;
        }

        ctx->response_index++;
        if (ctx->total_count)
            ctx->total_count[0]++;
        state = async_read_context_update(ctx->reader);
    }

    if (state == SHC_STATE_READING_ERR) {
        if (ctx->client->errno != SHARDCACHE_CLIENT_ERROR_PROTOCOL) {
            ctx->client->errno = SHARDCACHE_CLIENT_ERROR_PROTOCOL;
            snprintf(ctx->client->errstr, sizeof(ctx->client->errstr),
                    "Async context returned error while parsing response for item %d",
                    ctx->response_index + 1);
        }
        iomux_close(iomux, fd);
    } else if (ctx->response_index == ctx->num_requests) {
        iomux_close(iomux, fd);
    }

    return processed;
}

static void
shc_multi_timeout(iomux_t *iomux, int fd, void *priv)
{
    shc_multi_ctx_t *ctx = (shc_multi_ctx_t *)priv;
    int tcp_timeout = global_tcp_timeout(-1);
    struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
    struct timeval now, diff;
    gettimeofday(&now, NULL);
    timersub(&now, &ctx->last_update, &diff);
    if (timercmp(&diff, &maxwait, >)) {
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        SHC_WARNING("Timeout while waiting for data from %s", inet_ntoa(saddr.sin_addr));
        iomux_close(iomux, fd);
    } else { 
        iomux_set_timeout(iomux, fd, &maxwait);
    }
}

static inline int
shardcache_client_multi_loop(shardcache_client_t *c, iomux_t *iomux, int num_items, int *total_count)
{
    int rc = 0;
    uint32_t previous_count = 0;
    struct timeval hang_time = { 0, 0 };
    struct timeval max_hang_time = { c->multi_command_max_wait, 0 };
    for (;;) {
        struct timeval tv = { 1, 0 };
        iomux_run(iomux, &tv);

        if (iomux_isempty(iomux) || *total_count == num_items)
            break;

        if (previous_count == *total_count) {
            struct timeval now;
            struct timeval diff;
            if (hang_time.tv_sec == 0) {
                gettimeofday(&hang_time, NULL);
            } else {
                gettimeofday(&now, NULL);
                timersub(&now, &hang_time, &diff);
                if (timercmp(&diff, &max_hang_time, >)) {
                    rc = (*total_count == 0) ? -1 : 1;
                    break;
                }
            }
        } else {
            memset(&hang_time, 0, sizeof(hang_time));
        }

        previous_count = *total_count;
    }

    return rc;
}

static inline linked_list_t *
shardcache_client_multi_send_requests(shardcache_client_t *c,
                                      shardcache_hdr_t cmd,
                                      shc_multi_item_t **items,
                                      int num_items,
                                      linked_list_t *pools,
                                      iomux_t *iomux,
                                      uint32_t *total_count,
                                      int (*cb)(shc_multi_ctx_t *, int eof),
                                      void *priv)
{
    uint32_t count = list_count(pools);

    c->errno = SHARDCACHE_CLIENT_OK;
    c->errstr[0] = 0;

    linked_list_t *contexts = list_create();

    int i;
    int tcp_timeout = shardcache_client_tcp_timeout(c, -1);
    struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
    for (i = 0; i < count; i++) {

        tagged_value_t *tval = list_pick_tagged_value(pools, i);
        linked_list_t *items = (linked_list_t *)tval->value;
        char *addr = tval->tag;
        int fd = connections_pool_get(c->connections, addr);
        if (fd < 0) {
            // 1 retry
            char *failed_addr = addr;
            addr = select_other_node(c, addr);
            fd = connections_pool_get(c->connections, addr);
            SHC_WARNING("Can't connect to node at address %s, falling back to %s",
                        failed_addr, addr);
        }

        if (fd < 0) {
            c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
            snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);

            shc_multi_ctx_t *ctx;
            while ((ctx = list_shift_value(contexts))) {
                iomux_remove(iomux, ctx->fd);
                shc_multi_context_destroy(ctx);
            }
            list_destroy(contexts);
            return NULL;
        }

        shc_multi_ctx_t *ctx = shc_multi_context_create(c, cmd, addr, items, total_count, cb, priv);
        if (!ctx) {
            while ((ctx = list_shift_value(contexts))) {
                iomux_remove(iomux, ctx->fd);
                shc_multi_context_destroy(ctx);
            }
            list_destroy(contexts);
            return NULL;
        }

        ctx->fd = fd;
        list_push_value(contexts, ctx);

        iomux_callbacks_t cbs = {
            .mux_output = NULL,
            .mux_timeout = shc_multi_timeout,
            .mux_eof = shc_multi_eof,
            .mux_input = shc_multi_fetch_response,
            .priv = ctx
        };


        if (!iomux_add(iomux, fd, &cbs)) {
            while ((ctx = list_shift_value(contexts))) {
                iomux_remove(iomux, ctx->fd);
                shc_multi_context_destroy(ctx);
            }
            list_destroy(contexts);
            return NULL;
        }

        iomux_set_timeout(iomux, fd, &maxwait);

        char *output = NULL;
        unsigned int len = fbuf_detach(ctx->commands, &output, NULL);
        iomux_write(iomux, fd, (unsigned char *)output, len, 1);
    }
    return contexts;
}

static inline int
shardcache_client_multi(shardcache_client_t *c,
                         shc_multi_item_t **items,
                         shardcache_hdr_t cmd)
{
    int num_items = 0;
    linked_list_t *pools = shc_split_buckets(c, items, &num_items);

    SHC_DEBUG("Requesting %d items using %u connections",
              num_items, list_count(pools));

    iomux_t *iomux = iomux_create(0, 0);


    uint32_t total_count = 0;

    linked_list_t *contexts = shardcache_client_multi_send_requests(c, cmd, items, num_items, pools, iomux, &total_count, NULL, NULL);
    if (!contexts) {
        iomux_destroy(iomux);
        list_destroy(pools);
        return -1;
    }

    // this will run the iomux until we get all the response, an error occurs
    // or the timeout (c->multi_command_max_wait) expires
    int rc = shardcache_client_multi_loop(c, iomux, num_items, &total_count);

    shc_multi_ctx_t *ctx = NULL;
    while ((ctx = list_shift_value(contexts))) {
        if (ctx->fd >= 0)
            iomux_remove(iomux, ctx->fd);
        shc_multi_context_destroy(ctx);
    }
    iomux_destroy(iomux);
    list_destroy(contexts);
    list_destroy(pools);

    if (total_count != num_items) {
        if (c->errno != SHARDCACHE_CLIENT_ERROR_PROTOCOL) {
            c->errno = SHARDCACHE_CLIENT_ERROR_PROTOCOL;
            snprintf(c->errstr, sizeof(c->errstr),
                    "Number of responses doesn't match (received: %d, expected: %d)",
                    total_count, num_items);

        }
        rc = 2;
    }

    return rc;
}

int
shardcache_client_get_multi(shardcache_client_t *c,
                            shc_multi_item_t **items)

{
    return shardcache_client_multi(c, items, SHC_HDR_GET);
}

int
shardcache_client_set_multi(shardcache_client_t *c,
                            shc_multi_item_t **items)

{
    return shardcache_client_multi(c, items, SHC_HDR_SET);
}

shardcache_node_t *
shardcache_client_current_node(shardcache_client_t *c)
{
    return c->current_node;
}

typedef struct {
    enum {
        JOB_CMD_GET,
        JOB_CMD_GET_MULTI
    } cmd;
    union {
        struct {
            void *key;
            size_t klen;
            int fd;
            async_read_wrk_t *wrk;
        } single;
        struct {
            shc_multi_item_t **items;
            int nitems;
            linked_list_t *pools;
            linked_list_t *contexts;
            int done;
        } multi;
    } arg;
    int pipe[2];
    fbuf_t buf;
} async_job_t;

static async_job_t *
async_job_create(shardcache_client_t *c, int cmd, void *argp, size_t argn)
{
    async_job_t *job = calloc(1, sizeof(async_job_t));
    job->cmd = cmd;
    int rc = pipe(job->pipe);
    if (rc != 0) {
        free(job);
        return NULL;
    }

    switch(cmd) {
        case JOB_CMD_GET:
        {
            job->arg.single.key = malloc(argn);
            memcpy(job->arg.single.key, argp, argn);
            job->arg.single.klen = argn;
            break;
        }
        case JOB_CMD_GET_MULTI:
        {
            shc_multi_item_t **items = (shc_multi_item_t **)argp;
            shc_multi_item_t **copy = malloc(sizeof(shc_multi_item_t *));
            int i;
            for (i = 0; items[i]; i++) {
                copy = realloc(copy, (sizeof(shc_multi_item_t *) * (i+1)) + 1);
                copy[i] = shc_multi_item_create(items[i]->key, items[i]->klen, NULL, 0);
            }
            copy[i] = NULL;
            job->arg.multi.items = copy;
            job->arg.multi.pools = shc_split_buckets(c,  job->arg.multi.items, &job->arg.multi.nitems);
            break;
        }
        default:
            // TODO - error message for unsupported command
            free(job);
            return NULL;
            break;
    }

    FBUF_STATIC_INITIALIZER_POINTER(&job->buf, FBUF_MAXLEN_NONE, 64, 1024, 512);
    return job;
}

static void
async_job_destroy(async_job_t *job)
{
    switch(job->cmd) {
        case JOB_CMD_GET:
            free(job->arg.single.key);
            if (job->arg.single.fd >= 0)
                close(job->arg.single.fd);
            free(job->arg.single.wrk);
            break;
        case JOB_CMD_GET_MULTI:
            list_destroy(job->arg.multi.pools);
            if (job->arg.multi.contexts) {
                shc_multi_ctx_t *ctx = NULL;
                while ((ctx = list_shift_value(job->arg.multi.contexts)))
                    shc_multi_context_destroy(ctx);
                list_destroy(job->arg.multi.contexts);
                int i;
                for (i = 0; job->arg.multi.items[i]; i++)
                    shc_multi_item_destroy(job->arg.multi.items[i]);
                free(job->arg.multi.items);
            }
            break;
    }
    if (job->pipe[1] >= 0)
        close(job->pipe[1]);

    fbuf_destroy(&job->buf);

    free(job);
}

static int
async_thread_get(char *peer,
                 void *key,
                 size_t klen,
                 void *data,
                 size_t len,
                 int idx, // >= 0 OK, -1 DONE, -2 ERR -3 CLOSE
                 size_t total_len,
                 void *priv)
{
    async_job_t *job = (async_job_t *)priv;
    switch (idx) {
        case 0:
        {
            int pending = fbuf_used(&job->buf);
            if (pending) {
                int wb = write(job->pipe[1], fbuf_data(&job->buf), pending);
                if (wb == -1) {
                    async_job_destroy(job);
                    return -1;
                } else if (wb > 0) {
                    fbuf_remove(&job->buf, wb);
                }
            }
            if (data && len) {
                if (fbuf_used(&job->buf)) {
                    fbuf_add_binary(&job->buf, data, len);
                    return 0;
                }
                int wb = write(job->pipe[1], data, len);
                if (wb != len) {
                    if (wb == -1) {
                        // Error
                    } else if (wb > 0) {
                        fbuf_add_binary(&job->buf, data + wb, len - wb);
                        return 0;
                    }
                }
            }
            break;
        }
        case -1:
            break;
        case -2:
            async_job_destroy(job);
            return -1;
        case -3:
            async_job_destroy(job);
            break;
        default:
            break;
    }
    return 0;
}
 
static int
async_thread_get_multi(shc_multi_ctx_t *ctx, int eof)
{
    async_job_t *job = (async_job_t *)ctx->priv;
    shc_multi_item_t *item = ctx->items[ctx->response_index];
    
    int pending = fbuf_used(&job->buf);
    if (pending) {
        int wb = write(job->pipe[1], fbuf_data(&job->buf), pending);
        if (wb == -1) {
            async_job_destroy(job);
            return -1;
        } else if (wb > 0) {
            fbuf_remove(&job->buf, wb);
        }
    }

    if (eof) {
        job->arg.multi.done++;
        if (job->arg.multi.done == list_count(job->arg.multi.contexts))
            async_job_destroy(job);
        return 0;
    }

    uint32_t idx_nbo = htonl(item->idx);
    uint32_t dlen_nbo = htonl(item->dlen);
    if (fbuf_used(&job->buf)) {
        fbuf_add_binary(&job->buf, (char *)&idx_nbo, sizeof(idx_nbo));
        fbuf_add_binary(&job->buf, (char *)&dlen_nbo, sizeof(dlen_nbo));
        fbuf_add_binary(&job->buf, item->data, item->dlen);
        return 0;
    }

    int wb = write(job->pipe[1], &idx_nbo, sizeof(idx_nbo));
    if (wb != sizeof(idx_nbo)) {
        if (wb == -1)
            return -1;
    }

    wb = write(job->pipe[1], &dlen_nbo, sizeof(dlen_nbo));
    if (wb != sizeof(dlen_nbo)) {
        return -1;
    }

    wb = write(job->pipe[1], item->data, item->dlen);
    if (wb != item->dlen) {
        if (wb >= 0) {
            fbuf_add_binary(&job->buf, item->data + wb, item->dlen - wb);
        } else {
            return -1;
        }
    }

    return 0;
}

static void *
async_thread(void *priv)
{
    shardcache_client_t *c = (shardcache_client_t *)priv;

    iomux_t *iomux = iomux_create(0, 0);
    while (!__sync_fetch_and_add(&c->quit, 0)) {
        async_job_t *job = queue_pop_right(c->async_jobs);
        while (job) {
            switch(job->cmd) {
                case JOB_CMD_GET:
                {
                    job->arg.single.fd = -1;
                    char *addr = select_node(c, job->arg.single.key, job->arg.single.klen, &job->arg.single.fd);
                    if (job->arg.single.fd < 0) {
                        c->errno = SHARDCACHE_CLIENT_ERROR_NETWORK;
                        snprintf(c->errstr, sizeof(c->errstr), "Can't connect to '%s'", addr);
                        async_job_destroy(job);
                        continue;
                    }

                    int rc = fetch_from_peer_async(addr,
                                                   job->arg.single.key,
                                                   job->arg.single.klen,
                                                   0,
                                                   0,
                                                   async_thread_get,
                                                   job,
                                                   job->arg.single.fd,
                                                   &job->arg.single.wrk);
                    if (rc == 0) {
                        if (!iomux_add(iomux, job->arg.single.wrk->fd, &job->arg.single.wrk->cbs)) {
                             if (job->arg.single.wrk->fd >= 0)
                                 job->arg.single.wrk->cbs.mux_eof(iomux, job->arg.single.wrk->fd, job->arg.single.wrk->cbs.priv);
                             else
                                 async_read_context_destroy(job->arg.single.wrk->ctx);
                             async_job_destroy(job);
                        } else {
                            int tcp_timeout = shardcache_client_tcp_timeout(c, -1);
                            struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
                            iomux_set_timeout(iomux, job->arg.single.wrk->fd, &maxwait);
                        }
                    } else {
                        async_job_destroy(job);
                    }
                    break;
                }
                case JOB_CMD_GET_MULTI:
                {
                    job->arg.multi.contexts = shardcache_client_multi_send_requests(c,
                                                                                    SHC_HDR_GET,
                                                                                    job->arg.multi.items,
                                                                                    job->arg.multi.nitems,
                                                                                    job->arg.multi.pools,
                                                                                    iomux,
                                                                                    NULL,
                                                                                    async_thread_get_multi,
                                                                                    job);
                    if (!job->arg.multi.contexts) {
                        // TODO - Error messages
                        async_job_destroy(job);
                    }

                    break;
                }
            }
            job = queue_pop_right(c->async_jobs);
        }

        struct timeval wait_time = { 0, 500000 }; // 500 ms
        if (!iomux_isempty(iomux)) {
            iomux_run(iomux, &wait_time);
        } else {
            struct timeval now, abs_wait_time;
            gettimeofday(&now, NULL);
            timeradd(&now, &wait_time, &abs_wait_time);
            struct timespec ts = { abs_wait_time.tv_sec, abs_wait_time.tv_usec * 1000 };
            CONDITION_TIMEDWAIT(c->wakeup_cond, c->wakeup_lock, &ts);
        }

    }
    iomux_destroy(iomux);
    pthread_exit(0);
}

int
shardcache_client_getf(shardcache_client_t *c, void *key, size_t klen)
{

    async_job_t *job = async_job_create(c, JOB_CMD_GET, key, klen);

    if (job) {
        if (!c->thread) {
            MUTEX_INIT(c->wakeup_lock);
            CONDITION_INIT(c->wakeup_cond);
            pthread_create(&c->thread, NULL, async_thread, c);
        }
        queue_push_left(c->async_jobs, job);
        CONDITION_SIGNAL(c->wakeup_cond, c->wakeup_lock);
        return job->pipe[0];
    }
    return -1;
}

int
shardcache_client_get_multif(shardcache_client_t *c, shc_multi_item_t **items)
{
    async_job_t *job = async_job_create(c, JOB_CMD_GET_MULTI, items, 0);

    if (job) {
        if (!c->thread) {
            MUTEX_INIT(c->wakeup_lock);
            CONDITION_INIT(c->wakeup_cond);
            pthread_create(&c->thread, NULL, async_thread, c);
        }

        queue_push_left(c->async_jobs, job);
        CONDITION_SIGNAL(c->wakeup_cond, c->wakeup_lock);
        return job->pipe[0];
    }
    return -1;
}
// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
