#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <regex.h>
#include <chash.h>
#include <fbuf.h>
#include <linklist.h>

#include "connections.h"
#include "messaging.h"
#include "shardcache_client.h"

typedef struct chash_t chash_t;

typedef struct {
    char *label;
    int fd;
} shardcache_connection_t;

struct shardcache_client_s {
    chash_t *chash;
    shardcache_node_t *shards;
    shardcache_connection_t *connections;
    int num_shards;
    const char *auth;
};

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"
static int check_address_string(char *str)
{
    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        fprintf(stderr, "Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        return -1;
    }

    int matched = regexec(&addr_regexp, str, 0, NULL, 0);
    regfree(&addr_regexp);

    if (matched != 0) {
        return -1;
    }

    return 0;
}

shardcache_client_t *shardcache_client_create(shardcache_node_t *nodes, int num_nodes, char *auth)
{
    int i;
    shardcache_client_t *c = calloc(1, sizeof(shardcache_client_t));

    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    c->shards = malloc(sizeof(shardcache_node_t) * num_nodes);
    c->connections = malloc(sizeof(shardcache_connection_t) * num_nodes);
    memcpy(c->shards, nodes, sizeof(shardcache_node_t) * num_nodes);
    for (i = 0; i < num_nodes; i++) {
        if (check_address_string(c->shards[i].address) != 0) {
            fprintf(stderr, "Bad address format %s\n", c->shards[i].address);
            free(c->shards);
            free(c);
            return NULL;
        }
        shard_names[i] = c->shards[i].label;
        shard_lens[i] = strlen(shard_names[i]);
        c->connections[i].label = c->shards[i].label;
        c->connections[i].fd = -1;
    }

    c->num_shards = num_nodes;

    c->chash = chash_create((const char **)shard_names, shard_lens, c->num_shards, 200);

    if (auth && strlen(auth))
        strncpy((char *)c->auth, auth, 16);

    return c;
}


int get_connection_for_peer(shardcache_client_t *c, char *peer)
{
    int i;
    shardcache_connection_t *conn = NULL;

    for (i = 0; i < c->num_shards; i++) {
        if (strcmp(peer, c->connections[i].label) == 0) {
            if (c->connections[i].fd >= 0) {
                char noop = SHARDCACHE_HDR_NOP;
                if (write(c->connections[i].fd, &noop, 1) == 1) {
                    return c->connections[i].fd;
                } else {
                    close(c->connections[i].fd);
                    c->connections[i].fd = -1;
                }
            }
            conn = &c->connections[i];
            break;
        }
    }
    for (i = 0; i < c->num_shards; i++) {
        if (strcmp(peer, c->shards[i].label) == 0) {
            int fd = connect_to_peer(c->shards[i].address, 30);
            if (conn)
                conn->fd = fd;
            return fd;
        }
    }
    return -1; 
}

char *select_peer(shardcache_client_t *c, void *key, size_t klen)
{
    const char *node_name;
    size_t name_len = 0;
    int i;
    char *addr = NULL;
    chash_lookup(c->chash, key, klen, &node_name, &name_len);

    for (i = 0; i < c->num_shards; i++) {
        if (strcmp(node_name, c->shards[i].label) == 0) {
            addr = c->shards[i].label;
            break;
        }
    }

    return addr;
}

size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data)
{
    char *peer = select_peer(c, key, klen);
    int fd = get_connection_for_peer(c, peer);
    if (fd < 0)
        return 0;
    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = fetch_from_peer(peer, (char *)c->auth, key, klen, &value, fd);
    if (rc == 0) {
        if (data)
            *data = fbuf_data(&value);
        return fbuf_used(&value);
    }
    return 0;
}

int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire)
{
    char *peer = select_peer(c, key, klen);
    int fd = get_connection_for_peer(c, peer);
    if (fd < 0)
        return -1;

    return send_to_peer(peer, (char *)c->auth, key, klen, data, dlen, expire, fd);
}

int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen)
{
    char *peer = select_peer(c, key, klen);
    int fd = get_connection_for_peer(c, peer);
    if (fd < 0)
        return -1;
    return delete_from_peer(peer, (char *)c->auth, key, klen, 1, fd);
}

int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen)
{
    char *peer = select_peer(c, key, klen);
    int fd = get_connection_for_peer(c, peer);
    if (fd < 0)
        return -1;

    return delete_from_peer(peer, (char *)c->auth, key, klen, 0, fd);
}

void shardcache_client_destroy(shardcache_client_t *c)
{
    chash_free(c->chash);
    free(c->shards);
    if (c->auth)
        free((void *)c->auth);
    free(c);
}


