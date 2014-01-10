#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <regex.h>
#include <chash.h>
#include <fbuf.h>
#include <rbuf.h>
#include <linklist.h>
#include <iomux.h>

#include "connections.h"
#include "messaging.h"
#include "connections_pool.h"
#include "shardcache_client.h"

typedef struct chash_t chash_t;

struct shardcache_client_s {
    chash_t *chash;
    shardcache_node_t *shards;
    connections_pool_t *connections;
    int num_shards;
    const char *auth;
    int errno;
    char errstr[1024];
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
    if (!num_nodes) {
        fprintf(stderr, "Can't create a shardcache client with no nodes\n");
        return NULL;
    }
    shardcache_client_t *c = calloc(1, sizeof(shardcache_client_t));

    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    c->shards = malloc(sizeof(shardcache_node_t) * num_nodes);
    c->connections = connections_pool_create(30);
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
    }

    c->num_shards = num_nodes;

    c->chash = chash_create((const char **)shard_names, shard_lens, c->num_shards, 200);

    if (auth && strlen(auth)) {
        c->auth = calloc(1, 16);
        strncpy((char *)c->auth, auth, 16);
    }

    return c;
}

char *select_node(shardcache_client_t *c, void *key, size_t klen)
{
    const char *node_name;
    size_t name_len = 0;
    int i;
    char *addr = NULL;
    chash_lookup(c->chash, key, klen, &node_name, &name_len);

    for (i = 0; i < c->num_shards; i++) {
        if (strncmp(node_name, c->shards[i].label, name_len) == 0) {
            addr = c->shards[i].address;
            break;
        }
    }

    return addr;
}

size_t shardcache_client_get(shardcache_client_t *c, void *key, size_t klen, void **data)
{
    char *node = select_node(c, key, klen);
    int fd = connections_pool_get(c->connections, node);
    if (fd < 0)
        return 0;

    fbuf_t value = FBUF_STATIC_INITIALIZER;
    int rc = fetch_from_peer(node, (char *)c->auth, key, klen, &value, fd);
    if (rc == 0) {
        if (data)
            *data = fbuf_data(&value);

        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;

        connections_pool_add(c->connections, node, fd);
        return fbuf_used(&value);
    } else {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't fetch data from node: %s", node);
        return 0;
    }
    return 0;
}

int shardcache_client_set(shardcache_client_t *c, void *key, size_t klen, void *data, size_t dlen, uint32_t expire)
{
    char *node = select_node(c, key, klen);
    int fd = connections_pool_get(c->connections, node);
    if (fd < 0)
        return -1;

    int rc = send_to_peer(node, (char *)c->auth, key, klen, data, dlen, expire, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't set new data on node: %s", node);
    } else {
        connections_pool_add(c->connections, node, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

int shardcache_client_del(shardcache_client_t *c, void *key, size_t klen)
{
    char *node = select_node(c, key, klen);
    int fd = connections_pool_get(c->connections, node);
    if (fd < 0)
        return -1;
    int rc = delete_from_peer(node, (char *)c->auth, key, klen, 1, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't delete data from node: %s", node);
    } else {
        connections_pool_add(c->connections, node, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

int shardcache_client_evict(shardcache_client_t *c, void *key, size_t klen)
{
    char *node = select_node(c, key, klen);
    int fd = connections_pool_get(c->connections, node);
    if (fd < 0)
        return -1;

    int rc = delete_from_peer(node, (char *)c->auth, key, klen, 0, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't evict data from node: %s", node);
    } else {
        connections_pool_add(c->connections, node, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return rc;
}

shardcache_node_t *shardcache_get_node(shardcache_client_t *c, char *node_name)
{
    shardcache_node_t *node = NULL;

    int i;
    for (i = 0; i < c->num_shards; i++) {
        if (strcmp(node_name, c->shards[i].label) == 0) {
            node = &c->shards[i];
            break;
        }
    }

    if (!node) {
        c->errno = SHARDCACHE_CLIENT_ERROR_ARGS;
        snprintf(c->errstr, sizeof(c->errstr), "Unknown node: %s", node_name);
        return NULL;
    }

    return node;
}
int shardcache_client_stats(shardcache_client_t *c, char *node_name, char **buf, size_t *len)
{

    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return -1;

    int fd = connections_pool_get(c->connections, node->address);
    if (fd < 0)
        return -1;

    int rc = stats_from_peer(node->address, (char *)c->auth, buf, len, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't get stats from node: %s", node->label);
    } else {
        connections_pool_add(c->connections, node->address, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return rc;
}

int shardcache_client_check(shardcache_client_t *c, char *node_name) {
    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return -1;

    int fd = connections_pool_get(c->connections, node->address);
    if (fd < 0)
        return -1;

    int rc = check_peer(node->address, (char *)c->auth, fd);
    if (rc != 0) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't check node: %s", node->label);
    } else {
        connections_pool_add(c->connections, node->address, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }
    return rc;
}

void shardcache_client_destroy(shardcache_client_t *c)
{
    chash_free(c->chash);
    free(c->shards);
    if (c->auth)
        free((void *)c->auth);
    connections_pool_destroy(c->connections);
    free(c);
}

int shardcache_client_errno(shardcache_client_t *c)
{
    return c->errno;
}

char *shardcache_client_errstr(shardcache_client_t *c)
{
    return c->errstr;
}

shardcache_storage_index_t *shardcache_client_index(shardcache_client_t *c, char *node_name)
{
    shardcache_node_t *node = shardcache_get_node(c, node_name);
    if (!node)
        return NULL;

    int fd = connections_pool_get(c->connections, node->address);
    if (fd < 0)
        return NULL;

    shardcache_storage_index_t *index = index_from_peer(node->address, (char *)c->auth, fd);
    if (!index) {
        close(fd);
        c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
        snprintf(c->errstr, sizeof(c->errstr), "Can't get index from node: %s", node->label);
    } else {
        connections_pool_add(c->connections, node->address, fd);
        c->errno = SHARDCACHE_CLIENT_OK;
        c->errstr[0] = 0;
    }

    return index;
}

int shardcache_client_migration_begin(shardcache_client_t *c, shardcache_node_t *nodes, int num_nodes)
{
    fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

    int i;
    for (i = 0; i < num_nodes; i++) {
        if (check_address_string(nodes[i].address) != 0) {
            c->errno = SHARDCACHE_CLIENT_ERROR_ARGS;
            snprintf(c->errstr, sizeof(c->errstr), "Bad address format %s\n", nodes[i].address);
            fbuf_destroy(&mgb_message);
            return -1;
        }
        if (i > 0) 
            fbuf_add(&mgb_message, ",");
        fbuf_printf(&mgb_message, "%s:%s", nodes[i].label, nodes[i].address);
    }

    for (i = 0; i < c->num_shards; i++) {
        int fd = connections_pool_get(c->connections, c->shards[i].address);
        if (fd < 0) {
            fbuf_destroy(&mgb_message);
            return -1;
        }

        int rc = migrate_peer(c->shards[i].address,
                              (char *)c->auth,
                              fbuf_data(&mgb_message),
                              fbuf_used(&mgb_message), fd);
        if (rc != 0) {
            close(fd);
            c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
            snprintf(c->errstr, sizeof(c->errstr), "Node %s (%s) didn't aknowledge the migration\n",
                    c->shards[i].label, c->shards[i].address);
            fbuf_destroy(&mgb_message);
            // XXX - should we abort migration on peers that have been notified (if any)? 
            return -1;
        }
        connections_pool_add(c->connections, c->shards[i].address, fd);
    }
    fbuf_destroy(&mgb_message);

    c->errno = SHARDCACHE_CLIENT_OK;
    c->errstr[0] = 0;

    return 0;
}

int shardcache_client_migration_abort(shardcache_client_t *c)
{
    int i;
    for (i = 0; i < c->num_shards; i++) {
        int fd = connections_pool_get(c->connections, c->shards[i].address);
        if (fd < 0) {
            return -1;
        }

        int rc =  abort_migrate_peer(c->shards[i].label, (char *)c->auth, fd);

        if (rc != 0) {
            close(fd);
            c->errno = SHARDCACHE_CLIENT_ERROR_NODE;
            snprintf(c->errstr, sizeof(c->errstr),
                     "Can't abort migration from node: %s", c->shards[i].label);
            return -1;
        }
        connections_pool_add(c->connections, c->shards[i].address, fd);
    }
 
    c->errno = SHARDCACHE_CLIENT_OK;
    c->errstr[0] = 0;

    return 0;
}

typedef struct {
    shardcache_client_get_aync_data_cb cb;
    void *key;
    size_t klen;
    void *cb_priv;
    char *auth;
    rbuf_t *buf;
#define STATE_READING_NONE   0x00
#define STATE_READING_HDR    0x01
#define STATE_READING_RECORD 0x02
#define STATE_READING_AUTH   0x05
#define STATE_READING_DONE   0x06
#define STATE_READING_ERR    0x07
    char    state;
    uint16_t clen;
    char    eom_expected;
 
} read_message_async_ctx;

static void input_data(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    read_message_async_ctx *ctx = (read_message_async_ctx *)priv;

    int wb = rbuf_write(ctx->buf, data, len);
    if (wb != len) {
        fprintf(stderr, "read_message_async buffer underrun!\n");
        iomux_close(iomux, fd);
        return;
    }

    if (!rbuf_len(ctx->buf))
        return;

    if (ctx->state == STATE_READING_NONE || ctx->state == STATE_READING_HDR)
    {
        unsigned char hdr;
        rbuf_read(ctx->buf, &hdr, 1);
        while (hdr == SHARDCACHE_HDR_NOP && rbuf_len(ctx->buf) > 0)
            rbuf_read(ctx->buf, &hdr, 1); // skip

        if (hdr == SHARDCACHE_HDR_NOP)
            return;

        if (ctx->state == STATE_READING_NONE) {
            if (hdr == SHARDCACHE_HDR_SIG)
            {
                if (!ctx->auth) {
                    ctx->state = STATE_READING_ERR;
                    iomux_close(iomux, fd);
                    return;
                }

                ctx->state = STATE_READING_HDR;

                if (rbuf_len(ctx->buf) < 1) {
                    return;
                }

                rbuf_read(ctx->buf, &hdr, 1);
            } else if (ctx->auth) {
                // we are expecting the signature header
                ctx->state = STATE_READING_ERR;
                iomux_close(iomux, fd);
                return;
            }
        }

        if (hdr != SHARDCACHE_HDR_RES)
        {
            // BAD RESPONSE
#ifdef SHARDCACHE_DEBUG
            struct sockaddr_in saddr;
            socklen_t addr_len = sizeof(struct sockaddr_in);
            getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
            fprintf(stderr, "BAD RESPONSE %02x from %s\n", hdr, inet_ntoa(saddr.sin_addr));
#endif
            ctx->state = STATE_READING_ERR;
            iomux_close(iomux, fd);
            return;
        }

        ctx->state = STATE_READING_RECORD;
    }

    for (;;) {
        if (ctx->state == STATE_READING_AUTH)
            break;
        
        if (ctx->clen == 0) {
            if (rbuf_len(ctx->buf) < 2)
                break;
            uint16_t nlen = 0;
            rbuf_read(ctx->buf, (u_char *)&nlen, 2);
            ctx->clen = ntohs(nlen);
        }
        if (ctx->clen > 0) {
            char chunk[ctx->clen];
            int rb = rbuf_read(ctx->buf, chunk, ctx->clen);
            ctx->cb(ctx->key, ctx->klen, chunk, rb, ctx->cb_priv);
            ctx->clen -= rb;
            if (!rbuf_len(ctx->buf))
                break; // TRUNCATED - we need more data
        } else {
            if (rbuf_len(ctx->buf) < 1) {
                // TRUNCATED - we need more data
                ctx->eom_expected = 1;
                break;
            }

            u_char bsep = 0;
            rbuf_read(ctx->buf, &bsep, 1);
            ctx->eom_expected = 0;
            if (bsep == 0) {
                if (ctx->auth)
                    ctx->state = STATE_READING_AUTH;
                else
                    ctx->state = STATE_READING_DONE;
                break;
            } else {
                // unexpected response (contains more than 1 record?)
                iomux_close(iomux, fd);
                ctx->state = STATE_READING_ERR;
                return;
            }
        }
    }

    if (ctx->state == STATE_READING_AUTH) {
        if (rbuf_len(ctx->buf) < SHARDCACHE_MSG_SIG_LEN)
            return;

        uint64_t received_digest;
        rbuf_read(ctx->buf, (u_char *)&received_digest, sizeof(received_digest));
        // XXX - we are ignoring (and not checking) the signature
        ctx->state = STATE_READING_DONE;
    }

    if (ctx->state == STATE_READING_DONE)
        iomux_close(iomux, fd);
}

static void input_eof(iomux_t *iomux, int fd, void *priv)
{
    iomux_end_loop(iomux);
}

int read_message_async(int fd, char *auth, void *key, size_t len, shardcache_client_get_aync_data_cb cb, void *priv)
{
    struct timeval iomux_timeout = { 0, 20000 };
    read_message_async_ctx ctx = {
        .buf = rbuf_create(1<<16),
        .cb = cb,
        .cb_priv = priv,
        .auth = auth,
        .key = key,
        .klen = len
    };

    iomux_callbacks_t cbs = {
        .mux_input = input_data,
        .mux_eof = input_eof,
        .priv = &ctx
    };
    iomux_t *iomux= iomux_create();
    if (!iomux)
        return -1;

    iomux_add(iomux, fd, &cbs);
    iomux_loop(iomux, &iomux_timeout);
    iomux_destroy(iomux);

    rbuf_destroy(ctx.buf);
    if (ctx.state = STATE_READING_ERR)
        return -1;

    return 0;
}

int fetch_from_peer_async(char *peer, char *auth, void *key, size_t len, shardcache_client_get_aync_data_cb cb, void *priv, int fd)
{
    int rc = -1;
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        rc = write_message(fd, auth, SHARDCACHE_HDR_GET, key, len, NULL, 0, 0);
        if (rc == 0) {
            rc = read_message_async(fd, auth, key, len, cb, priv);
        }
        if (should_close)
            close(fd);
    }
    return rc;
}

int shardcache_client_get_async(shardcache_client_t *c,
                                   void *key,
                                   size_t klen,
                                   shardcache_client_get_aync_data_cb data_cb,
                                   void *priv)
{
    char *node = select_node(c, key, klen);
    int fd = connections_pool_get(c->connections, node);
    if (fd < 0)
        return -1;

    return fetch_from_peer_async(node, (char *)c->auth, key, klen, data_cb, priv, fd);
}

