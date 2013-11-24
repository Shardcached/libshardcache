#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <pthread.h>
#include <iomux.h>
#include "messaging.h"
#include "connections.h"
#include "groupcache.h"

#include "serving.h"

typedef struct {
    fbuf_t *input;
    fbuf_t *output;
    int fd;
    groupcache_t *cache;
    groupcache_hdr_t hdr;
    fbuf_t *key;
    fbuf_t *value;
#define STATE_READING_NONE 0x00
#define STATE_READING_KEY 0x01
#define STATE_READING_VALUE 0x02
#define STATE_READING_DONE 0x03
    char    state;
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

static void write_status(groupcache_worker_context_t *ctx, int rc) {
    if (rc != 0) {
        fprintf(stderr, "Error running command %d (key %s)\n", ctx->hdr, fbuf_data(ctx->key));
        write_message(ctx->fd, GROUPCACHE_HDR_RES, "ERR", 3);
    } else {
        write_message(ctx->fd, GROUPCACHE_HDR_RES, "OK", 2);
    }

}

static void *serve_request(void *priv) {
    groupcache_worker_context_t *ctx = (groupcache_worker_context_t *)priv;
    groupcache_t *cache = ctx->cache;
    int fd = ctx->fd;

    // let's ensure setting the fd to blocking mode
    // (if it was used in the iomux earlier, it was set to non-blocking)
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
    void *key = fbuf_data(ctx->key);
    size_t klen = fbuf_used(ctx->key);
    switch(ctx->hdr) {

        case GROUPCACHE_HDR_GET:
        {
            size_t vlen = 0;
            void *v = groupcache_get(cache, key, klen, &vlen);
            write_message(fd, GROUPCACHE_HDR_RES, v, vlen);
            break;
        }
        case GROUPCACHE_HDR_SET:
        {
            rc = groupcache_set(cache, key, klen,
                    fbuf_data(ctx->value), fbuf_used(ctx->value));
            write_status(ctx, rc);
            break;
        }
        case GROUPCACHE_HDR_DEL:
        {
            rc = groupcache_del(cache, key, klen);
            write_status(ctx, rc);
            break;
        }
        case GROUPCACHE_HDR_EVI:
        {
            groupcache_evict(cache, key, klen);
            write_status(ctx, 0);
            break;
        }
        default:
            fprintf(stderr, "Unknown command: 0x%02x\n", (char)ctx->hdr);
            break;
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

    if (ctx->state == STATE_READING_NONE) {
        char *input = fbuf_data(ctx->input);
        char hdr = *input;

        if (hdr != GROUPCACHE_HDR_GET &&
            hdr != GROUPCACHE_HDR_SET &&
            hdr != GROUPCACHE_HDR_DEL &&
            hdr != GROUPCACHE_HDR_EVI &&
            hdr != GROUPCACHE_HDR_RES)
        {
            // BAD REQUEST
            iomux_close(iomux, fd);
            return;
        }
        ctx->hdr = hdr;
        ctx->state = STATE_READING_KEY;
        fbuf_remove(ctx->input, 1);
    }

    for (;;) {
        char *chunk = fbuf_data(ctx->input);
        if (fbuf_used(ctx->input) < 2)
            break;

        uint16_t nlen;
        memcpy(&nlen, chunk, 2);
        uint16_t clen = ntohs(nlen);
        if (clen > 0) {
            if (fbuf_used(ctx->input) < 2+clen) {
                break;
            }
            chunk += 2;
            if (ctx->state == STATE_READING_KEY) {
                fbuf_add_binary(ctx->key, chunk, clen);
            } else if (ctx->state == STATE_READING_VALUE) {
                fbuf_add_binary(ctx->value, chunk, clen);
            }
            fbuf_remove(ctx->input, 2+clen);
        } else {
            if (ctx->state == STATE_READING_KEY) {
                if (ctx->hdr == GROUPCACHE_HDR_SET) {
                    ctx->state = STATE_READING_VALUE;
                } else {
                    ctx->state = STATE_READING_DONE;
                    break;
                }
            } else if (ctx->state == STATE_READING_VALUE) {
                ctx->state = STATE_READING_DONE;
                break;
            }
            fbuf_remove(ctx->input, 2);
        }
    }

    if (ctx->state == STATE_READING_DONE) {
        struct sockaddr saddr;
        socklen_t addr_len;
        getpeername(fd, &saddr, &addr_len);

        // we have a complete request so we can now start 
        // a background worker to take care of it
        pthread_t worker_thread;
        ctx->fd = fd;

        iomux_remove(iomux, fd); // this fd doesn't belong to the mux anymore
        shutdown(fd, SHUT_RD); // we don't want to read anymore from this socket

#ifdef GROUPCACHE_DEBUG
        fprintf(stderr, "Creating thread to serve request: (%d) %02x:%s\n", fd, ctx->cmd, fbuf_data(ctx->key));
#endif
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
    groupcache_serving_t *serv = (groupcache_serving_t *)priv;

    iomux_callbacks_t groupcache_callbacks = {
        .mux_connection = groupcache_connection_handler,
        .mux_input = NULL,
        .mux_eof = NULL,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = serv->cache
    };

    iomux_t *iomux = iomux_create();
    
    iomux_add(iomux, serv->sock, &groupcache_callbacks);
    iomux_listen(iomux, serv->sock);

    iomux_loop(iomux, 0);
    
    iomux_destroy(iomux);
    return NULL;
}


