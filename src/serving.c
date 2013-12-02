#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>
#include <pthread.h>
#include <iomux.h>
#include <rbuf.h>
#include <linklist.h>

#include "messaging.h"
#include "connections.h"
#include "shardcache.h"

#include "serving.h"

#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

#define ATOMIC_READ(__v) __sync_fetch_and_add(&(__v), 0)

#define ATOMIC_INCREMENT(__v) __sync_add_and_fetch(&(__v), 1)

#define ATOMIC_DECREMENT(__v) __sync_sub_and_fetch(&(__v), 1)

#define ATOMIC_CAS(__v, __o, __n) __sync_bool_compare_and_swap(&(__v), __o, __n)

#define ATOMIC_SET(__v, __n) {\
    int __b = 0;\
    do {\
        __b = __sync_bool_compare_and_swap(&__v, ATOMIC_READ(__v), __n);\
    } while (!__b);\
}

typedef struct {
    pthread_t thread;
    linked_list_t *jobs;
    int busy;
    int num_fds;
    int leave;
    pthread_cond_t wakeup_cond;
    pthread_mutex_t wakeup_lock;
} shardcache_worker_context_t;

struct __shardcache_serving_s {
    shardcache_t *cache;
    int sock;
    pthread_t listener;
    const char *auth;
    const char *me;
    int num_workers;
    shardcache_worker_context_t *workers;
};

typedef struct {
    fbuf_t *input;
    fbuf_t *output;
    int fd;
    shardcache_t *cache;
    shardcache_hdr_t hdr;
    fbuf_t *key;
    fbuf_t *value;
#define STATE_READING_NONE  0x00
#define STATE_READING_KEY   0x01
#define STATE_READING_VALUE 0x02
#define STATE_READING_AUTH  0x03
#define STATE_READING_DONE  0x04
    char    state;
    const char    *auth;
    sip_hash *shash;
    shardcache_worker_context_t *worker_ctx;
} shardcache_connection_context_t;

static shardcache_connection_context_t *
shardcache_create_connection_context(shardcache_t *cache, const char *auth, int fd)
{
    shardcache_connection_context_t *context = calloc(1, sizeof(shardcache_connection_context_t));

    context->input = fbuf_create(0);
    context->output = fbuf_create(0);

    context->key = fbuf_create(0);
    context->value = fbuf_create(0);

    context->cache = cache;
    context->auth = auth;
    context->shash = sip_hash_new((uint8_t *)auth, 2, 4);
    context->fd = fd;
    return context;
}

static void shardcache_destroy_connection_context(shardcache_connection_context_t *ctx) {
    fbuf_free(ctx->input);
    fbuf_free(ctx->output);
    fbuf_free(ctx->key);
    fbuf_free(ctx->value);
    sip_hash_free(ctx->shash);
    free(ctx);
}

static void write_status(shardcache_connection_context_t *ctx, int rc) {
    if (rc != 0) {
        fprintf(stderr, "Error running command %d (key %s)\n", ctx->hdr, fbuf_data(ctx->key));
        write_message(ctx->fd, (char *)ctx->auth, SHARDCACHE_HDR_RES, "ERR", 3, NULL, 0);
    } else {
        write_message(ctx->fd, (char *)ctx->auth, SHARDCACHE_HDR_RES, "OK", 2, NULL, 0);
    }

}

static void *serve_response(void *priv) {
    shardcache_connection_context_t *ctx = (shardcache_connection_context_t *)priv;
    shardcache_t *cache = ctx->cache;
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

        case SHARDCACHE_HDR_GET:
        {
            size_t vlen = 0;
            void *v = shardcache_get(cache, key, klen, &vlen);
            write_message(fd, (char *)ctx->auth, SHARDCACHE_HDR_RES, v, vlen, NULL, 0);
            if (v)
                free(v);
            break;
        }
        case SHARDCACHE_HDR_SET:
        {
            rc = shardcache_set(cache, key, klen,
                    fbuf_data(ctx->value), fbuf_used(ctx->value));
            write_status(ctx, rc);
            break;
        }
        case SHARDCACHE_HDR_DEL:
        {
            rc = shardcache_del(cache, key, klen);
            write_status(ctx, rc);
            break;
        }
        case SHARDCACHE_HDR_EVI:
        {
            shardcache_evict(cache, key, klen);
            write_status(ctx, 0);
            break;
        }
        default:
            fprintf(stderr, "Unknown command: 0x%02x\n", (char)ctx->hdr);
            break;
    }

    close(fd);
    shardcache_destroy_connection_context(ctx);
    return NULL;
}

static void shardcache_input_handler(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    shardcache_connection_context_t *ctx = (shardcache_connection_context_t *)priv;
    if (!ctx)
        return;

    fbuf_add_binary(ctx->input, data, len);

    if (!fbuf_used(ctx->input) > 0)
        return;
    
    if (ctx->state == STATE_READING_NONE) {
        char *input = fbuf_data(ctx->input);
        char hdr = *input;

        if (hdr != SHARDCACHE_HDR_GET &&
            hdr != SHARDCACHE_HDR_SET &&
            hdr != SHARDCACHE_HDR_DEL &&
            hdr != SHARDCACHE_HDR_EVI &&
            hdr != SHARDCACHE_HDR_RES)
        {
            // BAD REQUEST
#ifdef SHARDCACHE_DEBUG
            struct sockaddr_in saddr;
            socklen_t addr_len = sizeof(struct sockaddr_in);
            getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
            fprintf(stderr, "BAD REQUEST from %s\n", inet_ntoa(saddr.sin_addr));
#endif
            iomux_close(iomux, fd);
            return;
        }
        ctx->hdr = hdr;
        ctx->state = STATE_READING_KEY;
        sip_hash_update(ctx->shash, &hdr, 1);
        fbuf_remove(ctx->input, 1);
    }

    for (;;) {
        unsigned char *chunk = fbuf_data(ctx->input);
        if (fbuf_used(ctx->input) < 2)
            break;

        uint16_t nlen;
        memcpy(&nlen, chunk, 2);
        uint16_t clen = ntohs(nlen);
        if (clen > 0) {
            if (fbuf_used(ctx->input) < 2+clen) {
                // TRUNCATED - we need more data
                break;
            }
            chunk += 2;
            if (ctx->state == STATE_READING_KEY) {
                fbuf_add_binary(ctx->key, chunk, clen);
            } else if (ctx->state == STATE_READING_VALUE) {
                fbuf_add_binary(ctx->value, chunk, clen);
            }
            sip_hash_update(ctx->shash, fbuf_data(ctx->input), 2+clen);
            fbuf_remove(ctx->input, 2+clen);
        } else {
            if (fbuf_used(ctx->input) < 3) {
                // TRUNCATED - we need more data
                break;
            }
            sip_hash_update(ctx->shash, fbuf_data(ctx->input), 2);
            fbuf_remove(ctx->input, 2);
            chunk = fbuf_data(ctx->input);
            if (*chunk == SHARDCACHE_RSEP) {
                chunk++;
                sip_hash_update(ctx->shash, fbuf_data(ctx->input), 1);
                fbuf_remove(ctx->input, 1);
                if (ctx->state == STATE_READING_KEY) {
                    if (ctx->hdr == SHARDCACHE_HDR_SET) {
                        ctx->state = STATE_READING_VALUE;
                    } else {
                        // BAD FORMAT - Ignore
                        // we don't support multiple records if the message is not SET
                        iomux_close(iomux, fd);
                        return;
                    }
                } else if (ctx->state == STATE_READING_VALUE) {
                    ctx->state = STATE_READING_AUTH;
                    break;
                }
            } else if (*chunk == 0) {
                sip_hash_update(ctx->shash, fbuf_data(ctx->input), 1);
                fbuf_remove(ctx->input, 1);
                ctx->state = STATE_READING_AUTH;
            } else {
                // BAD FORMAT
                break;
            }
        }
    }

    if (ctx->state == STATE_READING_AUTH) {
        if (fbuf_used(ctx->input) < SHARDCACHE_MSG_SIG_LEN)
            return;

        uint64_t digest;
        size_t dlen = sizeof(digest);
        if (!sip_hash_final_integer(ctx->shash, &digest)) {
            // TODO - Error Messages
            iomux_close(iomux, fd);
            return;
        }

#ifdef SHARDCACHE_DEBUG
        int i;
        printf("computed digest for received data: ");
        for (i=0; i<8; i++) {
            printf("%02x", (unsigned char)((char *)&digest)[i]);
        }
        printf("\n");

        printf("digest from received data: ");
        uint8_t *remote = fbuf_data(ctx->input);
        for (i=0; i<8; i++) {
            printf("%02x", remote[i]);
        }
        printf("\n");
#endif
        if (memcmp(&digest, (uint8_t *)fbuf_data(ctx->input), sizeof(digest)) != 0) {
            // AUTH FAILED
            struct sockaddr_in saddr;
            socklen_t addr_len = sizeof(struct sockaddr_in);
            getpeername(fd, (struct sockaddr *)&saddr, &addr_len);

            fprintf(stderr, "Unauthorized request from %s\n",
                    inet_ntoa(saddr.sin_addr));

            iomux_close(iomux, fd);
            return;
        }
        ctx->state = STATE_READING_DONE;
        fbuf_remove(ctx->input, dlen);
    }

    if (ctx->state == STATE_READING_DONE) {
#if 0
        // we have a complete request so we can now start 
        // a background worker to take care of it
        pthread_t request_thread;
        ctx->fd = fd;

        iomux_remove(iomux, fd); // this fd doesn't belong to the mux anymore
        shutdown(fd, SHUT_RD); // we don't want to read anymore from this socket

#ifdef SHARDCACHE_DEBUG
        fprintf(stderr, "Creating thread to serve request: (%d) %02x:%s\n", fd, ctx->hdr, fbuf_data(ctx->key));
#endif
        pthread_create(&request_thread, NULL, serve_response, ctx);
        pthread_detach(request_thread);
#else
        shardcache_worker_context_t *wrkctx = ctx->worker_ctx;
        ATOMIC_CAS(wrkctx->busy, 0, 1);
        iomux_remove(iomux, fd); // this fd doesn't belong to the mux anymore
        shutdown(fd, SHUT_RD); // we don't want to read anymore from this socket
        serve_response(ctx);
        ATOMIC_CAS(wrkctx->busy, 1, 0);
        ATOMIC_DECREMENT(wrkctx->num_fds);
#endif
    }
}

static void shardcache_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx = (shardcache_connection_context_t *)priv;
    close(fd);
    if (ctx) {
        shardcache_worker_context_t *wrkctx = ctx->worker_ctx;
        ATOMIC_DECREMENT(wrkctx->num_fds);
        shardcache_destroy_connection_context(ctx);
    }
}

void *worker(void *priv) {
    shardcache_worker_context_t *wrk_ctx = (shardcache_worker_context_t *)priv;
    linked_list_t *jobs = wrk_ctx->jobs;

    iomux_t *iomux = iomux_create();
    while (ATOMIC_READ(wrk_ctx->leave) == 0) {
        shardcache_connection_context_t *ctx = shift_value(jobs);
        while(ctx) {
            iomux_callbacks_t connection_callbacks = {
                .mux_connection = NULL,
                .mux_input = shardcache_input_handler,
                .mux_eof = shardcache_eof_handler,
                .mux_output = NULL,
                .mux_timeout = NULL,
                .priv = ctx
            };
            ctx->worker_ctx = wrk_ctx;
            iomux_add(iomux, ctx->fd, &connection_callbacks);
            ATOMIC_INCREMENT(wrk_ctx->num_fds);
            ctx = shift_value(jobs);
        }
        pthread_testcancel();
        if (!iomux_isempty(iomux)) {
            struct timeval timeout = { 0, 1000 };
            iomux_run(iomux, &timeout);
        } else {
            struct timespec abstime;
            struct timeval now;
            int rc = 0;

            // reset the num_fds
            ATOMIC_SET(wrk_ctx->num_fds, 0);

            rc = gettimeofday(&now, NULL);
            if (rc == 0) {
                abstime.tv_sec = now.tv_sec + 1;
                abstime.tv_nsec = now.tv_usec * 1000;
                pthread_mutex_lock(&wrk_ctx->wakeup_lock);
                pthread_cond_timedwait(&wrk_ctx->wakeup_cond, &wrk_ctx->wakeup_lock, &abstime);
                pthread_mutex_unlock(&wrk_ctx->wakeup_lock);
            } else {
                // TODO - Error messsages
            }
        }
    }
    iomux_destroy(iomux);
    return NULL;
}

void *serve_cache(void *priv) {
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Listening on %s (num_workers: %d)\n", serv->me, serv->num_workers);
#endif

    if (listen(serv->sock, -1) != 0) {
        fprintf(stderr, "%s: Error listening on fd %d: %s", __FUNCTION__, serv->sock, strerror(errno));
        return NULL;
    }

    int idx = 0;
    for (;;) {
        struct sockaddr peer_addr = { 0 };
        socklen_t addr_len = 0;

        pthread_testcancel();

        int fd = accept(serv->sock, &peer_addr, &addr_len);
        if (fd >= 0) {
            // begin the worker slection process
            // first get the next worker in the array
            shardcache_worker_context_t *wrkctx = &serv->workers[idx++ % serv->num_workers];
            shardcache_worker_context_t *freemost_worker = NULL;
            int cnt = 0;

            // skip over workers who are busy computing/serving a response
            while (ATOMIC_READ(wrkctx->busy) != 0) {

                // update the ponter to the freemost worker, we might need it 
                // if all workers are busy
                if (!freemost_worker)
                    freemost_worker = wrkctx;
                else if (ATOMIC_READ(wrkctx->num_fds) < ATOMIC_READ(freemost_worker->num_fds))
                    freemost_worker = wrkctx;

                wrkctx = &serv->workers[idx++ % serv->num_workers];
                // well ...if we've gone through the whole list and all threads are busy)
                // let's queue this filedescriptor to the one with the least queued  fildescriptors
                if (++cnt == serv->num_workers) {
                    wrkctx = freemost_worker;
                    break;
                }
            }
            // now we have selected a worker, let's see if it makes sense to go ahead
            pthread_testcancel();

            // create and initialize the context for the new connection
            shardcache_connection_context_t *ctx = shardcache_create_connection_context(serv->cache, serv->auth, fd);
            push_value(wrkctx->jobs, ctx);
            pthread_mutex_lock(&wrkctx->wakeup_lock);
            pthread_cond_signal(&wrkctx->wakeup_cond);
            pthread_mutex_unlock(&wrkctx->wakeup_lock);
        }
    }
 
    return NULL;
}

shardcache_serving_t *start_serving(shardcache_t *cache, const char *auth, const char *me, int num_workers) {
    shardcache_serving_t *s = calloc(1, sizeof(shardcache_serving_t));
    s->cache = cache;
    s->me = me;
    s->auth = auth;
    s->num_workers = num_workers;

    // open the listening socket
    char *brkt = NULL;
    char *addr = strdup(me); // we need a temporary copy to be used by strtok
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : SHARDCACHE_PORT_DEFAULT;

    s->sock = open_socket(host, port);
    if (s->sock == -1) {
        fprintf(stderr, "Can't open listening socket %s:%d : %s\n", host, port, strerror(errno));
        free(s);
        return NULL;
    }

    free(addr); // we don't need it anymore
    
    // create the workers' pool
    s->workers = calloc(num_workers, sizeof(shardcache_worker_context_t));
    
    int i;
    for (i = 0; i < num_workers; i++) {
        s->workers[i].jobs = create_list();
        pthread_mutex_init(&s->workers[i].wakeup_lock, NULL);
        pthread_cond_init(&s->workers[i].wakeup_cond, NULL);
        pthread_create(&s->workers[i].thread, NULL, worker, &s->workers[i]);
    }

    // and start a background thread to handle incoming connections
    int rc = pthread_create(&s->listener, NULL, serve_cache, s);
    if (rc != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(errno));
        stop_serving(s);
        return NULL;
    }

    return s;
}

void stop_serving(shardcache_serving_t *s) {
    int i;

    pthread_cancel(s->listener);
    pthread_join(s->listener, NULL);
#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Collecting worker threads (might have to wait until i/o is finished)\n");
#endif
    for (i = 0; i < s->num_workers; i++) {
        ATOMIC_INCREMENT(s->workers[i].leave);

        // wake up the worker if slacking
        pthread_mutex_lock(&s->workers[i].wakeup_lock);
        pthread_cond_signal(&s->workers[i].wakeup_cond);
        pthread_mutex_unlock(&s->workers[i].wakeup_lock);

        pthread_join(s->workers[i].thread, NULL);

        destroy_list(s->workers[i].jobs);

        pthread_mutex_destroy(&s->workers[i].wakeup_lock);
        pthread_cond_destroy(&s->workers[i].wakeup_cond);
    }
    free(s->workers);
    close(s->sock);
    free(s);
}
