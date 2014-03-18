#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <errno.h>
#include <iomux.h>
#include <queue.h>

#include "messaging.h"
#include "connections.h"
#include "shardcache.h"
#include "counters.h"
#include "rbuf.h"

#include "serving.h"

#include "atomic.h"

#include "shardcache_internal.h" // for the replica memeber

#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

typedef struct {
    pthread_t thread;
    queue_t *jobs;
    uint32_t busy;
    int leave;
    pthread_cond_t wakeup_cond;
    pthread_mutex_t wakeup_lock;
    shardcache_serving_t *serv;
} shardcache_worker_context_t;

struct __shardcache_serving_s {
    shardcache_t *cache;
    int sock;
    pthread_t io_thread;
    int leave;
    const char *auth;
    const char *me;
    int num_workers;
    int max_workers;
    queue_t *workers;
    queue_t *busy;
    int worker_index;
    uint32_t num_fds;
    uint32_t busy_workers;
    uint32_t spare_workers;
    uint32_t total_workers;
    shardcache_counters_t *counters;
};

typedef struct __shardcache_connection_context_s shardcache_connection_context_t;

typedef void (*shardcache_get_remainder_callback_t)(shardcache_connection_context_t *ctx);

struct __shardcache_connection_context_s {
    shardcache_hdr_t hdr;
    shardcache_hdr_t sig_hdr;

    pthread_mutex_t output_lock;
    int fetching;
    sip_hash *fetch_shash;
    rbuf_t *fetch_accumulator;
    int fetch_error;

    shardcache_serving_t *serv;

    fbuf_t *output;
    int fd;
#define SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX 4
    fbuf_t records[SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX];
    shardcache_get_remainder_callback_t get_remainder_cb;
    shardcache_worker_context_t *worker_ctx;
    async_read_ctx_t *reader_ctx;
    iomux_t *iomux;
};

static int
async_read_handler(void *data, size_t len, int idx, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    if (idx >= 0 && idx < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX)
        fbuf_add_binary(&ctx->records[idx], data, len);

    // idx == -1 means that reading finished 
    // idx == -2 means error
    // any idx >= 0 refers to the record index
    return (idx >= -1) ? 0 : -1;
}


static shardcache_connection_context_t *
shardcache_connection_context_create(shardcache_serving_t *serv, int fd, iomux_t *iomux)
{
    shardcache_connection_context_t *ctx =
        calloc(1, sizeof(shardcache_connection_context_t));

    ctx->output = fbuf_create(0);

    ctx->iomux = iomux;
    ctx->serv = serv;
    ctx->fd = fd;
    ctx->reader_ctx = async_read_context_create((char *)serv->auth,
                                                    async_read_handler,
                                                    ctx);
    MUTEX_INIT(&ctx->output_lock);
    ctx->fetch_accumulator = rbuf_create(1<<16);
    return ctx;
}

static void
shardcache_connection_context_destroy(shardcache_connection_context_t *ctx)
{
    fbuf_free(ctx->output);
    int i;
    for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++)
        fbuf_destroy(&ctx->records[i]);
    async_read_context_destroy(ctx->reader_ctx);
    rbuf_destroy(ctx->fetch_accumulator);
    MUTEX_DESTROY(&ctx->output_lock);
    free(ctx);
}

#define WRITE_STATUS_MODE_SIMPLE  0x00
#define WRITE_STATUS_MODE_BOOLEAN 0x01
#define WRITE_STATUS_MODE_EXISTS  0x02
static void
write_status(shardcache_connection_context_t *ctx, int rc, char mode)
{
    char out[6] = { 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 };

    if (rc == -1) {
        out[2] = SHC_RES_ERR;
    } else {
        if (mode == WRITE_STATUS_MODE_BOOLEAN) {
            if (rc == 1)
                out[2] = SHC_RES_YES;
            else if (rc == 0)
                out[2] = SHC_RES_NO;
            else
                out[2] = SHC_RES_ERR;
        } else if (mode == WRITE_STATUS_MODE_EXISTS && rc == 1) {
            out[2] = SHC_RES_EXISTS;
        }
        else if (rc == 0) {
            out[2] = SHC_RES_OK;
        } else {
            out[2] = SHC_RES_ERR;
        }
    }

    uint32_t magic = htonl(SHC_MAGIC);
    fbuf_add_binary(ctx->output, (char *)&magic, sizeof(magic));

    sip_hash *shash = NULL;
    if (ctx->serv->auth) {
        unsigned char hdr_sig = SHC_HDR_SIGNATURE_SIP;
        fbuf_add_binary(ctx->output, (char *)&hdr_sig, 1);
        shash = sip_hash_new((char *)ctx->serv->auth, 2, 4);
    }

    uint16_t initial_offset = fbuf_used(ctx->output);

    unsigned char hdr = SHC_HDR_RESPONSE;

    fbuf_add_binary(ctx->output, (char *)&hdr, 1);
    
    fbuf_add_binary(ctx->output, out, sizeof(out));

    if (ctx->serv->auth) {
        uint64_t digest;
        sip_hash_digest_integer(shash,
                                fbuf_data(ctx->output) + initial_offset,
                                fbuf_used(ctx->output) - initial_offset, &digest);
        fbuf_add_binary(ctx->output, (char *)&digest, sizeof(digest));
    }

    if (shash)
        sip_hash_free(shash);
}

static int get_async_data_handler(void *key,
                                   size_t klen,
                                   void *data,
                                   size_t dlen,
                                   size_t total_size,
                                   struct timeval *timestamp,
                                   void *priv)
{

    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    if (dlen == 0 && total_size == 0) {
        // error
        ATOMIC_INCREMENT(ctx->fetch_error);
        ATOMIC_SET(ctx->fetching, 0);
        if (ctx->fetch_shash) {
            sip_hash_free(ctx->fetch_shash);
            ctx->fetch_shash = NULL;
        }
        return -1;
    }
    static int max_chunk_size = (1<<16)-1;

    uint16_t accumulated_size = rbuf_len(ctx->fetch_accumulator);
    size_t to_process = accumulated_size + dlen;
    size_t data_offset = 0;
    while(to_process >= max_chunk_size) {
        pthread_mutex_lock(&ctx->output_lock);
        size_t copy_size = max_chunk_size;
        uint16_t clen = htons(max_chunk_size);
        fbuf_add_binary(ctx->output, (void *)&clen, sizeof(clen));
        if (ctx->fetch_shash)
            sip_hash_update(ctx->fetch_shash, (void *)&clen, sizeof(clen));

        if (accumulated_size) {
            char buf[accumulated_size];
            rbuf_read(ctx->fetch_accumulator, buf, accumulated_size);
            fbuf_add_binary(ctx->output, buf, accumulated_size);
            if (ctx->fetch_shash)
                sip_hash_update(ctx->fetch_shash, buf, accumulated_size);
            copy_size -= accumulated_size;
            accumulated_size = 0;
        }
        if (dlen - data_offset >= copy_size) {
            fbuf_add_binary(ctx->output, data + data_offset, copy_size);
            if (ctx->fetch_shash)
                sip_hash_update(ctx->fetch_shash, data + data_offset, copy_size);
            data_offset += copy_size;
        }
        if (ctx->fetch_shash && (ctx->sig_hdr&0x01)) {
            uint64_t digest;
            if (!sip_hash_final_integer(ctx->fetch_shash, &digest)) {
                SHC_ERROR("Can't compute the siphash digest!\n");
                sip_hash_free(ctx->fetch_shash);
                ctx->fetch_shash = NULL;
                pthread_mutex_unlock(&ctx->output_lock);
                ATOMIC_SET(ctx->fetching, 0);
                ATOMIC_INCREMENT(ctx->fetch_error);
                return -1;
            }
            fbuf_add_binary(ctx->output, (void *)&digest, sizeof(digest));
        }
        pthread_mutex_unlock(&ctx->output_lock);
        to_process = accumulated_size + (dlen - data_offset);
    }

    if (dlen > data_offset) {
        int remainder = dlen - data_offset;
        if (remainder) {
            rbuf_write(ctx->fetch_accumulator, data + data_offset, remainder);
            accumulated_size = remainder;
        }
    }
    
    if (total_size > 0 && timestamp) {
        uint16_t eor = 0;
        char eom = 0;
        pthread_mutex_lock(&ctx->output_lock);
        if (accumulated_size) {
            // flush what we have left in the accumulator
            uint16_t clen = htons(accumulated_size);
            fbuf_add_binary(ctx->output, (void *)&clen, sizeof(clen));
            if (ctx->fetch_shash)
                sip_hash_update(ctx->fetch_shash, (void *)&clen, sizeof(clen));
            char buf[accumulated_size];
            rbuf_read(ctx->fetch_accumulator, buf, accumulated_size);
            fbuf_add_binary(ctx->output, buf, accumulated_size);
            if (ctx->fetch_shash) {
                sip_hash_update(ctx->fetch_shash, buf, accumulated_size);
                if (ctx->sig_hdr&0x01) {
                    uint64_t digest;
                    if (!sip_hash_final_integer(ctx->fetch_shash, &digest)) {
                        SHC_ERROR("Can't compute the siphash digest!\n");
                        sip_hash_free(ctx->fetch_shash);
                        ctx->fetch_shash = NULL;
                        pthread_mutex_unlock(&ctx->output_lock);
                        ATOMIC_SET(ctx->fetching, 0);
                        ATOMIC_INCREMENT(ctx->fetch_error);
                        return -1;
                    }
                    fbuf_add_binary(ctx->output, (void *)&digest, sizeof(digest));
                }
            }
        }
        fbuf_add_binary(ctx->output, (void *)&eor, 2);
        fbuf_add_binary(ctx->output, &eom, 1);
        if (ctx->fetch_shash) {
            uint64_t digest;
            sip_hash_update(ctx->fetch_shash, (void *)&eor, 2);
            sip_hash_update(ctx->fetch_shash, &eom, 1);
            if (sip_hash_final_integer(ctx->fetch_shash, &digest)) {
                fbuf_add_binary(ctx->output, (void *)&digest, sizeof(digest));
            } else {
                SHC_ERROR("Can't compute the siphash digest!\n");
                ATOMIC_INCREMENT(ctx->fetch_error);
            }
            sip_hash_free(ctx->fetch_shash);
            ctx->fetch_shash = NULL;
        }

        ATOMIC_SET(ctx->fetching, 0);
        pthread_mutex_unlock(&ctx->output_lock);
    }

    return 0;
}

static void get_async_data(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            shardcache_get_async_callback_t cb,
                            shardcache_connection_context_t *ctx)
{
    ATOMIC_SET(ctx->fetching, 1);
    int rc = shardcache_get_async(cache, key, klen, cb, ctx);
    if (rc != 0) {
        uint16_t eor = 0;
        char eom = 0;
        pthread_mutex_lock(&ctx->output_lock);
        fbuf_add_binary(ctx->output, (void *)&eor, 2);
        fbuf_add_binary(ctx->output, &eom, 1);
        if (ctx->fetch_shash) {
            uint64_t digest;
            sip_hash_update(ctx->fetch_shash, (void *)&eor, 2);
            sip_hash_update(ctx->fetch_shash, &eom, 1);
            if (sip_hash_final_integer(ctx->fetch_shash, &digest)) {
                fbuf_add_binary(ctx->output, (void *)&digest, sizeof(digest));
            } else {
                SHC_ERROR("Can't compute the siphash digest!\n");
                ATOMIC_INCREMENT(ctx->fetch_error);
                sip_hash_free(ctx->fetch_shash);
                ctx->fetch_shash = NULL;
            }
            sip_hash_free(ctx->fetch_shash);
            ctx->fetch_shash = NULL;
        }
        pthread_mutex_unlock(&ctx->output_lock);
        ATOMIC_SET(ctx->fetching, 0);
    }
}

static void
shardcache_output_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    pthread_mutex_lock(&ctx->output_lock);
    if (fbuf_used(ctx->output)) {
        int wb = iomux_write(iomux, fd,
                            fbuf_data(ctx->output),
                            fbuf_used(ctx->output));
        fbuf_remove(ctx->output, wb);
    }

    if (!fbuf_used(ctx->output) && !ATOMIC_READ(ctx->fetching)) {
        iomux_callbacks_t *cbs = iomux_callbacks(iomux, fd);
        cbs->mux_output = NULL;
    }
    pthread_mutex_unlock(&ctx->output_lock);
}

static void *
process_request(void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;
    shardcache_t *cache = ctx->serv->cache;

    int rc = 0;
    void *key = fbuf_data(&ctx->records[0]);
    size_t klen = fbuf_used(&ctx->records[0]);

    switch(ctx->hdr) {
        case SHC_HDR_GET_OFFSET:
        {
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            char buf[1<<16];

            uint32_t offset = 0;
            if (fbuf_used(&ctx->records[1]) == 4) {
                memcpy(&offset, fbuf_data(&ctx->records[1]), sizeof(uint32_t));
                offset = ntohl(offset);
            } else {
                // TODO - Error Messages
                break;
            }

            uint32_t size = 0;
            if (fbuf_used(&ctx->records[2]) == 4) {
                memcpy(&size, fbuf_data(&ctx->records[2]), sizeof(uint32_t));
                size = ntohl(size);
            } else {
                // TODO - Error Messages
                break;
            }

            size_t bsize = size;
            uint32_t remainder = shardcache_get_offset(cache, key, klen, buf, &bsize, offset, NULL);
            if (size) {
                uint32_t remainder_nbo = htonl(remainder);
                shardcache_record_t record[2] = {
                    {
                        .v = buf,
                        .l = size
                    },
                    {
                        .v = &remainder_nbo,
                        .l = sizeof(remainder_nbo)
                    }
                };

                if (build_message((char *)ctx->serv->auth,
                                  ctx->sig_hdr,
                                  SHC_HDR_RESPONSE,
                                  record, 2, &out) == 0)
                {
                    fbuf_add_binary(ctx->output, fbuf_data(&out), fbuf_used(&out));
                }
                else
                {
                    // TODO - Error Messages
                    break;
                }
            }
            fbuf_destroy(&out);
            break;
        }
        case SHC_HDR_GET_ASYNC:
        {
            shardcache_hdr_t hdr = SHC_HDR_RESPONSE;

            uint32_t magic = htonl(SHC_MAGIC);
            fbuf_add_binary(ctx->output, (char *)&magic, sizeof(magic));

            if (ctx->serv->auth) {
                if (ctx->fetch_shash) {
                    sip_hash_free(ctx->fetch_shash);
                    ctx->fetch_shash = NULL;
                }
                ctx->fetch_shash = sip_hash_new((char *)ctx->serv->auth, 2, 4);
                fbuf_add_binary(ctx->output, (void *)&ctx->sig_hdr, 1);
            }

            fbuf_add_binary(ctx->output, (void *)&hdr, 1);

            if (ctx->serv->auth && ctx->fetch_shash) {
                sip_hash_update(ctx->fetch_shash, (char *)&hdr, 1);
                if (ctx->sig_hdr&0x01) {
                    uint64_t digest;
                    if (!sip_hash_final_integer(ctx->fetch_shash, &digest)) {
                        fbuf_set_used(ctx->output, fbuf_used(ctx->output) - 2);
                        SHC_ERROR("Can't compute the siphash digest!\n");
                        break;
                    }
                    fbuf_add_binary(ctx->output, (void *)&digest, sizeof(digest));
                }
         
            }

            get_async_data(cache, key, klen, get_async_data_handler, ctx);
            break;
        }
        case SHC_HDR_GET:
        {
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            size_t vlen = 0;
            void *v = shardcache_get(cache, key, klen, &vlen, NULL);
            shardcache_record_t record = {
                .v = v,
                .l = vlen
            };
            if (build_message((char *)ctx->serv->auth,
                              ctx->sig_hdr,
                              SHC_HDR_RESPONSE,
                              &record, 1, &out) == 0)
            {
                fbuf_add_binary(ctx->output, fbuf_data(&out), fbuf_used(&out));
            }
            else
            {
                // TODO - Error Messages
            }
            fbuf_destroy(&out);

            if (v)
                free(v);
            break;
        }
        case SHC_HDR_SET:
        {

            if (fbuf_used(&ctx->records[2]) == 4) {
                uint32_t expire;
                memcpy(&expire, fbuf_data(&ctx->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
                rc = shardcache_set_volatile(cache,
                                             key,
                                             klen,
                                             fbuf_data(&ctx->records[1]),
                                             fbuf_used(&ctx->records[1]),
                                             expire);

            } else {
                rc = shardcache_set(cache, key, klen,
                        fbuf_data(&ctx->records[1]), fbuf_used(&ctx->records[1]));
            }
            write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_ADD:
        {
            if (fbuf_used(&ctx->records[2]) == 4) {
                uint32_t expire;
                memcpy(&expire, fbuf_data(&ctx->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
                rc = shardcache_add_volatile(cache,
                                             key,
                                             klen,
                                             fbuf_data(&ctx->records[1]),
                                             fbuf_used(&ctx->records[1]),
                                             expire);

            } else {
                rc = shardcache_add(cache, key, klen,
                        fbuf_data(&ctx->records[1]), fbuf_used(&ctx->records[1]));
            }
            write_status(ctx, rc, WRITE_STATUS_MODE_EXISTS);
            break;
        }
        case SHC_HDR_EXISTS:
        {
            rc = shardcache_exists(cache, key, klen);
            write_status(ctx, rc, WRITE_STATUS_MODE_BOOLEAN);
            break;
        }
        case SHC_HDR_TOUCH:
        {
            rc = shardcache_touch(cache, key, klen);
            write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_DELETE:
        {
            rc = shardcache_del(cache, key, klen);
            write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_EVICT:
        {
            shardcache_evict(cache, key, klen);
            write_status(ctx, 0, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_BEGIN:
        {
            int num_shards = 0;
            shardcache_node_t **nodes = NULL;
            char *s = (char *)fbuf_data(&ctx->records[0]);
            while (s && *s) {
                char *tok = strsep(&s, ",");
                if(tok) {
                    char *label = strsep(&tok, ":");
                    char *addr = tok;
                    size_t size = (num_shards + 1) * sizeof(shardcache_node_t *);
                    nodes = realloc(nodes, size);
                    shardcache_node_t *node = shardcache_node_create(label, &addr, 1);
                    nodes[num_shards++] = node;
                } 
            }
            rc = shardcache_migration_begin(cache, nodes, num_shards, 0);
            if (rc != 0) {
                // TODO - Error messages
            }
            int i;
            for (i = 0; i < num_shards; i++)
                shardcache_node_destroy(nodes[i]);
            free(nodes);
            write_status(ctx, 0, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_ABORT:
        {
            rc = shardcache_migration_abort(cache);
            write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_END:
        {
            rc = shardcache_migration_end(cache);
            write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_CHECK:
        {
            // TODO - HEALTH CHECK
            write_status(ctx, 0, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_STATS:
        {
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            shardcache_counter_t *counters = NULL;
            int i, num_nodes;

            shardcache_node_t **nodes = shardcache_get_nodes(cache, &num_nodes);
            if (nodes) {
                fbuf_printf(&buf, "num_nodes;%d\r\nnodes;", num_nodes);
                for (i = 0; i < num_nodes; i++) {
                    if (i > 0)
                        fbuf_add(&buf, ",");
                    fbuf_printf(&buf, "%s", shardcache_node_get_string(nodes[i]));
                }
                fbuf_add(&buf, "\r\n");
                shardcache_free_nodes(nodes, num_nodes);
            }

            int ncounters = shardcache_get_counters(cache, &counters);
            if (counters) {
                for (i = 0; i < ncounters; i++) {
                    fbuf_printf(&buf, "%s;%u\r\n",
                                counters[i].name, counters[i].value);
                }

                fbuf_t out = FBUF_STATIC_INITIALIZER;
                shardcache_record_t record = {
                    .v = fbuf_data(&buf),
                    .l = fbuf_used(&buf)
                };
                if (build_message((char *)ctx->serv->auth,
                                  ctx->sig_hdr,
                                  SHC_HDR_RESPONSE,
                                  &record, 1, &out) == 0)
                {
                    fbuf_add_binary(ctx->output,
                            fbuf_data(&out), fbuf_used(&out));
                } else {
                    // TODO - Error Messages
                }
                fbuf_destroy(&out);
                free(counters);
            }
            fbuf_destroy(&buf);
            break;
        }
        case SHC_HDR_GET_INDEX:
        {
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            shardcache_storage_index_t *index = shardcache_get_index(cache);
            if (index) {
                int i;
                for (i = 0; i < index->size; i++) {
                    uint32_t klen = (uint32_t)index->items[i].klen;
                    uint32_t vlen = (uint32_t)index->items[i].vlen;
                    void *key = index->items[i].key;
                    uint32_t nklen = htonl(klen);
                    uint32_t nvlen = htonl(vlen);
                    fbuf_add_binary(&buf, (char *)&nklen, sizeof(nklen));
                    fbuf_add_binary(&buf, key, klen);
                    fbuf_add_binary(&buf, (char *)&nvlen, sizeof(nvlen));
                }
                size_t zero = 0;
                // no klen terminates the list
                fbuf_add_binary(&buf, (char *)&zero, sizeof(zero));

                shardcache_free_index(index); 
            }

            // chunkize the data and build an actual message
            shardcache_record_t record = {
                .v = fbuf_data(&buf),
                .l = fbuf_used(&buf)
            };
            if (build_message((char *)ctx->serv->auth,
                              ctx->sig_hdr,
                              SHC_HDR_INDEX_RESPONSE,
                              &record, 1, &out) == 0)
            {
                // destroy it early ... since we still need one more copy
                fbuf_destroy(&buf);
                fbuf_add_binary(ctx->output, fbuf_data(&out), fbuf_used(&out));
            } else {
                fbuf_destroy(&buf);
                // TODO - Error Messages
            }
            fbuf_destroy(&out);
            break;
        }
        case SHC_HDR_REPLICA_COMMAND:
        case SHC_HDR_REPLICA_PING:
        {
            void *response = NULL;
            size_t response_len = 0;
            if (!cache->replica) {
                write_status(ctx, -1, WRITE_STATUS_MODE_SIMPLE);
                break;
            }

            shardcache_hdr_t rhdr =
                shardcache_replica_received_command(cache->replica,
                                                    ctx->hdr,
                                                    fbuf_data(&ctx->records[0]),
                                                    fbuf_used(&ctx->records[0]),
                                                    &response,
                                                    &response_len);
            if (response_len) {
                fbuf_t out = FBUF_STATIC_INITIALIZER;
                shardcache_record_t record = {
                    .v = response,
                    .l = response_len
                };
                if (build_message((char *)ctx->serv->auth,
                                  ctx->sig_hdr, rhdr,
                                  &record, 1, &out) == 0)
                {
                    // destroy it early ... since we still need one more copy
                    free(response);
                    fbuf_add_binary(ctx->output, fbuf_data(&out), fbuf_used(&out));
                } else {
                    free(response);
                    // TODO - Error Messages
                }
                fbuf_destroy(&out);
            } else {
                write_status(ctx, rc, WRITE_STATUS_MODE_SIMPLE);
            }
            break;
        }
        default:
            fprintf(stderr, "Unsupported command: 0x%02x\n", (char)ctx->hdr);
            write_status(ctx, -1, WRITE_STATUS_MODE_SIMPLE);
            break;
    }

    return NULL;
}

static void * worker(void *priv);

static shardcache_worker_context_t *
shardcache_select_worker(shardcache_serving_t *serv)
{
    shardcache_worker_context_t *wrk = queue_pop_left(serv->workers);

    // skip over workers who are busy computing/serving a response
    if (!wrk) {
        // create a new worker
        wrk = calloc(1, sizeof(shardcache_worker_context_t));
        wrk->serv = serv;
        wrk->jobs = queue_create();
        queue_set_free_value_callback(wrk->jobs,
                (queue_free_value_callback_t)shardcache_connection_context_destroy);
        
        SHC_DEBUG("No idle worker found, spawning an extra worker %p", wrk);
        MUTEX_INIT(&wrk->wakeup_lock);
        CONDITION_INIT(&wrk->wakeup_cond);
        pthread_create(&wrk->thread, NULL, worker, wrk);
        ATOMIC_INCREMENT(serv->total_workers);
    }
    queue_push_right(serv->busy, wrk);

    return wrk;
}

static void
shardcache_dispose_worker(shardcache_serving_t *serv, shardcache_worker_context_t *wrk)
{
    if (queue_count(serv->workers) >= serv->max_workers) {
        ATOMIC_INCREMENT(wrk->leave);
        CONDITION_SIGNAL(&wrk->wakeup_cond, &wrk->wakeup_lock);
        pthread_join(wrk->thread, NULL);
        queue_destroy(wrk->jobs);
        MUTEX_DESTROY(&wrk->wakeup_lock);
        CONDITION_DESTROY(&wrk->wakeup_cond);
        SHC_DEBUG("Disposing worker %p", wrk);
        free(wrk);
        ATOMIC_DECREMENT(serv->total_workers);
        return;
    }
    queue_push_right(serv->workers, wrk);
}

static void
shardcache_input_handler(iomux_t *iomux,
                         int fd,
                         void *data,
                         int len,
                         void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    if (!ctx)
        return;

    if (ATOMIC_READ(ctx->fetching) != 0)
        return;


    // new data arrived so we want to run the asyncrhonous reader to update
    // the context
    async_read_context_input_data(data, len, ctx->reader_ctx);

    // if a complete message has been handled (so we are back to
    // SHC_STATE_READING_NONE) but we still have data in the read buffer,
    // it means that multiple commands were concatenated, so we need to run
    // the asynchronous reader again to consume the buffer until we can.
    int state = async_read_context_state(ctx->reader_ctx);
    if (state == SHC_STATE_READING_DONE) {

        ctx->hdr = async_read_context_hdr(ctx->reader_ctx);
        ctx->sig_hdr = async_read_context_sig_hdr(ctx->reader_ctx);


        // begin the worker slection process
        // first get the next worker in the array
        shardcache_worker_context_t *wrkctx = shardcache_select_worker(ctx->serv);

        if (wrkctx) {
            iomux_remove(iomux, fd);
            queue_push_right(wrkctx->jobs, ctx);
            CONDITION_SIGNAL(&wrkctx->wakeup_cond, &wrkctx->wakeup_lock);
        }

    }

    if (state == SHC_STATE_READING_ERR || state == SHC_STATE_AUTH_ERR) {
        // if the asynchronous reader is in error state we want
        // to close the connection, probably an unauthorized or a
        // badly formatted message has been sent by the client
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        SHC_DEBUG("Bad message %02x from %s (%d)",
                  ctx->hdr, inet_ntoa(saddr.sin_addr), state);
        iomux_close(iomux, fd);
    }
}

static void
shardcache_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    close(fd);

    if (ctx) {
        shardcache_connection_context_destroy(ctx);
    }
}

static void
shardcache_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

    shardcache_connection_context_t *ctx =
        shardcache_connection_context_create(serv, fd, iomux);

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = NULL,
        .mux_input = shardcache_input_handler,
        .mux_eof = shardcache_eof_handler,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = ctx
    };
    iomux_add(iomux, fd, &connection_callbacks);
}


static void *
worker(void *priv)
{
    shardcache_worker_context_t *wrkctx = (shardcache_worker_context_t *)priv;
    queue_t *jobs = wrkctx->jobs;

    while (ATOMIC_READ(wrkctx->leave) == 0) {
        shardcache_connection_context_t *ctx = queue_pop_left(jobs);

        ATOMIC_SET(wrkctx->busy, 1);
        ATOMIC_INCREMENT(wrkctx->serv->busy_workers);
        while(ctx) {
            int state = async_read_context_state(ctx->reader_ctx);
            while (state == SHC_STATE_READING_DONE) {
                process_request(ctx);
                int i;
                for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++)
                    fbuf_clear(&ctx->records[i]);

                if (ATOMIC_READ(ctx->fetching)) {
                    break;
                }

                // we might have received already data for the next command,
                // so let's run the asynchronous reader again to update
                // consume all pending data, if any, and to update the context 
                async_read_context_input_data(NULL, 0, ctx->reader_ctx);

                state = async_read_context_state(ctx->reader_ctx);
            }

            if (state != SHC_STATE_READING_ERR && state != SHC_STATE_AUTH_ERR) {
                iomux_callbacks_t connection_callbacks = {
                    .mux_connection = NULL,
                    .mux_input = shardcache_input_handler,
                    .mux_output = shardcache_output_handler,
                    .mux_timeout = NULL,
                    .priv = ctx
                };
                iomux_add(ctx->iomux, ctx->fd, &connection_callbacks);
            }
            ctx = queue_pop_left(jobs);
        }
        ATOMIC_SET(wrkctx->busy, 0);
        ATOMIC_DECREMENT(wrkctx->serv->busy_workers);
        
        // we don't have any filedescriptor to handle in the mux,
        // let's sit for 1 second waiting for the listener thread to wake
        // us up if new filedescriptors arrive
        struct timespec abstime;
        struct timeval now;
        int rc = 0;

        rc = gettimeofday(&now, NULL);
        if (rc == 0) {
            abstime.tv_sec = now.tv_sec + 1;
            abstime.tv_nsec = now.tv_usec * 1000;

            CONDITION_TIMEDWAIT(&wrkctx->wakeup_cond,
                                &wrkctx->wakeup_lock,
                                &abstime);

        } else {
            // TODO - Error messsages
        }
    }
    return NULL;


}

void *
serve_cache(void *priv)
{
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

    SHC_NOTICE("Listening on %s (num_workers: %d)",
               serv->me, serv->num_workers);

    if (listen(serv->sock, -1) != 0) {
        SHC_ERROR("Error listening on fd %d: %s",
                  serv->sock, strerror(errno));
        return NULL;
    }

    iomux_t *iomux = iomux_create();
    iomux_set_threadsafe(iomux, 1); 

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = shardcache_connection_handler,
        .mux_input = NULL,
        .mux_eof = NULL,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = serv
    };

    iomux_add(iomux, serv->sock, &connection_callbacks); iomux_listen(iomux,
            serv->sock);

    while (!ATOMIC_READ(serv->leave)) {
        struct timeval threshold = { 0, 20000 };
        struct timeval tv = { 0, 1000 };
        struct timeval before, now, diff;

        gettimeofday(&before, NULL);

        iomux_run(iomux, &tv);
        int i;
        for (i = 0; i < queue_count(serv->busy); i++) {
            shardcache_worker_context_t *wrk = (shardcache_worker_context_t *)queue_pop_left(serv->busy);
            if (wrk) {
                if (ATOMIC_READ(wrk->busy) == 0) {
                    shardcache_dispose_worker(serv, wrk);
                }
                else
                    queue_push_right(serv->busy, wrk);
            }
        }

        ATOMIC_SET(serv->spare_workers, queue_count(serv->workers));

        if (queue_count(serv->busy)) {
            gettimeofday(&now, NULL);
            timersub(&now, &before, &diff);
            if (timercmp(&diff, &threshold, <)) {
                timersub(&threshold, &diff, &diff);
                struct timespec ts = { 0, diff.tv_usec * 1000 };
                // we don't care if we are waken up by an interrupt
                nanosleep(&ts, NULL);
            }
        }
    }

    return NULL;
}

shardcache_serving_t *start_serving(shardcache_t *cache,
                                    const char *auth,
                                    const char *me,
                                    int num_workers,
                                    shardcache_counters_t *counters)
{
    shardcache_serving_t *s = calloc(1, sizeof(shardcache_serving_t));
    s->cache = cache;
    s->me = me;
    s->auth = auth;
    s->num_workers = s->max_workers = num_workers;
    s->counters = counters;

    // open the listening socket
    char *brkt = NULL;
    char *addr = strdup(me); // we need a temporary copy to be used by strtok
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : SHARDCACHE_PORT_DEFAULT;

    s->sock = open_socket(host, port);
    if (s->sock == -1) {
        fprintf(stderr, "Can't open listening socket %s:%d : %s\n",
                host, port, strerror(errno));
        free(addr);
        free(s);
        return NULL;
    }

    free(addr); // we don't need it anymore

    // create the workers' pool
    s->workers = queue_create();
    s->busy = queue_create();

    if (s->counters) {
        shardcache_counter_add(s->counters, "busy_workers", &s->busy_workers);
        shardcache_counter_add(s->counters, "num_fds", &s->num_fds);
        shardcache_counter_add(s->counters, "spare_workers", &s->spare_workers);
        shardcache_counter_add(s->counters, "total_workers", &s->total_workers);
    }

    int i;
    for (i = 0; i < num_workers; i++) {
        shardcache_worker_context_t *wrk = calloc(1, sizeof(shardcache_worker_context_t));
        wrk->serv = s;
        wrk->jobs = queue_create();
        queue_set_free_value_callback(wrk->jobs,
                (queue_free_value_callback_t)shardcache_connection_context_destroy);

        MUTEX_INIT(&wrk->wakeup_lock);
        CONDITION_INIT(&wrk->wakeup_cond);
        pthread_create(&wrk->thread, NULL, worker, wrk);
        queue_push_right(s->workers, wrk);
    }

    // and start a background thread to handle incoming connections
    int rc = pthread_create(&s->io_thread, NULL, serve_cache, s);
    if (rc != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(errno));
        stop_serving(s);
        return NULL;
    }

    return s;
}

void stop_serving(shardcache_serving_t *s) {

    // first stop the listener thread, so we won't get new connections
    ATOMIC_INCREMENT(s->leave);
    pthread_join(s->io_thread, NULL);

    SHC_NOTICE("Collecting worker threads (might have to wait until i/o is finished)");
    shardcache_worker_context_t *wrk = queue_pop_left(s->busy);
    while (wrk) {
        ATOMIC_INCREMENT(wrk->leave);
        queue_push_right(s->workers, wrk);
        wrk = queue_pop_left(s->busy);
    }
    queue_destroy(s->busy);
    // second pass
    wrk = queue_pop_left(s->workers);
    while (wrk) {
        ATOMIC_INCREMENT(wrk->leave);

        // wake up the worker if slacking
        CONDITION_SIGNAL(&wrk->wakeup_cond, &wrk->wakeup_lock);

        pthread_join(wrk->thread, NULL);

        queue_destroy(wrk->jobs);

        MUTEX_DESTROY(&wrk->wakeup_lock);
        CONDITION_DESTROY(&wrk->wakeup_cond);
        SHC_DEBUG3("Worker thread %p exited", wrk);
        wrk = queue_pop_left(s->workers);
    }
    SHC_DEBUG2("All worker threads have been collected");
    queue_destroy(s->workers);
    if (s->counters) {
        shardcache_counter_remove(s->counters, "busy_workers");
        shardcache_counter_remove(s->counters, "num_fds");
        shardcache_counter_remove(s->counters, "spare_workers");
        shardcache_counter_remove(s->counters, "total_workers");
    }

    close(s->sock);
    free(s);
}
