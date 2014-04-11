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
#include <linklist.h>
#include <hashtable.h>

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

#define SHARDCACHE_SERVING_MAX_RETRIES 10 

typedef struct {
    pthread_t thread;
    queue_t *jobs;
    int leave;
    pthread_cond_t wakeup_cond;
    pthread_mutex_t wakeup_lock;
    shardcache_serving_t *serv;
    //hashtable_t *fds;
    iomux_t *iomux;
    linked_list_t *prune;
    uint32_t numfds;
    uint32_t pruning;
} shardcache_worker_context_t;

struct __shardcache_serving_s {
    shardcache_t *cache;
    int sock;
    pthread_t io_thread;
    iomux_t *io_mux;
    int leave;
    const char *auth;
    const char *me;
    int num_workers;
    int next_worker_index;
    linked_list_t *workers;
    int worker_index;
    uint32_t num_connections;
    uint32_t busy_workers;
    uint32_t spare_workers;
    uint32_t total_workers;
    shardcache_counters_t *counters;
};

typedef struct __shardcache_connection_context_s shardcache_connection_context_t;

typedef struct {
#define SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX 4
    fbuf_t records[SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX];
    int fd;
    shardcache_hdr_t hdr;
    shardcache_hdr_t sig_hdr;
    shardcache_connection_context_t *ctx;
    fbuf_t output;
    sip_hash *fetch_shash;
    int fetch_error;
    int skipped;
    int copied;
    int done;
    pthread_mutex_t lock;
    rbuf_t *fetch_accumulator;
} shardcache_request_t;

struct __shardcache_connection_context_s {
    shardcache_hdr_t hdr;
    shardcache_hdr_t sig_hdr;

    int request_number;
    linked_list_t *requests;

#define SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX 4
    fbuf_t records[SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX];

    shardcache_serving_t *serv;

    rbuf_t *output;
    int fd;
    shardcache_worker_context_t *worker_ctx;
    async_read_ctx_t *reader_ctx;
    int retries;
    struct timeval retry_timeout;
    rbuf_t *input;
    shardcache_worker_context_t *worker;
    int closed;
    struct timeval in_prune_since;
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
shardcache_connection_context_create(shardcache_serving_t *serv, int fd)
{
    shardcache_connection_context_t *ctx =
        calloc(1, sizeof(shardcache_connection_context_t));

    ctx->input = rbuf_create(1<<16);

    ctx->serv = serv;
    ctx->fd = fd;
    ctx->reader_ctx = async_read_context_create((char *)serv->auth,
                                                    async_read_handler,
                                                    ctx);
    ctx->requests = create_list();
    return ctx;
}

static void
shardcache_request_destroy(shardcache_request_t *req)
{
    int i;
    for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++) {
        fbuf_destroy(&req->records[i]);
    }
    fbuf_destroy(&req->output);
    if (req->fetch_shash)
        sip_hash_free(req->fetch_shash);
    MUTEX_DESTROY(&req->lock);
    rbuf_destroy(req->fetch_accumulator);
    free(req);
}

static void
shardcache_connection_context_destroy(shardcache_connection_context_t *ctx)
{
    rbuf_destroy(ctx->input);
    int i;
    for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++) {
        fbuf_destroy(&ctx->records[i]);
    }
    shardcache_request_t *req = shift_value(ctx->requests);
    while(req) {
        shardcache_request_destroy(req);
        req = shift_value(ctx->requests);
    }
    destroy_list(ctx->requests);
    async_read_context_destroy(ctx->reader_ctx);
    free(ctx);
}

#define WRITE_STATUS_MODE_SIMPLE  0x00
#define WRITE_STATUS_MODE_BOOLEAN 0x01
#define WRITE_STATUS_MODE_EXISTS  0x02
static void
write_status(shardcache_request_t *req, int rc, char mode)
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

    MUTEX_LOCK(&req->lock);
    uint32_t magic = htonl(SHC_MAGIC);
    fbuf_add_binary(&req->output, (char *)&magic, sizeof(magic));

    sip_hash *shash = NULL;
    if (req->ctx->serv->auth) {
        unsigned char hdr_sig = SHC_HDR_SIGNATURE_SIP;
        fbuf_add_binary(&req->output, (char *)&hdr_sig, 1);
        shash = sip_hash_new((char *)req->ctx->serv->auth, 2, 4);
    }

    uint16_t initial_offset = fbuf_used(&req->output);

    unsigned char hdr = SHC_HDR_RESPONSE;

    fbuf_add_binary(&req->output, (char *)&hdr, 1);
    
    fbuf_add_binary(&req->output, out, sizeof(out));

    if (req->ctx->serv->auth) {
        uint64_t digest;
        sip_hash_digest_integer(shash,
                                fbuf_data(&req->output) + initial_offset,
                                fbuf_used(&req->output) - initial_offset, &digest);
        fbuf_add_binary(&req->output, (char *)&digest, sizeof(digest));
    }

    if (shash)
        sip_hash_free(shash);

    req->done = 1;
    MUTEX_UNLOCK(&req->lock);
}

static int get_async_data_handler(void *key,
                                   size_t klen,
                                   void *data,
                                   size_t dlen,
                                   size_t total_size,
                                   struct timeval *timestamp,
                                   void *priv)
{

    shardcache_request_t *req =
        (shardcache_request_t *)priv;

    MUTEX_LOCK(&req->lock);

    if (dlen == 0 && total_size == 0) {
        if (!timestamp) {
            // if there is no timestamp here it means there was an
            // error (and not just an empty item)
            MUTEX_UNLOCK(&req->lock);
            write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
            return -1;
        }
        uint16_t eor = 0;
        char eom = 0;
        fbuf_add_binary(&req->output, (void *)&eor, 2);
        fbuf_add_binary(&req->output, &eom, 1);
        if (req->fetch_shash) {
            uint64_t digest;
            sip_hash_update(req->fetch_shash, (void *)&eor, 2);
            sip_hash_update(req->fetch_shash, &eom, 1);
            if (sip_hash_final_integer(req->fetch_shash, &digest)) {
                fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
            } else {
                SHC_ERROR("Can't compute the siphash digest!\n");
                req->fetch_error = 1;
            }
            sip_hash_free(req->fetch_shash);
            req->fetch_shash = NULL;
        }

        req->done = 1;
        MUTEX_UNLOCK(&req->lock);
        return 0;
    }

    uint32_t offset = 0;
    uint32_t size = 0;

    if (req->hdr == SHC_HDR_GET_OFFSET) {
        if (fbuf_used(&req->records[1]) == 4) {
            memcpy(&offset, fbuf_data(&req->records[1]), sizeof(uint32_t));
            offset = ntohl(offset);
        } else {
            SHC_WARNING("Bad record (1) format for message GET_OFFSET");
            MUTEX_UNLOCK(&req->lock);
            write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
            return -1;
        }

        if (fbuf_used(&req->records[2]) == 4) {
            memcpy(&size, fbuf_data(&req->records[2]), sizeof(uint32_t));
            size = ntohl(size);
        } else {
            SHC_WARNING("Bad record (1) format for message GET_OFFSET");
            MUTEX_UNLOCK(&req->lock);
            write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
            return -1;
        }
    }

    if (offset && (req->skipped + dlen) < offset) {
        req->skipped += dlen;
        MUTEX_UNLOCK(&req->lock);
        return 0;
    }

    static int max_chunk_size = (1<<16)-1;

    uint16_t accumulated_size = rbuf_used(req->fetch_accumulator);
    size_t to_process = accumulated_size + dlen;
    size_t data_offset = 0;
    while(to_process >= max_chunk_size) {
        size_t copy_size = max_chunk_size;

        uint16_t clen = htons((uint16_t)copy_size);

        fbuf_add_binary(&req->output, (void *)&clen, sizeof(clen));

        if (req->fetch_shash)
            sip_hash_update(req->fetch_shash, (void *)&clen, sizeof(clen));

        if (accumulated_size) {
            char buf[accumulated_size];
            rbuf_read(req->fetch_accumulator, buf, accumulated_size);
            fbuf_add_binary(&req->output, buf, accumulated_size);
            if (req->fetch_shash)
                sip_hash_update(req->fetch_shash, buf, accumulated_size);
            copy_size -= accumulated_size;
            accumulated_size = 0;
        }
        if (dlen - data_offset >= copy_size) {
            fbuf_add_binary(&req->output, data + data_offset, copy_size);
            if (req->fetch_shash)
                sip_hash_update(req->fetch_shash, data + data_offset, copy_size);
            data_offset += copy_size;
        }
        if (req->fetch_shash && (req->sig_hdr&0x01)) {
            uint64_t digest;
            if (!sip_hash_final_integer(req->fetch_shash, &digest)) {
                SHC_ERROR("Can't compute the siphash digest!\n");
                sip_hash_free(req->fetch_shash);
                req->fetch_shash = NULL;
                req->done = 1;
                req->fetch_error = 1;
                MUTEX_UNLOCK(&req->lock);
                return -1;
            }
            fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
        }
        to_process = accumulated_size + (dlen - data_offset);
    }

    if (dlen > data_offset) {
        int remainder = dlen - data_offset;
        if (remainder) {
            rbuf_write(req->fetch_accumulator, data + data_offset, remainder);
            accumulated_size = remainder;
        }
    }
    
    if (total_size > 0 && timestamp) {
        uint16_t eor = 0;
        char eom = 0;
        if (accumulated_size) {
            // flush what we have left in the accumulator
            uint16_t clen = htons(accumulated_size);
            fbuf_add_binary(&req->output, (void *)&clen, sizeof(clen));
            if (req->fetch_shash)
                sip_hash_update(req->fetch_shash, (void *)&clen, sizeof(clen));
            char buf[accumulated_size];
            rbuf_read(req->fetch_accumulator, buf, accumulated_size);
            fbuf_add_binary(&req->output, buf, accumulated_size);
            if (req->fetch_shash) {
                sip_hash_update(req->fetch_shash, buf, accumulated_size);
                if (req->sig_hdr&0x01) {
                    uint64_t digest;
                    if (!sip_hash_final_integer(req->fetch_shash, &digest)) {
                        SHC_ERROR("Can't compute the siphash digest!\n");
                        sip_hash_free(req->fetch_shash);
                        req->fetch_shash = NULL;
                        req->done = 1;
                        req->fetch_error = 1;
                        MUTEX_UNLOCK(&req->lock);
                        return -1;
                    }
                    fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
                }
            }
        }
        fbuf_add_binary(&req->output, (void *)&eor, 2);
        fbuf_add_binary(&req->output, &eom, 1);
        if (req->fetch_shash) {
            uint64_t digest;
            sip_hash_update(req->fetch_shash, (void *)&eor, 2);
            sip_hash_update(req->fetch_shash, &eom, 1);
            if (sip_hash_final_integer(req->fetch_shash, &digest)) {
                fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
            } else {
                req->fetch_error = 1;
                SHC_ERROR("Can't compute the siphash digest!\n");
            }
            sip_hash_free(req->fetch_shash);
            req->fetch_shash = NULL;
        }

        req->done = 1;
    }

    MUTEX_UNLOCK(&req->lock);
    return 0;
}

static void get_async_data(shardcache_t *cache,
                            void *key,
                            size_t klen,
                            shardcache_get_async_callback_t cb,
                            shardcache_request_t *req)
{
    int rc;
    
    if (req->hdr == SHC_HDR_GET_OFFSET) {
        uint32_t offset = ntohl(*((uint32_t *)fbuf_data(&req->records[1])));
        uint32_t length = ntohl(*((uint32_t *)fbuf_data(&req->records[2])));
        rc = shardcache_get_offset_async(cache, key, klen, offset, length, cb, req);
    } else {
        rc = shardcache_get_async(cache, key, klen, cb, req);
    }
    if (rc != 0) {
        uint16_t eor = 0;
        char eom = 0;
        MUTEX_LOCK(&req->lock);
        fbuf_add_binary(&req->output, (void *)&eor, 2);
        fbuf_add_binary(&req->output, &eom, 1);
        if (req->fetch_shash) {
            uint64_t digest;
            sip_hash_update(req->fetch_shash, (void *)&eor, 2);
            sip_hash_update(req->fetch_shash, &eom, 1);
            if (sip_hash_final_integer(req->fetch_shash, &digest)) {
                fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
            } else {
                SHC_ERROR("Can't compute the siphash digest!\n");
		req->fetch_error = 1;
                sip_hash_free(req->fetch_shash);
                req->fetch_shash = NULL;
            }
            sip_hash_free(req->fetch_shash);
            req->fetch_shash = NULL;
        }
        req->done = 1;
        MUTEX_UNLOCK(&req->lock);
    }
}

static void
shardcache_async_command_response(void *key, size_t klen, int ret, void *priv)
{
    shardcache_request_t *req = (shardcache_request_t *)priv;

    write_status(req, ret, (req->hdr == SHC_HDR_ADD)
                           ? WRITE_STATUS_MODE_EXISTS
                           : (req->hdr == SHC_HDR_EXISTS)
                             ? WRITE_STATUS_MODE_BOOLEAN
                             : WRITE_STATUS_MODE_SIMPLE);
}

static void
process_request(shardcache_request_t *req)
{

    shardcache_t *cache = req->ctx->serv->cache; //XXX

    int rc = 0;
    void *key = fbuf_data(&req->records[0]);
    size_t klen = fbuf_used(&req->records[0]);

    switch(req->hdr) {
        case SHC_HDR_GET:
        case SHC_HDR_GET_ASYNC:
        case SHC_HDR_GET_OFFSET:
        {
            shardcache_hdr_t hdr = SHC_HDR_RESPONSE;

            uint32_t magic = htonl(SHC_MAGIC);

            fbuf_add_binary(&req->output, (char *)&magic, sizeof(magic));

            if (req->ctx->serv->auth) {
                if (req->fetch_shash) {
                    sip_hash_free(req->fetch_shash);
                    req->fetch_shash = NULL;
                }
                req->fetch_shash = sip_hash_new((char *)req->ctx->serv->auth, 2, 4);
                fbuf_add_binary(&req->output, (void *)&req->sig_hdr, 1);
            }

            fbuf_add_binary(&req->output, (void *)&hdr, 1);

            if (req->ctx->serv->auth && req->fetch_shash) {
                sip_hash_update(req->fetch_shash, (char *)&hdr, 1);
                if (req->sig_hdr&0x01) {
                    uint64_t digest;
                    if (!sip_hash_final_integer(req->fetch_shash, &digest)) {
                        fbuf_set_used(&req->output, fbuf_used(&req->output) - 2);
                        SHC_ERROR("Can't compute the siphash digest!\n");
                        break;
                    }
                    fbuf_add_binary(&req->output, (void *)&digest, sizeof(digest));
                }
         
            }
            get_async_data(cache, key, klen, get_async_data_handler, req);
            break;
        }
        case SHC_HDR_ADD:
        case SHC_HDR_SET:
        {
            uint32_t expire = 0;
            if (fbuf_used(&req->records[2]) == 4) {
                memcpy(&expire, fbuf_data(&req->records[2]), sizeof(uint32_t));
                expire = ntohl(expire);
            }
            shardcache_set_async(cache, key, klen,
                                 fbuf_data(&req->records[1]),
                                 fbuf_used(&req->records[1]),
                                 expire,
                                 req->hdr == SHC_HDR_SET ? 0 : 1,
                                 shardcache_async_command_response,
                                 req);
            break;
        }
        case SHC_HDR_EXISTS:
        {
            shardcache_exists_async(cache, key, klen, shardcache_async_command_response, req);
            break;
        }
        case SHC_HDR_TOUCH:
        {
            rc = shardcache_touch(cache, key, klen);
            write_status(req, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_DELETE:
        {
            shardcache_del_async(cache, key, klen, shardcache_async_command_response, req);
            break;
        }
        case SHC_HDR_EVICT:
        {
            shardcache_evict(cache, key, klen);
            write_status(req, 0, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_BEGIN:
        {
            int num_shards = 0;
            shardcache_node_t **nodes = NULL;
            char *s = (char *)fbuf_data(&req->records[0]);
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
            if (rc != 0)
                SHC_WARNING("Can't begin the migration");
            int i;
            for (i = 0; i < num_shards; i++)
                shardcache_node_destroy(nodes[i]);
            free(nodes);
            write_status(req, 0, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_ABORT:
        {
            rc = shardcache_migration_abort(cache);
            if (rc != 0)
                SHC_WARNING("Can't abort the migration");
            write_status(req, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_MIGRATION_END:
        {
            rc = shardcache_migration_end(cache);
            if (rc != 0)
                SHC_WARNING("Can't end the migration");
            write_status(req, rc, WRITE_STATUS_MODE_SIMPLE);
            break;
        }
        case SHC_HDR_CHECK:
        {
            // TODO - HEALTH CHECK
            write_status(req, 0, WRITE_STATUS_MODE_SIMPLE);
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
                if (build_message((char *)req->ctx->serv->auth,
                                  req->sig_hdr,
                                  SHC_HDR_RESPONSE,
                                  &record, 1, &out) == 0)
                {
                    MUTEX_LOCK(&req->lock);
                    fbuf_add_binary(&req->output,
                            fbuf_data(&out), fbuf_used(&out));
                    MUTEX_UNLOCK(&req->lock);
                } else {
                    SHC_ERROR("Can't build the STATS response");
                }
                fbuf_destroy(&out);
                free(counters);
            }
            fbuf_destroy(&buf);
            MUTEX_LOCK(&req->lock);
            req->done = 1;
            MUTEX_UNLOCK(&req->lock);
            break;
        }
        case SHC_HDR_GET_INDEX:
        {
            fbuf_t buf = FBUF_STATIC_INITIALIZER;
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            SHC_DEBUG("Fetching index");
            shardcache_storage_index_t *index = shardcache_get_index(cache);
            SHC_DEBUG("Index got");
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
            if (build_message((char *)req->ctx->serv->auth,
                              req->sig_hdr,
                              SHC_HDR_INDEX_RESPONSE,
                              &record, 1, &out) == 0)
            {
                // destroy it early ... since we still need one more copy
                MUTEX_LOCK(&req->lock);
                fbuf_add_binary(&req->output, fbuf_data(&out), fbuf_used(&out));
                req->done = 1;
                SHC_DEBUG("Index response sent (%d)", fbuf_used(&req->output));
                MUTEX_UNLOCK(&req->lock);
            } else {
                write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
                SHC_ERROR("Can't build the index response");
            }
            fbuf_destroy(&buf);
            fbuf_destroy(&out);
            break;
        }
        case SHC_HDR_REPLICA_COMMAND:
        case SHC_HDR_REPLICA_PING:
        {
            void *response = NULL;
            size_t response_len = 0;
            if (!cache->replica) {
                write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
                break;
            }

            shardcache_hdr_t rhdr =
                shardcache_replica_received_command(cache->replica,
                                                    req->hdr,
                                                    fbuf_data(&req->records[0]),
                                                    fbuf_used(&req->records[0]),
                                                    &response,
                                                    &response_len);
            if (response_len) {
                fbuf_t out = FBUF_STATIC_INITIALIZER;
                shardcache_record_t record = {
                    .v = response,
                    .l = response_len
                };
                if (build_message((char *)req->ctx->serv->auth,
                                  req->sig_hdr, rhdr,
                                  &record, 1, &out) == 0)
                {
                    // destroy it early ... since we still need one more copy
                    free(response);
                    MUTEX_LOCK(&req->lock);
                    fbuf_add_binary(&req->output, fbuf_data(&out), fbuf_used(&out));
                    req->done = 1;
                    MUTEX_UNLOCK(&req->lock);
                } else {
                    free(response);
                    SHC_ERROR("Can't build the REPLICA command response");
                }
                fbuf_destroy(&out);
            } else {
                write_status(req, rc, WRITE_STATUS_MODE_SIMPLE);
            }
            break;
        }
        default:
            fprintf(stderr, "Unsupported command: 0x%02x\n", (char)req->hdr);
            write_status(req, -1, WRITE_STATUS_MODE_SIMPLE);
            break;
    }
}


static void * worker(void *priv);

static shardcache_worker_context_t *
shardcache_select_worker(shardcache_serving_t *serv)
{
    if (ATOMIC_READ(serv->leave))
        return NULL;

    shardcache_worker_context_t *wrk = pick_value(serv->workers,
            __sync_fetch_and_add(&serv->next_worker_index, 1)%list_count(serv->workers));

    return wrk;
}


static void
shardcache_request_handler(iomux_t *iomux, int fd, shardcache_connection_context_t *ctx)
{
    // begin the worker slection process
    // first get the next worker in the array
    shardcache_worker_context_t *wrkctx = ctx->worker;
    // XXX 
    if (!wrkctx)
        wrkctx = shardcache_select_worker(ctx->serv);

    if (wrkctx) {
        ctx->retries = 0;
        //printf("PUSHED JOB %p\n", wrkctx);
        queue_push_right(wrkctx->jobs, ctx);
        CONDITION_SIGNAL(&wrkctx->wakeup_cond, &wrkctx->wakeup_lock);
    } else {
        // all workers are busy, we need to postpone this command
        ctx->retries++;
        struct timeval timeout = { 0, ctx->retries * 666 };
        if (timeout.tv_usec > 1000000) {
            timeout.tv_sec = 1;
            timeout.tv_usec = 0;
        }
        iomux_set_timeout(iomux, fd, &timeout);
    }
}

shardcache_request_t *
shardcache_request_create(shardcache_connection_context_t *ctx)
{
    shardcache_request_t *req = calloc(1, sizeof(shardcache_request_t));
    req->hdr = async_read_context_hdr(ctx->reader_ctx);
    req->sig_hdr = async_read_context_sig_hdr(ctx->reader_ctx);
    req->ctx = ctx;
    MUTEX_INIT(&req->lock);
    int i;
    for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++) {
        fbuf_copy(&ctx->records[i], &req->records[i]);
    }
    req->fetch_accumulator = rbuf_create(1<<16);
    return req;
}

static int
shardcache_check_context_state(iomux_t *iomux,
                               int fd,
                               shardcache_connection_context_t *ctx,
                               async_read_context_state_t state)
{
    if (state == SHC_STATE_READING_DONE) {
        // create a new request
        ctx->retries = 0;
        shardcache_request_t *req = shardcache_request_create(ctx);
        int i;
        for (i = 0; i < SHARDCACHE_CONNECTION_CONTEX_RECORDS_MAX; i++) {
            fbuf_clear(&ctx->records[i]);
        }
        push_value(ctx->requests, req);
        process_request(req);
    } else if (state == SHC_STATE_READING_ERR || state == SHC_STATE_AUTH_ERR) {
        // if the asynchronous reader is in error state we want
        // to close the connection, probably an unauthorized or a
        // badly formatted message has been sent by the client
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        SHC_WARNING("Bad message %02x from %s (%d)",
                    ctx->hdr, inet_ntoa(saddr.sin_addr), state);
        return -1;
    }
    return 0;
}


static void
shardcache_output_handler(iomux_t *iomux, int fd, unsigned char *out, int *len, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    int max = *len;
    int offset = 0;
    while (offset < max) {
        shardcache_request_t *req = pick_value(ctx->requests, 0);
        if (!req)
            break;

        MUTEX_LOCK(&req->lock);
        if (req->fetch_error) {
            // abort the request and close the connection
            // if there was an error while fetching a remote object
            iomux_close(iomux, fd);
            return;
        }
        void *req_output = fbuf_data(&req->output);
        int req_outlen = fbuf_used(&req->output);
        if (req_outlen > (max - offset)) {
            int to_copy = max - offset;
            memcpy(out + offset, req_output, to_copy);
            fbuf_remove(&req->output, to_copy);
            offset += to_copy;
        } else if (req_outlen > 0) {
            memcpy(out + offset, req_output, req_outlen);
            // NOTE: the buffer is already empty but calling
            // fbuf_clear() forces the fbuf to reset its
            // internal indexes and avoid wasting some memory
            // or doing a memmove later
            fbuf_clear(&req->output);
            offset += req_outlen;
        }

        int empty = (fbuf_used(&req->output) == 0);
        int done = req->done;
        MUTEX_UNLOCK(&req->lock);

        if (done && empty) {
            shift_value(ctx->requests);
            shardcache_request_destroy(req);
            // if we have pending input data this is time
            // to process it and move to the next request
            if (rbuf_used(ctx->input)) {
                int state = async_read_context_update(ctx->reader_ctx);
                shardcache_check_context_state(iomux, fd, ctx, state);
            }
        } else if (empty) {
            break;
        }
    }
    *len = offset;
}

static int
shardcache_input_handler(iomux_t *iomux,
                         int fd,
                         unsigned char *data,
                         int len,
                         void *priv)
{
    int processed = 0;

    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;


    if (ctx) {
        async_read_context_state_t state = async_read_context_input_data(ctx->reader_ctx, data, len, &processed);
        // updating the context state might eventually push a new requeset
        // (if entirely dowloaded) to a worker
        shardcache_check_context_state(iomux, fd, ctx, state);
    }

    return processed;
}

static void
shardcache_timeout_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    shardcache_request_handler(iomux, fd, ctx);
}

static void
shardcache_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_connection_context_t *ctx =
        (shardcache_connection_context_t *)priv;

    close(fd);
    ATOMIC_DECREMENT(ctx->serv->num_connections);

    if (ctx) {
        if (list_count(ctx->requests)) {
            ctx->closed = 1;
            gettimeofday(&ctx->in_prune_since, NULL);
            push_value(ctx->worker->prune, ctx);
            return;
        }
        shardcache_connection_context_destroy(ctx);
    }
}

static void
shardcache_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    shardcache_serving_t *serv = (shardcache_serving_t *)priv;

    if (!ATOMIC_READ(serv->leave)) {
        shardcache_worker_context_t *wrkctx = shardcache_select_worker(serv);
        if (wrkctx) {
            shardcache_connection_context_t *ctx =
                shardcache_connection_context_create(serv, fd);

            ctx->worker = wrkctx;
            queue_push_right(wrkctx->jobs, ctx);
            ATOMIC_INCREMENT(serv->num_connections);
        } else {
            close(fd);
            SHC_WARNING("Can't find any usable worker to handle the new connection");
        }
    }
}


static void *
worker(void *priv)
{
    shardcache_worker_context_t *wrkctx = (shardcache_worker_context_t *)priv;
    queue_t *jobs = wrkctx->jobs;

    while (ATOMIC_READ(wrkctx->leave) == 0) {
        shardcache_connection_context_t *ctx = queue_pop_left(jobs);
        while(ctx) {
            iomux_callbacks_t connection_callbacks = {
                .mux_connection = NULL,
                .mux_input = shardcache_input_handler,
                .mux_eof = shardcache_eof_handler,
                .mux_timeout = shardcache_timeout_handler,
                .mux_output = shardcache_output_handler,
                .priv = ctx
            };
            //ht_set(wrkctx->fds, &ctx->fd, sizeof(ctx->fd), ctx, sizeof(shardcache_connection_context_t));
            if (!iomux_add(wrkctx->iomux, ctx->fd, &connection_callbacks)) {
                shardcache_connection_context_destroy(ctx);
            }
            ctx = queue_pop_left(jobs);
        }


        int timeout = ATOMIC_READ(wrkctx->serv->cache->iomux_run_timeout_low);
        struct timeval tv = { timeout/1e6, timeout%(int)1e6 };
        iomux_run(wrkctx->iomux, &tv);

        int to_check = list_count(wrkctx->prune);
        while (to_check--) {
            shardcache_connection_context_t *to_prune = shift_value(wrkctx->prune);
            int pending = list_count(to_prune->requests);
            while (pending--) {
                shardcache_request_t *req = shift_value(to_prune->requests);
                MUTEX_LOCK(&req->lock);
                if (req->done) {
                    // the request is served, we can destroy it
                    MUTEX_UNLOCK(&req->lock);
                    shardcache_request_destroy(req);
                    continue;
                } 
                // put it back;
                push_value(to_prune->requests, req);
                MUTEX_UNLOCK(&req->lock);
            }
            struct timeval quarantine = { 60, 0 };
            struct timeval now, diff;
            gettimeofday(&now, NULL);
            timersub(&now, &to_prune->in_prune_since, &diff);
            if (!list_count(to_prune->requests) || timercmp(&diff, &quarantine, >)) {
                shardcache_connection_context_destroy(to_prune);
            } else {
                push_value(wrkctx->prune, to_prune);
            }
        }

        ATOMIC_SET(wrkctx->numfds, iomux_num_fds(wrkctx->iomux));
        ATOMIC_SET(wrkctx->pruning, list_count(wrkctx->prune));

        if (iomux_isempty(wrkctx->iomux)) {
            // we don't have any filedescriptor to handle in the mux,
            // let's sit for 1 second waiting for the listener thread to wake
            // us up if new filedescriptors arrive
            struct timespec abstime;
            struct timeval now;

            int rc = gettimeofday(&now, NULL);
            if (rc == 0) {
                struct timeval wait_time = { 0, 250000 };
                timeradd(&now, &wait_time, &wait_time);
                abstime.tv_sec = wait_time.tv_sec;
                abstime.tv_nsec = (wait_time.tv_usec * 1000);
                CONDITION_TIMEDWAIT(&wrkctx->wakeup_cond,
                                    &wrkctx->wakeup_lock,
                                    &abstime);

            }
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

    iomux_callbacks_t connection_callbacks = {
        .mux_connection = shardcache_connection_handler,
        .mux_input = NULL,
        .mux_eof = NULL,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = serv
    };

    iomux_add(serv->io_mux, serv->sock, &connection_callbacks);
    iomux_listen(serv->io_mux, serv->sock);

    while (!ATOMIC_READ(serv->leave)) {
        int timeout = ATOMIC_READ(serv->cache->iomux_run_timeout_high);
        struct timeval tv = { timeout/1e6, timeout%(int)1e6 };
        iomux_run(serv->io_mux, &tv);
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
    s->num_workers = num_workers;
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
    s->workers = create_list();

    if (s->counters) {
        shardcache_counter_add(s->counters, "connections", &s->num_connections);
        shardcache_counter_add(s->counters, "num_workers", &s->total_workers);
    }

    int i;
    for (i = 0; i < ATOMIC_READ(num_workers); i++) {
        shardcache_worker_context_t *wrk = calloc(1, sizeof(shardcache_worker_context_t));
        wrk->serv = s;
        wrk->jobs = queue_create();
        queue_set_free_value_callback(wrk->jobs,
                (queue_free_value_callback_t)shardcache_connection_context_destroy);
        wrk->prune = create_list();
        set_free_value_callback(wrk->prune, (free_value_callback_t)shardcache_connection_context_destroy);

        char label[64];
        snprintf(label, sizeof(label), "worker[%d].numfds", i);
        shardcache_counter_add(s->counters, label, &wrk->numfds);
        snprintf(label, sizeof(label), "worker[%d].pruning", i);
        shardcache_counter_add(s->counters, label, &wrk->pruning);

        MUTEX_INIT(&wrk->wakeup_lock);
        CONDITION_INIT(&wrk->wakeup_cond);
        //wrk->fds = ht_create(1024, 65535, NULL);
        wrk->iomux = iomux_create(0, 0, 0);
        pthread_create(&wrk->thread, NULL, worker, wrk);
        push_value(s->workers, wrk);
        ATOMIC_INCREMENT(s->total_workers);
    }

    s->io_mux = iomux_create(1<<13, 1<<13, 0);

    // and start a background thread to handle incoming connections
    int rc = pthread_create(&s->io_thread, NULL, serve_cache, s);
    if (rc != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(errno));
        stop_serving(s);
        return NULL;
    }

    return s;
}

static void
clear_workers_list(linked_list_t *list)
{
    shardcache_worker_context_t *wrk = shift_value(list);

    int cnt = 0;
    while (wrk) {
        ATOMIC_INCREMENT(wrk->leave);

        // wake up the worker if slacking
        CONDITION_SIGNAL(&wrk->wakeup_cond, &wrk->wakeup_lock);

        pthread_join(wrk->thread, NULL);

        queue_destroy(wrk->jobs);

        MUTEX_DESTROY(&wrk->wakeup_lock);
        CONDITION_DESTROY(&wrk->wakeup_cond);
        SHC_DEBUG3("Worker thread %p exited", wrk);

        shardcache_connection_context_t *ctx = shift_value(wrk->prune);
        while (ctx) {
            shardcache_request_t *req = shift_value(ctx->requests);
            while (req) {
                shardcache_request_destroy(req);
                req = shift_value(ctx->requests);
            }
            shardcache_connection_context_destroy(ctx);
            ctx = shift_value(wrk->prune);
        }

        char label[64];
        snprintf(label, sizeof(label), "worker[%d].numfds", cnt);
        shardcache_counter_remove(wrk->serv->counters, label);
        snprintf(label, sizeof(label), "worker[%d].pruning", cnt);
        shardcache_counter_remove(wrk->serv->counters, label);
        cnt++;

        iomux_destroy(wrk->iomux);

        destroy_list(wrk->prune);

        //ht_destroy(wrk->fds);
        free(wrk);
        wrk = shift_value(list);
    }
 
}

void
stop_serving(shardcache_serving_t *s)
{
    ATOMIC_INCREMENT(s->leave);

    iomux_remove(s->io_mux, s->sock);
    close(s->sock);

    // now the workers
    SHC_NOTICE("Collecting worker threads (might have to wait until i/o is finished)");
    clear_workers_list(s->workers); 
    SHC_DEBUG2("All worker threads have been collected");
 
    // unregister our counters if we did at creation time
    if (s->counters) {
        shardcache_counter_remove(s->counters, "connections");
        shardcache_counter_remove(s->counters, "num_workers");
    }

    pthread_join(s->io_thread, NULL);

    iomux_destroy(s->io_mux);
    destroy_list(s->workers);

    free(s);
}

