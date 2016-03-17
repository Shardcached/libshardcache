#include <sys/types.h>
#include <fcntl.h>
#include <fbuf.h>
#include <rbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include "messaging.h"
#include "connections.h"
#include "shardcache.h"
#include "shardcache_internal.h"

#include <iomux.h>
#include <inttypes.h>

#include "async_reader.h"

#define DEBUG_DUMP_MAXSIZE 128

static char hdr_check[256] = {
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, // 0x00 - 0x0F
    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x10 - 0x1F
    0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x20 - 0x2F
    0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x30 - 0x3F
    0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x40 - 0x4F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x50 - 0x5F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x60 - 0x6F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x70 - 0x7F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x80 - 0x8F
    1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, // 0x90 - 0x9F
    1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xA0 - 0xAF
};

static int _tcp_timeout = SHARDCACHE_TCP_TIMEOUT_DEFAULT;

int
global_tcp_timeout(int timeout)
{
    int old_value = ATOMIC_READ(_tcp_timeout);

    if (timeout >= 0)
        ATOMIC_SET(_tcp_timeout, timeout);

    return old_value;
}

static void read_message_async_eof(iomux_t *iomux, int fd, void *priv)
{
    read_async_input_eof(iomux, fd, priv);
    iomux_end_loop(iomux);
}

int
read_message_async(int fd,
                   async_read_callback_t cb,
                   void *priv,
                   async_read_wrk_t **worker)
{
    struct timeval iomux_timeout = { 0, 20000 }; // 20ms

    if (fd < 0)
        return -1;

    async_read_wrk_t *wrk = calloc(1, sizeof(async_read_wrk_t));
    wrk->ctx = async_read_context_create(cb, priv);
    wrk->cbs.mux_input = read_async_input_data;
    wrk->cbs.mux_timeout = read_async_timeout;
    wrk->cbs.mux_eof = read_async_input_eof;
    wrk->cbs.priv = wrk->ctx;
    wrk->fd = fd;

    int blocking = (!worker);
    if (blocking)
        wrk->cbs.mux_eof = read_message_async_eof;

    if (blocking) {
        iomux_t *iomux = iomux_create(1<<13, 0);
        if (!iomux) {
            async_read_context_destroy(wrk->ctx);
            free(wrk);
            return -1;
        }

        char state = SHC_STATE_READING_ERR;
        if (iomux_add(iomux, fd, &wrk->cbs)) {
            int tcp_timeout = global_tcp_timeout(-1);
            struct timeval maxwait = { tcp_timeout / 1000, (tcp_timeout % 1000) * 1000 };
            iomux_set_timeout(iomux, fd, &maxwait);

            // we are in blocking mode, let's wait for the job
            // to be completed
            for (;;) {
                iomux_run(iomux, &iomux_timeout);
                if (iomux_isempty(iomux))
                    break;
            }
            state = async_read_context_state(wrk->ctx);
        } else {
            async_read_context_destroy(wrk->ctx);
        }

        iomux_destroy(iomux);

        // NOTE: the read context (wrk->ctx) has been already
        //       released in the eof callback, so we don't
        //       need to release it here
        free(wrk);

        if (state == SHC_STATE_READING_ERR) {
            return -1;
        }
    } else {
        *worker = wrk;
    }

    return 0;
}

typedef struct {
    char *peer;
    void *key;
    size_t klen;
    int fd;
    fetch_from_peer_async_cb cb;
    void *priv;
    char buf[32];
} fetch_from_peer_helper_arg_t;

int
fetch_from_peer_helper(void *data,
                       size_t len,
                       int idx,
                       size_t total_len,
                       void *priv)
{
    fetch_from_peer_helper_arg_t *arg = (fetch_from_peer_helper_arg_t *)priv;

    // idx == -1 means that reading finished 
    // idx == -2 means error
    // idx == -3 means the async connection can been closed
    // any idx >= 0 refers to the record index
    
    int ret = 0;
    if (arg->cb) {
        if (idx >= 0)
            ret = arg->cb(arg->peer, arg->key, arg->klen, data, len, idx , total_len, arg->priv);
        else if (idx == -1)
            ret = arg->cb(arg->peer, arg->key, arg->klen, NULL, 0, -1, total_len, arg->priv);
        else
            ret = arg->cb(arg->peer, arg->key, arg->klen, NULL, 0, (idx == -3) ? -3 : -2, total_len, arg->priv);
    }

    if (ret != 0) {
        arg->cb = NULL;
        arg->priv = NULL;
    }

    if (idx == -3) {
        if (arg->fd >= 0)
            close(arg->fd);
        if (arg->key != arg->buf)
            free(arg->key);
        free(arg);
    }

    return ret;
}

int
fetch_from_peer_async(char *peer,
                      void *key,
                      size_t klen,
                      size_t offset,
                      size_t len,
                      fetch_from_peer_async_cb cb,
                      void *priv,
                      int fd,
                      async_read_wrk_t **wrk)
{
    int rc = -1;
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    uint32_t offset_nbo = htonl(offset);
    uint32_t len_nbo = htonl(len);
    if (fd >= 0) {
        shardcache_record_t record[3] = {
            {
                .v = key,
                .l = klen
            },
            {
                .v = &offset_nbo,
                .l = sizeof(uint32_t)
            },
            {
                .v = &len_nbo,
                .l = sizeof(uint32_t)
            }
        };

        if (!offset && !len)
            rc = write_message(fd, SHC_HDR_GET_ASYNC, &record[0], 1);
        else
            rc = write_message(fd, SHC_HDR_GET_OFFSET, record, 3);

        if (rc == 0) {
            fetch_from_peer_helper_arg_t *arg = calloc(1, sizeof(fetch_from_peer_helper_arg_t));
            arg->peer = peer;
            if (klen > sizeof(arg->buf))
                arg->key = malloc(klen);
            else
                arg->key = arg->buf;
            memcpy(arg->key, key, klen);
            arg->klen = klen;
            arg->fd = should_close ? fd : -1;
            arg->cb = cb;
            arg->priv = priv;
            rc = read_message_async(fd, fetch_from_peer_helper, arg, wrk);
            if (rc != 0) {
                if (fd >= 0 && should_close)
                    close(fd);
                if (arg->key != arg->buf)
                    free(arg->key);
                free(arg);
            }
        } else {
            if (fd >= 0 && should_close)
                close(fd);
        }
    }
    return rc;
}

// synchronous (blocking)  message reading
int
read_message(int fd,
             fbuf_t **records,
             int expected_records,
             shardcache_hdr_t *ohdr,
             int ignore_timeout)
{
    uint16_t clen;
    int reading_message = 0;
    unsigned char hdr;
    char version = 0;


    // there is no point in reading the message
    // if we are not interested in any record
    if (expected_records < 1)
        return -1;

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);

    int record_index = 0;
    fbuf_t *out = records[record_index];
    int initial_len = out ? fbuf_used(out) : 0;

    for(;;) {
        int rb;

        if (reading_message == 0) {
            uint32_t magic = 0;
            do {
                rb = read_socket(fd, (char *)&hdr, 1, ignore_timeout);
            } while (rb == 1 && hdr == SHC_HDR_NOOP);

            ((char *)&magic)[0] = hdr;
            rb = read_socket(fd, ((char *)&magic)+1, sizeof(magic)-1, ignore_timeout);
            if (rb != sizeof(magic) -1) {
                return -1;
            }

            if (((ntohl(magic))&0xFFFFFF00) != (SHC_MAGIC&0xFFFFFF00)) {
                SHC_DEBUG("Wrong magic");
                return -1;
            }
            version = ((char *)&magic)[3];
            if (version > SHC_PROTOCOL_VERSION) {
                SHC_WARNING("Unsupported protocol version 0x%02x\n", version);
                return -1;
            }

            rb = read_socket(fd, (char *)&hdr, 1, ignore_timeout);
            if (rb != 1) {
                return -1;
            }

            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                return -1;
            } else if (rb == -1) {
                continue;
            }

            if (!hdr_check[hdr]) {
                fprintf(stderr, "Unknown message type %02x in read_message()\n", hdr);
                return -1;
            }

            if (ohdr)
                *ohdr = hdr;
            reading_message = 1;
        }

        if (version < 2) {
            rb = read_socket(fd, (char *)&clen, 2, ignore_timeout);
            // XXX - bug if read only one byte at this point
            if (rb == 2) {
                uint16_t chunk_len = ntohs(clen);

                if (chunk_len == 0) {
                    unsigned char rsep = 0;
                    rb = read_socket(fd, (char *)&rsep, 1, ignore_timeout);
                    if (rb != 1) {
                        if (out)
                            fbuf_set_used(out, initial_len);
                        return -1;
                    }

                    if (rsep == SHARDCACHE_RSEP) {
                        // go ahead fetching the next record
                        record_index++;
                        if (record_index == expected_records) {
                            return record_index + 1;
                        }
                        out = records[record_index];
                    } else if (rsep == 0) {
                        return record_index + 1;
                    } else {
                        // BOGUS RESPONSE
                        if (out)
                            fbuf_set_used(out, initial_len);
                        return -1;
                    }
                }

                while (chunk_len != 0) {
                    char buf[chunk_len];
                    rb = read_socket(fd, buf, chunk_len, ignore_timeout);
                    if (rb == -1) {
                        if (errno != EINTR && errno != EAGAIN) {
                            // ERROR
                            if (out)
                                fbuf_set_used(out, initial_len);

                            return -1;
                        }
                        continue;
                    } else if (rb == 0) {
                        if (out)
                            fbuf_set_used(out, initial_len);

                        return -1;
                    }
                    chunk_len -= rb;
                    if (out)
                        fbuf_add_binary(out, buf, rb);

                    if (out && fbuf_used(out) > SHARDCACHE_MSG_MAX_RECORD_LEN) {
                        // we have exceeded the maximum size for a record
                        // let's abort this request
                        fprintf(stderr, "Maximum record size exceeded (%dMB)",
                                SHARDCACHE_MSG_MAX_RECORD_LEN >> 20);
                        fbuf_set_used(out, initial_len);

                        return -1;
                    }
                }
            } else if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                // ERROR
                break;
            }
        } else {
            uint32_t rlen = 0;
            rb = read_socket(fd, (char *)&rlen, 4, ignore_timeout);
            // XXX - bug if read only one byte at this point
            if (rb == 4) {
                uint32_t record_len = ntohl(rlen);

                while (record_len != 0) {
                    char buf[record_len];
                    rb = read_socket(fd, buf, record_len, ignore_timeout);
                    if (rb == -1) {
                        if (errno != EINTR && errno != EAGAIN) {
                            // ERROR
                            if (out)
                                fbuf_set_used(out, initial_len);

                            return -1;
                        }
                        continue;
                    } else if (rb == 0) {
                        if (out)
                            fbuf_set_used(out, initial_len);

                        return -1;
                    }
                    record_len -= rb;
                    if (out)
                        fbuf_add_binary(out, buf, rb);

                    if (out && fbuf_used(out) > SHARDCACHE_MSG_MAX_RECORD_LEN) {
                        // we have exceeded the maximum size for a record
                        // let's abort this request
                        fprintf(stderr, "Maximum record size exceeded (%dMB)",
                                SHARDCACHE_MSG_MAX_RECORD_LEN >> 20);
                        fbuf_set_used(out, initial_len);

                        return -1;
                    }
                }

                unsigned char rsep = 0;
                rb = read_socket(fd, (char *)&rsep, 1, ignore_timeout);
                if (rb != 1) {
                    if (out)
                        fbuf_set_used(out, initial_len);
                    return -1;
                }

                if (rsep == SHARDCACHE_RSEP) {
                    // go ahead fetching the next record
                    record_index++;
                    if (record_index == expected_records) {
                        return record_index + 1;
                    }
                    out = records[record_index];
                } else if (rsep == 0) {
                    return record_index + 1;
                } else {
                    // BOGUS RESPONSE
                    if (out)
                        fbuf_set_used(out, initial_len);
                    return -1;
                }
            } else if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                // ERROR
                break;
            }
        }
    }

    return -1;
}

int
_chunkize_buffer(void *buf,
                 size_t blen,
                 fbuf_t *out)
{
    int ofx = 0;

    do {
        int writelen = (blen > (size_t)UINT16_MAX) ? UINT16_MAX : blen;
        blen -= writelen;
        uint16_t size = htons(writelen);
        fbuf_add_binary(out, (char *)&size, 2);
        fbuf_add_binary(out, buf + ofx, writelen);
        if (blen == 0) {
            uint16_t eor = 0;
            fbuf_add_binary(out, (char *)&eor, 2);
            return 0;
        }
        ofx += writelen;
    } while (blen != 0);

    return -1;
}

// convert a (de-chunkized) record to an array of vaules
// NOTE: the record MUST be complete and without the chunk-size headers
uint32_t
record_to_array(fbuf_t *record, char ***items, size_t **lens)
{
    char *data = fbuf_data(record);
    int data_len = fbuf_used(record);

    if (data_len < sizeof(uint32_t))
        return -1;

    uint32_t num_items = ntohl(*((uint32_t *)data));
    data += sizeof(uint32_t);

    int i;
    for (i = 0; i < num_items; i++) {
        uint32_t item_size = ntohl(*((uint32_t *)data));
        data += sizeof(uint32_t);
        *items = realloc(*items, (sizeof(char *) * num_items) + 1);
        if (items) {
            if (item_size) {
                (*items)[i] = malloc(item_size);
                memcpy((*items)[i], data, item_size);
                data += item_size;
            } 
        } else {
            data += item_size;
        }

        if (lens) {
            *lens = realloc(*lens, (sizeof(size_t) * num_items) + 1);
            (*lens)[i] = item_size;
        }
    }

    return 0;
}

static inline uint32_t
_dump_records(int num_items, fbuf_t **items, fbuf_t *out)
{
    uint32_t wrote = 0;
    int i;
    for (i = 0; i < num_items; i++) { 
        if (items[i]) {
            uint32_t isize = fbuf_used(items[i]);
            uint32_t isize_nbo = htonl(isize);
            fbuf_add_binary(out, (char *)&isize_nbo, sizeof(isize_nbo)); 
            fbuf_concat(out, items[i]);
            wrote += isize + sizeof(isize_nbo);
        } else {
            uint32_t nullsize = 0;
            fbuf_add_binary(out, (char *)&nullsize, sizeof(nullsize));
            wrote += sizeof(nullsize);
        }
    }
    return wrote;
}


// convert an array of items to a (chunkized) record ready to be sent on the wire
// NOTE: the produced record will be chunkized if necessary and will include
// the chunk-size headers
void
array_to_record(int num_items, fbuf_t **items, fbuf_t *out)
{
    uint32_t num_items_nbo = htonl(num_items);

    uint32_t to_write = sizeof(num_items_nbo);

    int count_wrote = 0;

    int idx, last_idx = 0;
    for (idx = 0; idx < num_items; idx++) {
        if (!items[idx]) {
            to_write += sizeof(uint32_t);
            continue;
        }
        int current_size = fbuf_used(items[idx]);
        // flush one chunk if we need to
        if (to_write + current_size + sizeof(uint32_t) > 65535) {
            // the size of the chunk
            uint16_t cs = htons(to_write);
            fbuf_add_binary(out, (char *)&cs, sizeof(to_write));

            if (!count_wrote) {
                fbuf_add_binary(out, (char *)&num_items_nbo, sizeof(num_items_nbo));
                count_wrote = 1;
                to_write -= sizeof(num_items_nbo);
            }

            uint32_t wrote = _dump_records(idx - last_idx, &items[last_idx], out);
            if (wrote != to_write) {
                SHC_ERROR("%s (%s:%s) - Can't dump the complete message, it will be truncated!",
                          __FUNCTION__, __LINE__, __FILE__);
            }

            last_idx = idx;
            to_write = 0;
        }
        to_write += sizeof(uint32_t) + current_size;
    }

    // size of last chunk
    uint16_t cs = htons(to_write);
    fbuf_add_binary(out, (char *)&cs, sizeof(cs));

    uint32_t wrote = 0;
    if (!count_wrote) {
        fbuf_add_binary(out, (char *)&num_items_nbo, sizeof(num_items_nbo));
        wrote += sizeof(uint32_t);
    }

    if (num_items && items)
        wrote += _dump_records(idx - last_idx, &items[last_idx], out);

    if (wrote != to_write) {
        SHC_ERROR("%s (%s:%s) - Can't dump the complete message, it will be truncated!",
                  __FUNCTION__, __LINE__, __FILE__);
    }

    // end-of-record
    cs = 0;
    fbuf_add_binary(out, (char *)&cs, sizeof(cs));
}

char
rc_to_status(int rc, rc_to_status_mode_t mode)
{
    char out;
    if (rc == -1) {
        out = SHC_RES_ERR;
    } else {
        if (mode == WRITE_STATUS_MODE_BOOLEAN) {
            if (rc == 1)
                out = SHC_RES_YES;
            else if (rc == 0)
                out = SHC_RES_NO;
            else
                out = SHC_RES_ERR;
        } else if (mode == WRITE_STATUS_MODE_EXISTS && rc == 1) {
            out = SHC_RES_EXISTS;
        }
        else if (rc == 0) {
            out = SHC_RES_OK;
        } else {
            out = SHC_RES_ERR;
        }
    }
    return out;
}


int build_message(unsigned char hdr,
                  shardcache_record_t *records,
                  int num_records,
                  fbuf_t *out,
                  char version)
{
    static char eom = 0;
    static char sep = SHARDCACHE_RSEP;
    uint16_t    eor = 0;

    uint32_t magic = htonl(SHC_MAGIC);
    fbuf_add_binary(out, (char *)&magic, sizeof(magic));

    fbuf_add_binary(out, (char *)&hdr, 1);

    if (num_records) {
        int i;
        for (i = 0; i < num_records; i++) {
            if (i > 0) {
                fbuf_add_binary(out, &sep, 1);
            }
            if (records[i].v && records[i].l) {
                if (version < 2) {
                    if (_chunkize_buffer(records[i].v, records[i].l, out) != 0) {
                        return -1;
                    }
                } else {
                    uint32_t len_nbo = htonl(records[i].l);
                    fbuf_add_binary(out, (char *)&len_nbo, sizeof(len_nbo));
                    fbuf_add_binary(out, records[i].v, records[i].l);
                }
            } else if (version < 2) {
                fbuf_add_binary(out, (char *)&eor, sizeof(eor));
            }
        }
    } else { 
        if (version < 2) {
            fbuf_add_binary(out, (char *)&eor, sizeof(eor));
        } else {
            uint32_t zero_len = 0;
            fbuf_add_binary(out, (char *)&zero_len, sizeof(zero_len));
        }
    }

    fbuf_add_binary(out, &eom, 1);

    return 0;
}

int
write_message(int fd,
              unsigned char hdr,
              shardcache_record_t *records,
              int num_records)
{

    fbuf_t msg = FBUF_STATIC_INITIALIZER;

    if (build_message(hdr, records, num_records, &msg, SHC_PROTOCOL_VERSION) != 0)
    {
        // TODO - Error Messages
        fbuf_destroy(&msg);
        return -1;
    }

    size_t mlen = fbuf_used(&msg);
    size_t dlen = 0;
    SHC_DEBUG2("sending message: %s",
           shardcache_hex_escape(fbuf_data(&msg), mlen-dlen, DEBUG_DUMP_MAXSIZE, 0));

    if (dlen && fbuf_used(&msg) >= dlen) {
        SHC_DEBUG2("computed digest: %s",
                  shardcache_hex_escape(fbuf_end(&msg)-dlen, dlen, 0, 0));
    }

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);

    while(fbuf_used(&msg) > 0) {
        int wb = fbuf_write(&msg, fd, 0);
        if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
            fbuf_destroy(&msg);
            return -1;
        }
    }
    fbuf_destroy(&msg);
    return 0;
}


static inline int
_delete_from_peer_internal(char *peer,
                           void *key,
                           size_t klen,
                           int owner,
                           int fd,
                           int expect_response)
{
    int rc = -1;
    int should_close = 0;

    SHC_DEBUG2("Sending del command to peer %s (owner: %d)", peer, owner);

    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        unsigned char hdr;
        if (owner)
            hdr = SHC_HDR_DELETE;
        else
            hdr = SHC_HDR_EVICT;

        shardcache_record_t record = {
            .v = key,
            .l = klen
        };
        rc = write_message(fd, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0 && expect_response) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                SHC_DEBUG2("Got (del) response from peer %s: %02x\n",
                          peer, *((char *)fbuf_data(&resp)));
                if (should_close)
                    close(fd);
                rc = -1;
                char *res = fbuf_data(&resp);
                if (res && *res == SHC_RES_OK)
                    rc = 0;

                fbuf_destroy(&resp);
                return rc;
            } else {
                // TODO - Error messages
            }
            fbuf_destroy(&resp);
        }
        if (should_close)
            close(fd);
    }

    return (rc == 0) ? 0 : -1;
}

int
delete_from_peer(char *peer,
                 void *key,
                 size_t klen,
                 int fd,
                 int expect_response)
{
    return _delete_from_peer_internal(peer, key, klen, 1, fd, expect_response);
}

int
evict_from_peer(char *peer,
                void *key,
                size_t klen,
                int fd,
                int expect_response)
{
    return _delete_from_peer_internal(peer, key, klen, 0, fd, expect_response);
}



static inline int
_send_to_peer_internal(char *peer,
                       void *key,
                       size_t klen,
                       void *value1,
                       size_t vlen1,
                       void *value2,
                       size_t vlen2,
                       uint32_t ttl,
                       uint32_t cttl,
                       int mode, // 0 == SET, 1 == ADD, 2 == CAS, 3 == INCR, 4 == DECR
                       int64_t *computed_amount,
                       int fd,
                       int expect_response)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        if (fd < 0)
            return -1;
        should_close = 1;
    }

    int64_t rc = -1;
    // NOTE : the biggest command involves 5 reords
    shardcache_record_t record[5] = {
        {
            .v = key,
            .l = klen
        },
        {
            .v = value1,
            .l = vlen1
        }
    };
    int num_records = 2;

    unsigned char hdr;
    switch(mode) {
        case 0:
            hdr = SHC_HDR_SET;
            break;
        case 1:
            hdr = SHC_HDR_ADD;
            break;
        case 2:
        case 3:
        case 4:
            switch(mode) {
                case 2:
                    hdr = SHC_HDR_CAS;
                    break;
                case 3:
                    hdr = SHC_HDR_INCREMENT;
                    break;
                case 4:
                    hdr = SHC_HDR_DECREMENT;
                    break;
            }
            record[2].v = value2;
            record[2].l = vlen2;
            num_records = 3;
            break;
        default:
            // TODO - Error message for unsupported mode
            SHC_ERROR("Unknown mode %d in %s (%s:%s)", mode, __FUNCTION__, __FILE__, __LINE__);
            return -1;
    }

    uint32_t ttl_nbo = 0;
    if (ttl) {
        ttl_nbo = htonl(ttl);
        record[num_records].v = &ttl_nbo;
        record[num_records].l = sizeof(ttl);
        num_records++;
    }

    uint32_t cttl_nbo = 0;
    if (cttl) {
        cttl_nbo = htonl(cttl);
        record[num_records].v = &cttl_nbo;
        record[num_records].l = sizeof(cttl);
        num_records++;
    }

    rc = write_message(fd, hdr, record, num_records);
    if (rc != 0) {
        if (should_close)
            close(fd);
        return -1;
    }

    if (rc == 0 && expect_response) {
        shardcache_hdr_t hdr = 0;
        fbuf_t resp[2];
        fbuf_t *respp[2] = { &resp[0], &resp[1] };

        FBUF_STATIC_INITIALIZER_POINTER(&resp[0], 0, 10, 10, 1);
        FBUF_STATIC_INITIALIZER_POINTER(&resp[1], 0, 64, 256, 1);

        errno = 0;

        int num_records = read_message(fd, respp, 2, &hdr, 0);

        SHC_DEBUG2("%s: Got response for command %02x from peer %s : %s\n",
                  __FUNCTION__, hdr, peer, fbuf_data(&resp[0]));

        if (hdr == SHC_HDR_ERROR && num_records == 2) {
            char *error_code = fbuf_data(&resp[0]);
            SHC_ERROR("%s : %.*s (%d)", __FUNCTION__, fbuf_used(&resp[1]), fbuf_data(&resp[1]), *error_code);
            rc = *error_code;
        } else if (hdr != SHC_HDR_RESPONSE && num_records == 1) {
            SHC_ERROR("Bad response (%02x) from %s : %s\n", hdr, peer, strerror(errno));
            rc = -1;
        } else {
            if (mode < 3) { // anything but INCR and DECR
                char *res = fbuf_data(&resp[0]);
                if (res) {
                    switch(*res) {
                        case SHC_RES_EXISTS:
                            rc = 1;
                            break;
                        case SHC_RES_OK:
                            rc = 0;
                            break;
                        default:
                            rc = -1;
                            break;
                    }
                } else {
                    rc = -1;
                }
            } else { // INCR AND DECR responses differ from other set-related responses
                char *decimal_string = fbuf_data(&resp[0]);
                int64_t amount = strtoll(decimal_string, NULL, 10);
                if (computed_amount)
                    *computed_amount = amount;
                rc =  (!amount && errno) ? -1 : 0;
            }
        }
        fbuf_destroy(&resp[0]);
        fbuf_destroy(&resp[1]);
    } else if (rc != 0) {
        fprintf(stderr, "Error reading from socket %d (%s) : %s\n",
                fd, peer, strerror(errno));
    }

    if (should_close)
        close(fd);

    return rc;
}

int
send_to_peer(char *peer,
             void *key,
             size_t klen,
             void *value,
             size_t vlen,
             uint32_t ttl,
             uint32_t cttl,
             int fd,
             int expect_response)
{
    return _send_to_peer_internal(peer,
                                  key,
                                  klen,
                                  value,
                                  vlen,
                                  NULL,
                                  0,
                                  ttl,
                                  cttl,
                                  0,
                                  NULL,
                                  fd,
                                  expect_response);
}

int
add_to_peer(char *peer,
            void *key,
            size_t klen,
            void *value,
            size_t vlen,
            uint32_t ttl,
            uint32_t cttl,
            int fd,
            int expect_response)
{
    return _send_to_peer_internal(peer,
                                  key,
                                  klen,
                                  value,
                                  vlen,
                                  NULL,
                                  0,
                                  ttl,
                                  cttl,
                                  1,
                                  NULL,
                                  fd,
                                  expect_response);
}

int
cas_on_peer(char *peer,
            void *key,
            size_t klen,
            void *old_value,
            size_t old_vlen,
            void *value,
            size_t vlen,
            uint32_t ttl,
            uint32_t cttl,
            int fd,
            int expect_response)
{
    return _send_to_peer_internal(peer,
                                  key,
                                  klen,
                                  old_value,
                                  old_vlen,
                                  value,
                                  vlen,
                                  ttl,
                                  cttl,
                                  2,
                                  NULL,
                                  fd,
                                  expect_response);
}

int
increment_on_peer(char *peer,
                  void *key,
                  size_t klen,
                  int64_t amount,
                  int64_t initial,
                  time_t ttl,
                  time_t cttl,
                  int64_t *computed_amount,
                  int fd,
                  int expect_response)
{
    fbuf_t amount_data = FBUF_STATIC_INITIALIZER;
    fbuf_t initial_data = FBUF_STATIC_INITIALIZER;
    fbuf_printf(&amount_data, "%"PRIi64, amount);
    fbuf_printf(&initial_data, "%"PRIi64, initial);
    int64_t rc = _send_to_peer_internal(peer,
                                         key,
                                         klen,
                                         fbuf_data(&amount_data),
                                         fbuf_used(&amount_data),
                                         fbuf_data(&initial_data),
                                         fbuf_used(&initial_data),
                                         ttl,
                                         cttl,
                                         3,
                                         computed_amount,
                                         fd,
                                         expect_response);
    fbuf_destroy(&amount_data);
    fbuf_destroy(&initial_data);
    return rc;
}

int
decrement_on_peer(char *peer,
                  void *key,
                  size_t klen,
                  int64_t amount,
                  int64_t initial,
                  time_t ttl,
                  time_t cttl,
                  int64_t *computed_amount,
                  int fd,
                  int expect_response)
{
    fbuf_t amount_data = FBUF_STATIC_INITIALIZER;
    fbuf_t initial_data = FBUF_STATIC_INITIALIZER;
    fbuf_printf(&amount_data, "%"PRIi64, amount);
    fbuf_printf(&initial_data, "%"PRIi64, initial);
    int64_t rc = _send_to_peer_internal(peer,
                                        key,
                                        klen,
                                        fbuf_data(&amount_data),
                                        fbuf_used(&amount_data),
                                        fbuf_data(&initial_data),
                                        fbuf_used(&initial_data),
                                        ttl,
                                        cttl,
                                        4,
                                        computed_amount,
                                        fd,
                                        expect_response);
    fbuf_destroy(&amount_data);
    fbuf_destroy(&initial_data);
    return rc;
}

int
fetch_from_peer(char *peer,
                void *key,
                size_t len,
                fbuf_t *out,
                int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        shardcache_record_t record = {
            .v = key,
            .l = len
        };
        int rc = write_message(fd, SHC_HDR_GET, &record, 1);
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t *records[2] = { out, NULL };
            int num_records = read_message(fd, records, 2, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 2) {
                if (fbuf_used(out)) {
                    char keystr[1024];
                    memcpy(keystr, key, len < 1024 ? len : 1024);
                    keystr[len] = 0;
                    SHC_DEBUG2("Got new data from peer %s : %s => %s", peer, keystr,
                              shardcache_hex_escape(fbuf_data(out), fbuf_used(out), DEBUG_DUMP_MAXSIZE, 0));
                }
                if (should_close)
                    close(fd);
                
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

int
offset_from_peer(char *peer,
                 void *key,
                 size_t len,
                 uint32_t offset,
                 uint32_t dlen,
                 fbuf_t *out,
                 int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    size_t offset_nbo = htonl(offset);
    size_t dlen_nbo = htonl(dlen);
    if (fd >= 0) {
        shardcache_record_t record[3] = {
            {
                .v = key,
                .l = len
            },
            {
                .v = &offset_nbo,
                .l = sizeof(uint32_t)
            },
            {
                .v = &dlen_nbo,
                .l = sizeof(uint32_t)
            }
        };
        int rc = write_message(fd, SHC_HDR_GET_OFFSET, record, 3);

        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t *records[3] = { out, NULL, NULL };
            int num_records = read_message(fd, records, 3, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 3) {
                if (fbuf_used(out)) {
                    char keystr[1024];
                    memcpy(keystr, key, len < 1024 ? len : 1024);
                    keystr[len] = 0;
                    SHC_DEBUG2("Got new data from peer %s : %s => %s", peer, keystr,
                              shardcache_hex_escape(fbuf_data(out), fbuf_used(out), DEBUG_DUMP_MAXSIZE, 0));
                }
                if (should_close)
                    close(fd);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}



int
exists_on_peer(char *peer,
               void *key,
               size_t klen,
               int fd,
               int expect_response)
{
    int rc = -1;
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    SHC_DEBUG2("Sending exists command to peer %s", peer);
    if (fd >= 0) {
        unsigned char hdr = SHC_HDR_EXISTS;
        shardcache_record_t record = {
            .v = key,
            .l = klen
        };
        rc = write_message(fd, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0 && expect_response) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                SHC_DEBUG2("Got (exists) response from peer %s : %s\n",
                          peer, fbuf_data(&resp));
                if (should_close)
                    close(fd);

                unsigned char *res = (unsigned char *)fbuf_data(&resp);
                if (res) {
                    switch(*res) {
                        case SHC_RES_YES:
                            rc = 1;
                            break;
                        case SHC_RES_NO:
                            rc = 0;
                            break;
                        default:
                            rc = -1;
                            break;
                    }
                }
                fbuf_destroy(&resp);
                return rc;
            } else {
                // TODO - Error messages
            }
            fbuf_destroy(&resp);
        }
        if (should_close)
            close(fd);
        return 0;
    }
    return rc;
}

int
touch_on_peer(char *peer,
              void *key,
              size_t klen,
              int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    SHC_DEBUG2("Sending touch command to peer %s", peer);
    if (fd >= 0) {
        unsigned char hdr = SHC_HDR_TOUCH;
        shardcache_record_t record = {
            .v = key,
            .l = klen
        };
        int rc = write_message(fd, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                SHC_DEBUG2("Got (touch) response from peer %s : %s\n",
                          peer, fbuf_data(&resp));
                if (should_close)
                    close(fd);

                rc = -1;
                char *res = fbuf_data(&resp);
                if (res && *res == SHC_RES_OK)
                    rc = 0;

                fbuf_destroy(&resp);
                return rc;
            } else {
                // TODO - Error messages
            }
            fbuf_destroy(&resp);
        }
        if (should_close)
            close(fd);
        return 0;
    }
    return -1;
}


int
stats_from_peer(char *peer,
                char **out,
                size_t *len,
                int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, SHC_HDR_STATS, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                size_t l = fbuf_used(&resp)+1;
                if (len)
                    *len = l;
                if (out) {
                    *out = malloc(l);
                    memcpy(*out, fbuf_data(&resp), l-1);
                    (*out)[l-1] = 0;
                    if (should_close)
                        close(fd);
                }
                return 0;
            }
            fbuf_destroy(&resp);
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

int
check_peer(char *peer,
           int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, SHC_HDR_CHECK, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                rc = -1;
                char *res = fbuf_data(&resp);
                if (res && *res == SHC_RES_OK)
                    rc = 0;

                if (should_close)
                    close(fd);

                return rc;
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

shardcache_storage_index_t *
index_from_peer(char *peer,
                int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    shardcache_storage_index_t *index = calloc(1, sizeof(shardcache_storage_index_t));
    if (fd >= 0) {
        int rc = write_message(fd, SHC_HDR_GET_INDEX, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, &respp, 1, &hdr, 1);
            if (hdr == SHC_HDR_INDEX_RESPONSE && num_records == 1) {
                char *data = fbuf_data(&resp);
                int len = fbuf_used(&resp);
                int ofx = 0;
                while (ofx < len) {
                    uint32_t *nklen = (uint32_t *)(data+ofx);
                    uint32_t klen = ntohl(*nklen);
                    if (klen == 0) {
                        // the index has ended
                        break;
                    } else if (ofx + klen + 8 > len) {
                        // TODO - Error messages (truncated?)
                        break;
                    }
                    ofx += 4;
                    void *key = malloc(klen);
                    memcpy(key, data+ofx, klen);
                    ofx += klen;
                    uint32_t *nvlen = (uint32_t *)(data+ofx);
                    uint32_t vlen = ntohl(*nvlen);
                    ofx += 4;
                    index->items = realloc(index->items, (index->size + 1) * sizeof(shardcache_storage_index_item_t));
                    index->items[index->size].key = key;
                    index->items[index->size].klen = klen;
                    index->items[index->size].vlen = vlen;
                    index->size++;
                }
            }
            fbuf_destroy(&resp);
        }
        if (should_close)
            close(fd);
    }
    return index;
}

int
migrate_peer(char *peer,
             void *msgdata,
             size_t len,
             int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    SHC_NOTICE("Sending migration_begin command to peer %s", peer);

    if (fd >= 0) {
        shardcache_record_t record = {
            .v = msgdata,
            .l = len
        };
        int rc = write_message(fd, SHC_HDR_MIGRATION_BEGIN, &record, 1);
        if (rc != 0) {
            if (should_close)
                close(fd);
            return -1;
        }

        shardcache_hdr_t hdr = 0;
        fbuf_t resp = FBUF_STATIC_INITIALIZER;
        fbuf_t *respp = &resp;
        int num_records = read_message(fd, &respp, 1, &hdr, 0);
        if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
            SHC_DEBUG2("Got (del) response from peer %s : %s",
                    peer, fbuf_data(&resp));
            if (should_close)
                close(fd);
            fbuf_destroy(&resp);
            return 0;
        } else {
            // TODO - Error messages
        }
        fbuf_destroy(&resp);
        if (should_close)
            close(fd);
    }
    return -1;
}

int
abort_migrate_peer(char *peer,
                   int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, SHC_HDR_MIGRATION_ABORT, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                rc = -1;
                char *res = fbuf_data(&resp);
                if (res && *res == SHC_RES_OK)
                    rc = 0;

                if (should_close)
                    close(fd);

                return rc;
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

int
connect_to_peer(char *address_string, unsigned int timeout)
{
    static __thread char host[2048];
    static __thread int port = 0;
    static __thread int len = 0;

    char *sep = strchr(address_string, ':');

    if (sep) {
        len = sep - address_string;
        port = strtol(sep+1, NULL , 10);
    } else {
        len = strlen(address_string);
        port = SHARDCACHE_PORT_DEFAULT;
    }

    if (__builtin_expect(len < sizeof(host), 1)) {
        snprintf(host, len + 1, "%s", address_string);
    } else {
        SHC_ERROR("address_string too long : %s", address_string);
        return -1;
    }
    int fd = open_connection(host, port, timeout);
    if (__builtin_expect(fd < 0 && errno != EMFILE, 0))
        SHC_DEBUG("Can't connect to %s", address_string);
    return fd;
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
