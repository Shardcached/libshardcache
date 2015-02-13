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

#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

#include "messaging.h"
#include "connections.h"
#include "shardcache.h"
#include "shardcache_internal.h"

#include <iomux.h>

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
    1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, // 0x90 - 0x9F
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
                   char *auth,
                   async_read_callback_t cb,
                   void *priv,
                   async_read_wrk_t **worker)
{
    struct timeval iomux_timeout = { 0, 20000 }; // 20ms

    if (fd < 0)
        return -1;

    async_read_wrk_t *wrk = calloc(1, sizeof(async_read_wrk_t));
    wrk->ctx = async_read_context_create(auth, cb, priv);
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
            ret = arg->cb(arg->peer, arg->key, arg->klen, data, len, idx, arg->priv);
        else if (idx == -1)
            ret = arg->cb(arg->peer, arg->key, arg->klen, NULL, 0, -1, arg->priv);
        else
            ret = arg->cb(arg->peer, arg->key, arg->klen, NULL, 0, (idx == -3) ? -3 : -2, arg->priv);
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
                      char *auth,
                      unsigned char sig_hdr,
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
            rc = write_message(fd, auth, sig_hdr, SHC_HDR_GET_ASYNC, &record[0], 1);
        else
            rc = write_message(fd, auth, sig_hdr, SHC_HDR_GET_OFFSET, record, 3);

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
            rc = read_message_async(fd, auth, fetch_from_peer_helper, arg, wrk);
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

static inline int
read_and_check_siphash_signature(int fd, sip_hash *shash)
{
    uint64_t digest, received_digest;

    int rb = read_socket(fd, (char *)&received_digest, sizeof(received_digest), 0);
    if (rb != sizeof(received_digest)) {
        SHC_WARNING("Truncated message (expected signature)");
        return -1;
    }
    if (!sip_hash_final_integer(shash, &digest)) {
        SHC_ERROR("Errors computing the siphash digest");
        return -1;
    }

    int match = (memcmp(&digest, &received_digest, sizeof(digest)) == 0);

    SHC_DEBUG2("computed digest for received data: %s",
            shardcache_hex_escape((char *)&digest, sizeof(digest), 0, 0));

    SHC_DEBUG2("digest from received data: %s (%s)",
              shardcache_hex_escape((char *)&received_digest, sizeof(digest), 0, 0),
              match ? "MATCH" : "MISMATCH");


    return match;
}

// synchronous (blocking)  message reading
int
read_message(int fd,
             char *auth,
             fbuf_t **records,
             int expected_records,
             shardcache_hdr_t *ohdr,
             int ignore_timeout)
{
    uint16_t clen;
    int reading_message = 0;
    unsigned char hdr;
    int csig = 0;
    sip_hash *shash = NULL;
    char version = 0;

    // there is no point in reading the message
    // if we are not interested in any record
    if (expected_records < 1)
        return -1;

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);

    if (auth)
        shash = sip_hash_new((uint8_t *)auth, 2, 4);

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
                if (shash)
                    sip_hash_free(shash);
                return -1;
            }

            if (((ntohl(magic))&0xFFFFFF00) != (SHC_MAGIC&0xFFFFFF00)) {
                SHC_DEBUG("Wrong magic");
                if (shash)
                    sip_hash_free(shash);
                return -1;
            }
            version = ((char *)&magic)[3];
            if (version > SHC_PROTOCOL_VERSION) {
                SHC_WARNING("Unsupported protocol version 0x%02x\n", version);
                if (shash)
                    sip_hash_free(shash);
                return -1;
            }

            rb = read_socket(fd, (char *)&hdr, 1, ignore_timeout);
            if (rb != 1) {
                if (shash)
                    sip_hash_free(shash);
                return -1;
            }

            if (rb == 1) {
                if ((hdr&0xFE) == SHC_HDR_SIGNATURE_SIP) {
                    if (!shash) // no secred is configured but the message is signed
                        return -1;
                    csig = (hdr&0x01);
                    rb = read_socket(fd, (char *)&hdr, 1, ignore_timeout);
                } else if (shash) {
                    // we are expecting a signature header
                    sip_hash_free(shash);
                    return -1;
                }
            }

            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                if (shash)
                    sip_hash_free(shash);
                return -1;
            } else if (rb == -1) {
                continue;
            }

            if (!hdr_check[hdr]) {
                if (shash)
                    sip_hash_free(shash);
                fprintf(stderr, "Unknown message type %02x in read_message()\n", hdr);
                return -1;
            }

            if (shash) {
                sip_hash_update(shash, &hdr, 1);
                if (csig) {
                    if (!read_and_check_siphash_signature(fd, shash)) {
                        sip_hash_free(shash);
                        SHC_WARNING("Can't validate signature (message type %02x) in read_message()", hdr);
                        return -1;
                    }
                }

            }
            if (ohdr)
                *ohdr = hdr;
            reading_message = 1;
        }

        rb = read_socket(fd, (char *)&clen, 2, ignore_timeout);
        // XXX - bug if read only one byte at this point
        if (rb == 2) {
            if (shash)
                sip_hash_update(shash, (uint8_t *)&clen, 2);
            uint16_t chunk_len = ntohs(clen);

            if (chunk_len == 0) {
                unsigned char rsep = 0;
                rb = read_socket(fd, (char *)&rsep, 1, ignore_timeout);
                if (rb != 1) {
                    if (out)
                        fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);
                    return -1;
                }

                if (shash)
                    sip_hash_update(shash, &rsep, 1);

                if (rsep == SHARDCACHE_RSEP) {
                    // go ahead fetching the next record
                    if (shash && csig) {
                        if (!read_and_check_siphash_signature(fd, shash)) {
                            sip_hash_free(shash);
                            if (out)
                                fbuf_set_used(out, initial_len);
                            SHC_WARNING("Unauthorized message type %02x in read_message()", hdr);
                            return -1;
                        }
                    }
                    record_index++;
                    if (record_index == expected_records) {
                        if (shash)
                            sip_hash_free(shash);
                        return record_index + 1;
                    }
                    out = records[record_index];
                } else if (rsep == 0) {
                    if (shash) {
                        if (!read_and_check_siphash_signature(fd, shash)) {
                            sip_hash_free(shash);
                            if (out)
                                fbuf_set_used(out, initial_len);
                            SHC_WARNING("Unauthorized message type %02x in read_message()", hdr);
                            return -1;
                        }
                        sip_hash_free(shash);
                    }
                    return record_index + 1;
                } else {
                    // BOGUS RESPONSE
                    if (out)
                        fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);
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
                        if (shash)
                            sip_hash_free(shash);

                        return -1;
                    }
                    continue;
                } else if (rb == 0) {
                    if (out)
                        fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);

                    return -1;
                }
                chunk_len -= rb;
                if (out)
                    fbuf_add_binary(out, buf, rb);
                if (shash)
                    sip_hash_update(shash, (uint8_t *)buf, rb);
                if (out && fbuf_used(out) > SHARDCACHE_MSG_MAX_RECORD_LEN) {
                    // we have exceeded the maximum size for a record
                    // let's abort this request
                    fprintf(stderr, "Maximum record size exceeded (%dMB)",
                            SHARDCACHE_MSG_MAX_RECORD_LEN >> 20);
                    fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);

                    return -1;
                }
            }

            if (shash && csig) {
                if (!read_and_check_siphash_signature(fd, shash)) {
                    sip_hash_free(shash);
                    if (out)
                        fbuf_set_used(out, initial_len);
                    SHC_WARNING("Unauthorized message type %02x in read_message()", hdr);
                    return -1;
                }
            }
        } else if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
            // ERROR
            break;
        }
    }
    if (shash)
        sip_hash_free(shash);

    return -1;
}

uint64_t
_sign_chunk(sip_hash *shash, void *buf, size_t len)
{
    uint64_t digest;
    sip_hash_update(shash, buf, len);
    if (!sip_hash_final_integer(shash, &digest)) {
        // TODO - Error Messages
        return -1;
    }
    return digest;
}

int
_chunkize_buffer(sip_hash *shash,
                 unsigned char sig_hdr,
                 void *buf,
                 size_t blen,
                 fbuf_t *out)
{
    int ofx = 0;

    do {
        size_t out_initial_offset = fbuf_used(out);
        int writelen = (blen > (size_t)UINT16_MAX) ? UINT16_MAX : blen;
        blen -= writelen;
        uint16_t size = htons(writelen);
        fbuf_add_binary(out, (char *)&size, 2);
        fbuf_add_binary(out, buf + ofx, writelen);
        if (shash && sig_hdr == SHC_HDR_CSIGNATURE_SIP) {
            uint64_t digest = _sign_chunk(shash,
                                          fbuf_data(out) + out_initial_offset,
                                          fbuf_used(out) - out_initial_offset);
            fbuf_add_binary(out, (char *)&digest, sizeof(digest));
        }
        if (blen == 0) {
            uint16_t eor = 0;
            fbuf_add_binary(out, (char *)&eor, 2);
            return 0;
        }
        ofx += writelen;
    } while (blen != 0);

    return -1;
}

int build_message(char *auth,
                  unsigned char sig_hdr,
                  unsigned char hdr,
                  shardcache_record_t *records,
                  int num_records,
                  fbuf_t *out)
{
    static char eom = 0;
    static char sep = SHARDCACHE_RSEP;
    uint16_t    eor = 0;

    uint32_t magic = htonl(SHC_MAGIC);
    fbuf_add_binary(out, (char *)&magic, sizeof(magic));

    sip_hash *shash = NULL;
    if (auth) {
        unsigned char hdr_sig = sig_hdr ? sig_hdr : SHC_HDR_SIGNATURE_SIP;
        fbuf_add_binary(out, (char *)&hdr_sig, 1);
        shash = sip_hash_new((uint8_t *)auth, 2, 4);

    }

    uint16_t out_initial_offset = fbuf_used(out);
    fbuf_add_binary(out, (char *)&hdr, 1);
    if (auth && sig_hdr == SHC_HDR_CSIGNATURE_SIP) {
        uint64_t digest = _sign_chunk(shash, &hdr, 1);
        fbuf_add_binary(out, (char *)&digest, sizeof(digest));
    }

    if (num_records) {
        int i;
        for (i = 0; i < num_records; i++) {
            if (i > 0) {
                fbuf_add_binary(out, &sep, 1);
                if (auth && sig_hdr == SHC_HDR_CSIGNATURE_SIP) {
                    uint64_t digest = _sign_chunk(shash, fbuf_data(out) + fbuf_used(out) - 3, 3);
                    fbuf_add_binary(out, (char *)&digest, sizeof(digest));
                }
            }
            if (records[i].v && records[i].l) {
                if (_chunkize_buffer(shash, sig_hdr, records[i].v, records[i].l, out) != 0) {
                    if (shash)
                        sip_hash_free(shash);
                    return -1;
                }
            } else {
                fbuf_add_binary(out, (char *)&eor, sizeof(eor));
            }
        }
    } else {
        fbuf_add_binary(out, (char *)&eor, sizeof(eor));
    }

    fbuf_add_binary(out, &eom, 1);

    if (auth) {
        if (sig_hdr == SHC_HDR_CSIGNATURE_SIP) {
            uint64_t digest = _sign_chunk(shash, fbuf_data(out) + fbuf_used(out) - 3, 3);
            fbuf_add_binary(out, (char *)&digest, sizeof(digest));
        } else {
            uint64_t digest = _sign_chunk(shash,
                                          fbuf_data(out) + out_initial_offset,
                                          fbuf_used(out) - out_initial_offset);
            fbuf_add_binary(out, (char *)&digest, sizeof(digest));
        }

    }

    if (shash)
        sip_hash_free(shash);
    return 0;
}

int
write_message(int fd,
              char *auth,
              unsigned char sig_hdr,
              unsigned char hdr,
              shardcache_record_t *records,
              int num_records)
{

    fbuf_t msg = FBUF_STATIC_INITIALIZER;

    if (build_message(auth, sig_hdr, hdr, records, num_records, &msg) != 0)
    {
        // TODO - Error Messages
        fbuf_destroy(&msg);
        return -1;
    }

    size_t mlen = fbuf_used(&msg);
    size_t dlen = auth ? sizeof(uint64_t) : 0;
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
                           char *auth,
                           unsigned char sig_hdr,
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
        rc = write_message(fd, auth, sig_hdr, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0 && expect_response) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
                 char *auth,
                 unsigned char sig,
                 void *key,
                 size_t klen,
                 int fd,
                 int expect_response)
{
    return _delete_from_peer_internal(peer, auth, sig, key, klen, 1, fd, expect_response);
}

int
evict_from_peer(char *peer,
                char *auth,
                unsigned char sig,
                void *key,
                size_t klen,
                int fd,
                int expect_response)
{
    return _delete_from_peer_internal(peer, auth, sig, key, klen, 0, fd, expect_response);
}



static inline int
_send_to_peer_internal(char *peer,
                       char *auth,
                       unsigned char sig_hdr,
                       void *key,
                       size_t klen,
                       void *old_value,
                       size_t old_vlen,
                       void *value,
                       size_t vlen,
                       uint32_t expire,
                       int mode, // 0 == SET, 1 == ADD, 2 == CAS
                       int fd,
                       int expect_response)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    int rc = -1;
    if (fd >= 0) {
        shardcache_record_t record[4] = {
            {
                .v = key,
                .l = klen
            },
            {
                .v = value,
                .l = vlen
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
                hdr = SHC_HDR_CAS;
                record[1].v = old_value;
                record[1].l = old_vlen;
                record[2].v = value;
                record[2].l = vlen;
                num_records = 3;
                break;
            default:
                // TODO - Error message for unsupported mode
                SHC_ERROR("Unknown mode %d in %s (%s:%s)", mode, __FUNCTION__, __FILE__, __LINE__);
                return -1;
        }

        uint32_t expire_nbo = 0;
        if (expire) {
            expire_nbo = htonl(expire);
            record[num_records].v = &expire_nbo;
            record[num_records].l = sizeof(expire);
            num_records++;
        }

        // TODO - support cexpire (which can be provided as extra argument to set/add/cas commands)

        rc = write_message(fd, auth, sig_hdr, hdr, record, num_records);
        if (rc != 0) {
            if (should_close)
                close(fd);
            return -1;
        }

        if (rc == 0 && expect_response) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            errno = 0;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
            if (hdr == SHC_HDR_RESPONSE && num_records == 1) {
                SHC_DEBUG2("Got (set) response from peer %s : %s\n",
                          peer, fbuf_data(&resp));
                if (should_close)
                    close(fd);

                char *res = fbuf_data(&resp);
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
                fbuf_destroy(&resp);
                return rc;
            } else {
                fprintf(stderr, "Bad response (%02x) from %s : %s\n",
                        hdr, peer, strerror(errno));
            }
            fbuf_destroy(&resp);
        } else if (rc != 0) {
            fprintf(stderr, "Error reading from socket %d (%s) : %s\n",
                    fd, peer, strerror(errno));
        }
        if (should_close)
            close(fd);
    }
    return rc;
}

int
send_to_peer(char *peer,
             char *auth,
             unsigned char sig,
             void *key,
             size_t klen,
             void *value,
             size_t vlen,
             uint32_t expire,
             int fd,
             int expect_response)
{
    return _send_to_peer_internal(peer,
            auth, sig, key, klen, NULL, 0, value, vlen, expire, 0, fd, expect_response);
}

int
add_to_peer(char *peer,
            char *auth,
            unsigned char sig,
            void *key,
            size_t klen,
            void *value,
            size_t vlen,
            uint32_t expire,
            int fd,
            int expect_response)
{
    return _send_to_peer_internal(peer, auth, sig, key, klen, NULL, 0,
                                  value, vlen, expire, 1, fd, expect_response);
}

int
cas_on_peer(char *peer,
            char *auth,
            unsigned char sig,
            void *key,
            size_t klen,
            void *old_value,
            size_t old_vlen,
            void *value,
            size_t vlen,
            uint32_t expire,
            int fd,
            int expect_response)
{
    return _send_to_peer_internal(peer, auth, sig, key, klen, old_value, old_vlen,
                                  value, vlen, expire, 0, fd, expect_response);
}


int
fetch_from_peer(char *peer,
                char *auth,
                unsigned char sig_hdr,
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
        int rc = write_message(fd, auth, sig_hdr,
                SHC_HDR_GET, &record, 1);
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t *records[3] = { NULL, out, NULL };
            int num_records = read_message(fd, auth, records, 3, &hdr, 0);
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
offset_from_peer(char *peer,
                 char *auth,
                 unsigned char sig_hdr,
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
        int rc = write_message(fd, auth, sig_hdr,
                SHC_HDR_GET_OFFSET, record, 3);

        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t *records[3] = { NULL, out, NULL };
            int num_records = read_message(fd, auth, records, 3, &hdr, 0);
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
               char *auth,
               unsigned char sig_hdr,
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
        rc = write_message(fd, auth, sig_hdr, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0 && expect_response) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
              char *auth,
              unsigned char sig_hdr,
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
        int rc = write_message(fd, auth, sig_hdr, hdr, &record, 1);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
                char *auth,
                unsigned char sig_hdr,
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
        int rc = write_message(fd, auth, sig_hdr,
                SHC_HDR_STATS, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
           char *auth,
           unsigned char sig_hdr,
           int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, sig_hdr, SHC_HDR_CHECK, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
                char *auth,
                unsigned char sig_hdr,
                int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    shardcache_storage_index_t *index = calloc(1, sizeof(shardcache_storage_index_t));
    if (fd >= 0) {
        int rc = write_message(fd, auth, sig_hdr, SHC_HDR_GET_INDEX, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 1);
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
             char *auth,
             unsigned char sig_hdr,
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
        int rc = write_message(fd, auth, sig_hdr, SHC_HDR_MIGRATION_BEGIN, &record, 1);
        if (rc != 0) {
            if (should_close)
                close(fd);
            return -1;
        }

        shardcache_hdr_t hdr = 0;
        fbuf_t resp = FBUF_STATIC_INITIALIZER;
        fbuf_t *respp = &resp;
        int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
                   char *auth,
                   unsigned char sig_hdr,
                   int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, sig_hdr, SHC_HDR_MIGRATION_ABORT, NULL, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            fbuf_t *respp = &resp;
            shardcache_hdr_t hdr = 0;
            int num_records = read_message(fd, auth, &respp, 1, &hdr, 0);
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
