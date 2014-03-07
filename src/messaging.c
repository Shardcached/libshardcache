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
#include "atomic.h"

#include <iomux.h>

#define DEBUG_DUMP_MAXSIZE 128

struct __async_read_ctx_s {
    async_read_callback_t cb;
    shardcache_hdr_t hdr;
    shardcache_hdr_t sig_hdr;
    void *cb_priv;
    char *auth;
    rbuf_t *buf;
    char chunk[65536];
    uint16_t clen;
    uint16_t coff;
    int rnum;
    char state;
    int csig;
    char magic[4];
    char version;
    int moff;
    sip_hash *shash;
    int blocking;
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

int
async_read_context_state(async_read_ctx_t *ctx)
{
    return ctx->state;
}

shardcache_hdr_t
async_read_context_hdr(async_read_ctx_t *ctx)
{
    return ctx->hdr;
}

shardcache_hdr_t
async_read_context_sig_hdr(async_read_ctx_t *ctx)
{
    return ctx->sig_hdr;
}

int
async_read_context_input_data(void *data, int len, async_read_ctx_t *ctx)
{
    if (ctx->state == SHC_STATE_READING_DONE) {
        ctx->state = SHC_STATE_READING_NONE;
        ctx->rnum = 0;
        ctx->moff = 0;
        ctx->version = 0;
        ctx->csig = 0;
        memset(ctx->magic, 0, sizeof(ctx->magic));
    }

    int wb = 0;
    if (data && len)
        wb = rbuf_write(ctx->buf, data, len);

    if (wb != len) {
        fprintf(stderr, "read_message_async buffer underrun!\n");
        ctx->state = SHC_STATE_READING_ERR;
        return -1;
    }

    if (!rbuf_len(ctx->buf))
        return 0;

    if (ctx->state == SHC_STATE_READING_NONE || ctx->state == SHC_STATE_READING_MAGIC)
    {
        unsigned char byte;
        rbuf_read(ctx->buf, &byte, 1);
        while (byte == SHC_HDR_NOOP && rbuf_len(ctx->buf) > 0)
            rbuf_read(ctx->buf, &byte, 1); // skip

        if (byte == SHC_HDR_NOOP && !rbuf_len(ctx->buf))
            return 0;

        ctx->magic[0] = byte;
        ctx->state = SHC_STATE_READING_MAGIC;
        ctx->moff = 1;

        if (rbuf_len(ctx->buf) < sizeof(uint32_t) - ctx->moff) {
            return 0;
        }

        rbuf_read(ctx->buf, &ctx->magic[ctx->moff], sizeof(uint32_t) - ctx->moff);
        uint32_t rmagic;
        memcpy((char *)&rmagic, ctx->magic, sizeof(uint32_t));
        if ((rmagic&0xFFFFFF00) != (htonl(SHC_MAGIC)&0xFFFFFF00)) {
            ctx->state = SHC_STATE_READING_ERR;
            return -1;
        }
        ctx->version = ctx->magic[3];
        if (ctx->version > SHC_PROTOCOL_VERSION) {
            SHC_WARNING("Unsupported protocol version %02x", ctx->version);
            ctx->state = SHC_STATE_READING_ERR;
            return -1;
        }

        ctx->state = SHC_STATE_READING_SIG_HDR;
    }

    if (ctx->state == SHC_STATE_READING_SIG_HDR || ctx->state == SHC_STATE_READING_HDR)
    {
        if (ctx->state == SHC_STATE_READING_SIG_HDR) {
            if (rbuf_len(ctx->buf) < 1)
                return 0;
            rbuf_read(ctx->buf, (unsigned char *)&ctx->sig_hdr, 1);
            if (ctx->sig_hdr == SHC_HDR_SIGNATURE_SIP || ctx->sig_hdr == SHC_HDR_CSIGNATURE_SIP)
            {
                if (!ctx->auth) {
                    ctx->state = SHC_STATE_AUTH_ERR;
                    return -1;
                }

                ctx->state = SHC_STATE_READING_HDR;

                if (ctx->sig_hdr == SHC_HDR_CSIGNATURE_SIP)
                    ctx->csig = 1;

            } else if (ctx->auth) {
                // we are expecting the signature header
                ctx->state = SHC_STATE_AUTH_ERR;
                return -1;
            } else {
                ctx->hdr = ctx->sig_hdr;
                ctx->sig_hdr = 0;
                ctx->state = SHC_STATE_READING_RECORD;
            }
        }
        if (ctx->state == SHC_STATE_READING_HDR) {
            if (rbuf_len(ctx->buf) < 1)
                return 0;
            rbuf_read(ctx->buf, (unsigned char *)&ctx->hdr, 1);
        }

        ctx->state = SHC_STATE_READING_RECORD;
        if (ctx->auth) {
            ctx->shash = sip_hash_new((uint8_t *)ctx->auth, 2, 4);
            sip_hash_update(ctx->shash, (unsigned char *)&ctx->hdr, 1);
        }
    }

    for (;;) {
        if (ctx->state == SHC_STATE_READING_AUTH)
            break;

        if (ctx->coff == ctx->clen && ctx->state == SHC_STATE_READING_RECORD) {
            if (rbuf_len(ctx->buf) < 2)
                break;

            if (ctx->csig) {
                if (rbuf_len(ctx->buf) < SHARDCACHE_MSG_SIG_LEN + 2) // truncated
                    break;

                if (!ctx->shash) {
                    // TODO - Error Messages;
                    ctx->state = SHC_STATE_READING_ERR;
                    return -1;
                }

                uint64_t digest;
                if (!sip_hash_final_integer(ctx->shash, &digest)) {
                    // TODO - Error Messages
                    fprintf(stderr, "Bad signature\n");
                    ctx->state = SHC_STATE_AUTH_ERR;
                    return -1;
                }

                uint64_t received_digest;
                if (rbuf_len(ctx->buf) < sizeof(digest))
                    break;

                rbuf_read(ctx->buf, (u_char *)&received_digest, sizeof(digest));

                if (memcmp(&digest, &received_digest, sizeof(digest)) != 0) {
                    ctx->state = SHC_STATE_AUTH_ERR;
                    return -1;
                }
            }

            // let's call the read_async callback
            if (ctx->clen > 0 && ctx->cb(ctx->chunk, ctx->clen, ctx->rnum, ctx->cb_priv) != 0) {
                ctx->state = SHC_STATE_READING_ERR;
                return -1;
            }

            uint16_t nlen = 0;
            rbuf_read(ctx->buf, (u_char *)&nlen, 2);
            ctx->clen = ntohs(nlen);
            ctx->coff = 0;
            if (ctx->shash)
                sip_hash_update(ctx->shash, (char *)&nlen, 2);
        }
        if (ctx->clen > ctx->coff) {
            int rb = rbuf_read(ctx->buf, ctx->chunk + ctx->coff, ctx->clen - ctx->coff);
            if (ctx->shash)
                sip_hash_update(ctx->shash, ctx->chunk + ctx->coff, rb);
            ctx->coff += rb;
            if (!rbuf_len(ctx->buf))
                break; // TRUNCATED - we need more data
        } else {
            if (rbuf_len(ctx->buf) < 1) {
                // TRUNCATED - we need more data
                ctx->state = SHC_STATE_READING_RSEP;
                break;
            }

            u_char bsep = 0;
            rbuf_read(ctx->buf, &bsep, 1);
            if (ctx->shash)
                sip_hash_update(ctx->shash, (char *)&bsep, 1);

            if (bsep == SHARDCACHE_RSEP) {
                ctx->state = SHC_STATE_READING_RECORD;
                ctx->rnum++;
            } else if (bsep == 0) {
                if (ctx->auth)
                    ctx->state = SHC_STATE_READING_AUTH;
                else
                    ctx->state = SHC_STATE_READING_DONE;
                break;
            } else {
                // unexpected response (contains more than 1 record?)
                ctx->state = SHC_STATE_READING_ERR;
                return -1;
            }
        }
    }

    if (ctx->state == SHC_STATE_READING_AUTH) {
        if (rbuf_len(ctx->buf) < SHARDCACHE_MSG_SIG_LEN)
            return 0;

        if (ctx->shash) {
            uint64_t digest;
            if (!sip_hash_final_integer(ctx->shash, &digest)) {
                // TODO - Error Messages
                fprintf(stderr, "Bad signature\n");
                ctx->state = SHC_STATE_AUTH_ERR;
                return -1;
            }

            uint64_t received_digest;
            rbuf_read(ctx->buf, (char *)&received_digest, sizeof(digest));

            int match = (memcmp(&digest, &received_digest, sizeof(digest)) == 0);

            if (shardcache_log_level() >= LOG_DEBUG) {
                SHC_DEBUG("computed digest for received data: %s",
                          shardcache_hex_escape((char *)&digest, sizeof(digest), 0));

                uint8_t *remote = (uint8_t *)&received_digest;
                SHC_DEBUG("digest from received data: %s (%s)",
                          shardcache_hex_escape(remote, sizeof(digest), 0),
                          match ? "MATCH" : "MISMATCH");
            }

            if (!match) {
                ctx->state = SHC_STATE_AUTH_ERR;
                return -1;
            }
            sip_hash_free(ctx->shash);
            ctx->shash = NULL;
        }
        ctx->state = SHC_STATE_READING_DONE;
    }
    return 0;
}

void
read_async_input_data(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    async_read_ctx_t *ctx = (async_read_ctx_t *)priv;
    async_read_context_input_data(data, len, ctx);
    if (ctx->state == SHC_STATE_READING_DONE || ctx->state == SHC_STATE_READING_NONE) {
        iomux_close(iomux, fd);
    } else if (ctx->state == SHC_STATE_READING_ERR) {
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);
        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);
        fprintf(stderr, "Bad message %02x from %s\n", ctx->hdr, inet_ntoa(saddr.sin_addr));
        iomux_close(iomux, fd);
    } else if (ctx->state == SHC_STATE_AUTH_ERR) {
        // AUTH FAILED
        struct sockaddr_in saddr;
        socklen_t addr_len = sizeof(struct sockaddr_in);

        getpeername(fd, (struct sockaddr *)&saddr, &addr_len);

        fprintf(stderr, "Unauthorized request from %s\n",
                inet_ntoa(saddr.sin_addr));
        iomux_close(iomux, fd);
    }
}

async_read_ctx_t *
async_read_context_create(char *auth,
                          async_read_callback_t cb,
                          void *priv)
{
    async_read_ctx_t *ctx = calloc(1, sizeof(async_read_ctx_t));
    // NOTE: we will accumulate accumulate at most two chunks because
    //       of misalignment
    ctx->buf = rbuf_create(1<<17);
    ctx->cb = cb;
    ctx->cb_priv = priv;
    ctx->auth = auth;
    return ctx;
}

void
async_read_context_destroy(async_read_ctx_t *ctx)
{
    rbuf_destroy(ctx->buf);
    free(ctx);
}

static void
read_async_input_eof(iomux_t *iomux, int fd, void *priv)
{
    async_read_ctx_t *ctx = (async_read_ctx_t *)priv;

    if (ctx->state == SHC_STATE_READING_DONE || ctx->state == SHC_STATE_READING_NONE)
        ctx->cb(NULL, 0, -1, ctx->cb_priv);
    else
        ctx->cb(NULL, 0, -2, ctx->cb_priv);

    if (!ctx->blocking)
        iomux_end_loop(iomux);

    async_read_context_destroy(ctx);
}

int
read_message_async(int fd,
                   iomux_t *iomux,
                   char *auth,
                   async_read_callback_t cb,
                   void *priv)
{
    struct timeval iomux_timeout = { 0, 20000 }; // 20ms
    async_read_ctx_t *ctx = async_read_context_create(auth, cb, priv);

    iomux_callbacks_t cbs = {
        .mux_input = read_async_input_data,
        .mux_eof = read_async_input_eof,
        .priv = ctx
    };

    if (!iomux) {
        // if no iomux has been passed, we need to create
        // one, in which case we will block until the whole
        // message has been read
        iomux= iomux_create();
        if (!iomux) {
            async_read_context_destroy(ctx);
            return -1;
        }
        ctx->blocking = 1;
    }

    iomux_add(iomux, fd, &cbs);

    if (ctx->blocking) {
        // we are in blocking mode, let's wait for the job
        // to be completed
        for (;;) {
            iomux_run(iomux, &iomux_timeout);
            if (iomux_isempty(iomux))
                break;
        }

        iomux_destroy(iomux);

        char state = ctx->state;

        if (state == SHC_STATE_READING_ERR) {
            return -1;
        }
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
    // any idx >= 0 refers to the record index
    
    int ret;
    if (idx == 0)
        ret = arg->cb(arg->peer, arg->key, arg->klen, data, len, 0, arg->priv);
    else
        ret = arg->cb(arg->peer, arg->key, arg->klen, NULL, 0, (idx != -1), arg->priv);

    if (idx < 0) {
        // if the reading is finished or there was an error
        // we need to release the helper_arg structure
        // and eventually close the filedescriptor
        if (arg->fd >= 0)
            close(arg->fd);
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
                      fetch_from_peer_async_cb cb,
                      void *priv,
                      int fd,
                      iomux_t *iomux)
{
    int rc = -1;
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        message_record_t record = {
            .v = key,
            .l = klen
        };
        rc = write_message(fd, auth, sig_hdr, SHC_HDR_GET_ASYNC, &record, 1);
        if (rc == 0) {
            fetch_from_peer_helper_arg_t *arg = calloc(1, sizeof(fetch_from_peer_helper_arg_t));
            arg->peer = peer;
            arg->key = malloc(klen);
            memcpy(arg->key, key, klen);
            arg->klen = klen;
            arg->fd = should_close ? fd : -1;
            arg->cb = cb;
            arg->priv = priv;
            rc = read_message_async(fd, iomux, auth, fetch_from_peer_helper, arg);
        }
    }
    return rc;
}

static int
read_and_check_siphash_signature(int fd, sip_hash *shash)
{
    uint64_t digest, received_digest;

    int rb = read_socket(fd, (char *)&received_digest, sizeof(received_digest));
    if (rb != sizeof(received_digest)) {
        SHC_WARNING("Truncated message (expected signature)");
        return -1;
    }
    if (!sip_hash_final_integer(shash, &digest)) {
        SHC_ERROR("Errors computing the siphash digest");
        return -1;
    }

    int match = (memcmp(&digest, &received_digest, sizeof(digest)) == 0);

    SHC_DEBUG("computed digest for received data: %s",
            shardcache_hex_escape((unsigned char *)&digest, sizeof(digest), 0));

    SHC_DEBUG("digest from received data: %s (%s)",
              shardcache_hex_escape((unsigned char *)&received_digest, sizeof(digest), 0),
              match ? "MATCH" : "MISMATCH");


    return match;
}

// synchronous (blocking)  message reading
int
read_message(int fd, char *auth, fbuf_t *out, shardcache_hdr_t *ohdr)
{
    uint16_t clen;
    int initial_len = fbuf_used(out);;
    int reading_message = 0;
    unsigned char hdr;
    int csig = 0;
    sip_hash *shash = NULL;
    char version = 0;

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);

    if (auth)
        shash = sip_hash_new(auth, 2, 4);

    for(;;) {
        int rb;

        if (reading_message == 0) {
            uint32_t magic = 0;
            do {
                rb = read_socket(fd, &hdr, 1);
            } while (rb == 1 && hdr == SHC_HDR_NOOP);

            ((char *)&magic)[0] = hdr;
            rb = read_socket(fd, ((char *)&magic)+1, sizeof(magic)-1);
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

            rb = read_socket(fd, &hdr, 1);
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
                    rb = read_socket(fd, &hdr, 1);
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

            if (hdr != SHC_HDR_GET &&
                hdr != SHC_HDR_GET &&
                hdr != SHC_HDR_DELETE &&
                hdr != SHC_HDR_EVICT &&
                hdr != SHC_HDR_GET_ASYNC &&
                hdr != SHC_HDR_GET_OFFSET &&
                hdr != SHC_HDR_ADD &&
                hdr != SHC_HDR_EXISTS &&
                hdr != SHC_HDR_TOUCH &&
                hdr != SHC_HDR_MIGRATION_BEGIN &&
                hdr != SHC_HDR_MIGRATION_ABORT &&
                hdr != SHC_HDR_MIGRATION_END &&
                hdr != SHC_HDR_CHECK &&
                hdr != SHC_HDR_STATS &&
                hdr != SHC_HDR_GET_INDEX &&
                hdr != SHC_HDR_INDEX_RESPONSE &&
                hdr != SHC_HDR_RESPONSE)
            {
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

        rb = read_socket(fd, (char *)&clen, 2);
        // XXX - bug if read only one byte at this point
        if (rb == 2) {
            if (shash)
                sip_hash_update(shash, (char *)&clen, 2);
            uint16_t chunk_len = ntohs(clen);

            if (chunk_len == 0) {
                unsigned char rsep = 0;
                rb = read_socket(fd, &rsep, 1);
                if (rb != 1) {
                    fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);
                    return -1;
                }

                if (shash)
                    sip_hash_update(shash, &rsep, 1);

                if (rsep == SHARDCACHE_RSEP) {
                    // go ahead fetching the next record
                    // XXX - should we separate the records in the output buffer?
                    if (shash && csig) {
                        if (!read_and_check_siphash_signature(fd, shash)) {
                            sip_hash_free(shash);
                            fbuf_set_used(out, initial_len);
                            SHC_WARNING("Unauthorized message type %02x in read_message()", hdr);
                            return -1;
                        }
                    }
                    continue;
                } else if (rsep == 0) {
                    if (shash) {
                        if (!read_and_check_siphash_signature(fd, shash)) {
                            sip_hash_free(shash);
                            fbuf_set_used(out, initial_len);
                            SHC_WARNING("Unauthorized message type %02x in read_message()", hdr);
                            return -1;
                        }
                        sip_hash_free(shash);
                    }
                    return 0;
                } else {
                    // BOGUS RESPONSE
                    fbuf_set_used(out, initial_len);
                    sip_hash_free(shash);
                    return -1;
                }
            }

            while (chunk_len != 0) {
                char buf[chunk_len];
                rb = read_socket(fd, buf, chunk_len);
                if (rb == -1) {
                    if (errno != EINTR && errno != EAGAIN) {
                        // ERROR
                        fbuf_set_used(out, initial_len);
                        if (shash)
                            sip_hash_free(shash);

                        return -1;
                    }
                    continue;
                } else if (rb == 0) {
                    fbuf_set_used(out, initial_len);
                    if (shash)
                        sip_hash_free(shash);

                    return -1;
                }
                chunk_len -= rb;
                fbuf_add_binary(out, buf, rb);
                if (shash)
                    sip_hash_update(shash, buf, rb);
                if (fbuf_used(out) > SHARDCACHE_MSG_MAX_RECORD_LEN) {
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
                  message_record_t *records,
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
        shash = sip_hash_new(auth, 2, 4);

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
              message_record_t *records,
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
    SHC_DEBUG("sending message: %s",
           shardcache_hex_escape(fbuf_data(&msg), mlen-dlen, DEBUG_DUMP_MAXSIZE));

    if (dlen)
    if (dlen && fbuf_used(&msg) >= dlen) {
        SHC_DEBUG("computed digest: %s",
                  shardcache_hex_escape(fbuf_end(&msg)-dlen, dlen, 0));
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


static int
_delete_from_peer_internal(char *peer,
                           char *auth,
                           unsigned char sig_hdr,
                           void *key,
                           size_t klen,
                           int owner,
                           int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    SHC_DEBUG("Sending del command to peer %s (owner: %d)", peer, owner);
    if (fd >= 0) {
        unsigned char hdr = owner ? SHC_HDR_DELETE : SHC_HDR_EVICT;
        message_record_t record = {
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
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                SHC_DEBUG("Got (del) response from peer %s: %02x\n",
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
    return -1;
}

int
delete_from_peer(char *peer,
                 char *auth,
                 unsigned char sig,
                 void *key,
                 size_t klen,
                 int fd)
{
    return _delete_from_peer_internal(peer, auth, sig, key, klen, 1, fd);
}

int
evict_from_peer(char *peer,
                char *auth,
                unsigned char sig,
                void *key,
                size_t klen,
                int fd)
{
    return _delete_from_peer_internal(peer, auth, sig, key, klen, 0, fd);
}



int
_send_to_peer_internal(char *peer,
                       char *auth,
                       unsigned char sig_hdr,
                       void *key,
                       size_t klen,
                       void *value,
                       size_t vlen,
                       uint32_t expire,
                       int add,
                       int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    if (fd >= 0) {
        message_record_t record[3] = {
            {
                .v = key,
                .l = klen
            },
            {
                .v = value,
                .l = vlen
            }
        };

        uint32_t expire_nbo = 0;
        if (expire) {
            expire_nbo = htonl(expire);
            record[2].v = &expire_nbo;
            record[2].l = sizeof(expire);
        }
        int rc = write_message(fd, auth, sig_hdr,
                               add ? SHC_HDR_ADD : SHC_HDR_SET,
                               record, expire ? 3 : 2);
        if (rc != 0) {
            if (should_close)
                close(fd);
            return -1;
        }

        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            errno = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                SHC_DEBUG("Got (set) response from peer %s : %s\n",
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
        } else {
            fprintf(stderr, "Error reading from socket %d (%s) : %s\n",
                    fd, peer, strerror(errno));
        }
        if (should_close)
            close(fd);
    }
    return -1;
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
             int fd)
{
    return _send_to_peer_internal(peer,
            auth, sig, key, klen, value, vlen, expire, 0, fd);
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
            int fd)
{
    return _send_to_peer_internal(peer, auth, sig, key, klen,
                                  value, vlen, expire, 1, fd);
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
        message_record_t record = {
            .v = key,
            .l = len
        };
        int rc = write_message(fd, auth, sig_hdr,
                SHC_HDR_GET, &record, 1);
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, out, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                if (fbuf_used(out)) {
                    char keystr[1024];
                    memcpy(keystr, key, len < 1024 ? len : 1024);
                    keystr[len] = 0;
                    SHC_DEBUG("Got new data from peer %s : %s => %s", peer, keystr,
                              shardcache_hex_escape(fbuf_data(out), fbuf_used(out), DEBUG_DUMP_MAXSIZE));
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
        message_record_t record[3] = {
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
            rc = read_message(fd, auth, out, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                if (fbuf_used(out)) {
                    char keystr[1024];
                    memcpy(keystr, key, len < 1024 ? len : 1024);
                    keystr[len] = 0;
                    SHC_DEBUG("Got new data from peer %s : %s => %s", peer, keystr,
                              shardcache_hex_escape(fbuf_data(out), fbuf_used(out), DEBUG_DUMP_MAXSIZE));
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
               int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, ATOMIC_READ(_tcp_timeout));
        should_close = 1;
    }

    SHC_DEBUG("Sending exists command to peer %s", peer);
    if (fd >= 0) {
        unsigned char hdr = SHC_HDR_EXISTS;
        message_record_t record = {
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
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                SHC_DEBUG("Got (exists) response from peer %s : %s\n",
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
    return -1;
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

    SHC_DEBUG("Sending touch command to peer %s", peer);
    if (fd >= 0) {
        unsigned char hdr = SHC_HDR_TOUCH;
        message_record_t record = {
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
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
                SHC_DEBUG("Got (touch) response from peer %s : %s\n",
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
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
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
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {
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
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_INDEX_RESPONSE && rc == 0) {
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
        message_record_t record = {
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
        rc = read_message(fd, auth, &resp, &hdr);
        if (hdr == SHC_HDR_RESPONSE && rc == 0) {
            SHC_DEBUG("Got (del) response from peer %s : %s",
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
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHC_HDR_RESPONSE && rc == 0) {

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
    int fd = open_connection(address_string, SHARDCACHE_PORT_DEFAULT, timeout);
    return fd;
}
