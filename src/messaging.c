#include <fbuf.h>
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

// synchronous (blocking)  message reading
int read_message(int fd, char *auth, fbuf_t *out, shardcache_hdr_t *ohdr)
{
    uint16_t clen;
    int initial_len = fbuf_used(out);;
    int reading_message = 0;
    unsigned char hdr;
    sip_hash *shash = NULL;

    if (auth)
        shash = sip_hash_new(auth, 2, 4);

    for(;;) {
        int rb;

        if (reading_message == 0) {
            rb = read_socket(fd, &hdr, 1);
            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                if (shash)
                    sip_hash_free(shash);
                return -1;
            } else if (rb == -1) {
                continue;
            }
            if (hdr != SHARDCACHE_HDR_GET &&
                hdr != SHARDCACHE_HDR_SET &&
                hdr != SHARDCACHE_HDR_DEL &&
                hdr != SHARDCACHE_HDR_EVI &&
                hdr != SHARDCACHE_HDR_MGB &&
                hdr != SHARDCACHE_HDR_MGA &&
                hdr != SHARDCACHE_HDR_MGE &&
                hdr != SHARDCACHE_HDR_CHK &&
                hdr != SHARDCACHE_HDR_STS &&
                hdr != SHARDCACHE_HDR_IDG &&
                hdr != SHARDCACHE_HDR_IDR &&
                hdr != SHARDCACHE_HDR_RES)
            {
                if (shash)
                    sip_hash_free(shash);
                fprintf(stderr, "Uknown message type %02x in read_message()\n", hdr);
                return -1;
            }
            if (shash)
                sip_hash_update(shash, &hdr, 1);
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
                    continue;
                } else if (rsep == 0) {
                    if (shash) {
                        char sig[SHARDCACHE_MSG_SIG_LEN];
                        int ofx = 0;
                        do {
                            rb = read_socket(fd, &sig[ofx], SHARDCACHE_MSG_SIG_LEN-ofx);
                            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                                fbuf_set_used(out, initial_len);
                                if (shash)
                                    sip_hash_free(shash);
                                return -1;
                            } else if (rb == -1) {
                                continue;
                            }
                            ofx += rb;
                        } while (ofx != SHARDCACHE_MSG_SIG_LEN);

                        uint64_t digest;
                        if (!sip_hash_final_integer(shash, &digest)) {
                            // TODO - Error Messages
                            fbuf_set_used(out, initial_len);
                            sip_hash_free(shash);
                            return -1;
                        }

#ifdef SHARDCACHE_DEBUG
                        int i;
                        fprintf(stderr, "computed digest for received data: (%s) ", auth);
                        for (i=0; i<8; i++) {
                            fprintf(stderr, "%02x", (unsigned char)((char *)&digest)[i]);
                        }
                        fprintf(stderr, "\n");

                        fprintf(stderr, "digest from received data: ");
                        uint8_t *remote = sig;
                        for (i=0; i<8; i++) {
                            fprintf(stderr, "%02x", remote[i]);
                        }
                        fprintf(stderr, "\n");
#endif

                        if (memcmp(&digest, &sig, SHARDCACHE_MSG_SIG_LEN) != 0) {
                            struct sockaddr_in saddr;
                            socklen_t addr_len = sizeof(struct sockaddr_in);
                            getpeername(fd, (struct sockaddr *)&saddr, &addr_len);

                            fprintf(stderr, "Unauthorized message from %s\n",
                            inet_ntoa(saddr.sin_addr));
                            fbuf_set_used(out, initial_len);
                            sip_hash_free(shash);
                            return -1;
                            // AUTH FAILED
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
        } else if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
            // ERROR 
            break;
        }
    }
    if (shash)
        sip_hash_free(shash);
    return -1;
}

int _chunkize_buffer(void *buf, size_t blen, fbuf_t *out)
{
    int orig_used = fbuf_used(out);
    int ofx = 0;
    do {
        int writelen = (blen > (size_t)UINT16_MAX) ? UINT16_MAX : blen;
        blen -= writelen;
        uint16_t size = htons(writelen);
        int wb = fbuf_add_binary(out, (char *)&size, 2);
        if (wb == -1) {
            return -1;
        } else if (wb == 2) {
            int wrote = 0;
            while (wrote != writelen) {
                wb = fbuf_add_binary(out, buf + ofx + wrote, writelen - wrote);
                if (wb == -1) {
                    // discard what written so far
                    fbuf_set_used(out, orig_used);
                    return -1;
                }
                wrote += wb;
            }
            if (blen == 0 && wrote == writelen) {
                uint16_t eor = 0;
                fbuf_add_binary(out, (char *)&eor, 2);
                return 0;
            }
            ofx += wrote;
        }
    } while (blen != 0);
    return -1;
}

int build_message(char hdr, void *k, size_t klen, void *v, size_t vlen, uint32_t expire, fbuf_t *out)
{
    static char eom = 0;
    static char sep = SHARDCACHE_RSEP;
    uint16_t    eor = 0;

    fbuf_clear(out);
    fbuf_add_binary(out, &hdr, 1);
    if (k && klen) {
        if (_chunkize_buffer(k, klen, out) != 0)
            return -1;
    } else {
        fbuf_add_binary(out, (char *)&eor, sizeof(eor));
        fbuf_add_binary(out, &eom, 1);
        return 0;
    }
    if (hdr == SHARDCACHE_HDR_SET) {
        if (v && vlen) {
            fbuf_add_binary(out, &sep, 1);

            if (_chunkize_buffer(v, vlen, out) != 0)
                return -1;

            if (expire) {
                uint16_t clen = htons(sizeof(uint32_t));
                uint32_t exp = htonl(expire);
                fbuf_add_binary(out, &sep, 1);
                fbuf_add_binary(out, (char *)&clen, sizeof(clen));
                fbuf_add_binary(out, (char *)&exp, sizeof(exp));
                fbuf_add_binary(out, (char *)&eor, sizeof(eor));
            }
        } else {
            fbuf_add_binary(out, (char *)&eor, sizeof(eor));
        }
    }
    fbuf_add_binary(out, &eom, 1);
    return 0;
}

int write_message(int fd, char *auth, char hdr, void *k, size_t klen, void *v, size_t vlen, uint32_t expire)
{

    fbuf_t msg = FBUF_STATIC_INITIALIZER;
    if (build_message(hdr, k, klen, v, vlen, expire, &msg) != 0) {
        // TODO - Error Messages
        fbuf_destroy(&msg);
        return -1;
    }

    if (auth) {
        uint64_t digest;
        size_t dlen = sizeof(digest);
        sip_hash *shash = sip_hash_new(auth, 2, 4);
        sip_hash_digest_integer(shash, fbuf_data(&msg), fbuf_used(&msg), &digest);
        sip_hash_free(shash);
        fbuf_add_binary(&msg, (char *)&digest, dlen);

#ifdef SHARDCACHE_DEBUG
        int i;
        fprintf(stderr, "sending message: ");
        size_t mlen = fbuf_used(&msg);
        if (mlen > 256)
           mlen = 256;
        for (i = 0; i < mlen - dlen; i++) {
            fprintf(stderr, "%02x", (unsigned char)(fbuf_data(&msg))[i]);
        }
        if (mlen < fbuf_used(&msg))
            fprintf(stderr, "...");
        fprintf(stderr, "\n");

        fprintf(stderr, "computed digest: ");
        for (i=0; i < dlen; i++) {
            fprintf(stderr, "%02x", (unsigned char)((char *)&digest)[i]);
        }
        fprintf(stderr, "\n");
#endif
    }

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


int delete_from_peer(char *peer, char *auth, void *key, size_t klen, int owner, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Sending del command to peer %s (owner: %d)\n",
            peer, owner);
#endif
    if (fd >= 0) {
        char hdr = owner ? SHARDCACHE_HDR_DEL : SHARDCACHE_HDR_EVI;
        int rc = write_message(fd, auth, hdr, key, klen, NULL, 0, 0);

        // if we are not forwarding a delete command to the owner
        // of the key, but only an eviction request to a peer,
        // we don't need to wait for the response
        if (owner && rc == 0) {
            shardcache_hdr_t hdr = 0;
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
#ifdef SHARDCACHE_DEBUG
                fprintf(stderr, "Got (del) response from peer %s : %s\n",
                        peer, fbuf_data(&resp));
#endif
                if (should_close)
                    close(fd);
                fbuf_destroy(&resp);
                return 0;
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


int send_to_peer(char *peer, char *auth, void *key,
        size_t klen, void *value, size_t vlen, uint32_t expire, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_SET,
                key, klen, value, vlen, expire);
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
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
#ifdef SHARDCACHE_DEBUG
                fprintf(stderr, "Got (set) response from peer %s : %s\n",
                        peer, fbuf_data(&resp));
#endif
                if (should_close)
                    close(fd);
                fbuf_destroy(&resp);
                return 0;
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

int fetch_from_peer(char *peer, char *auth, void *key, size_t len, fbuf_t *out, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_GET, key, len, NULL, 0, 0);
        if (rc == 0) {
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, out, &hdr);
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
#ifdef SHARDCACHE_DEBUG
                if (fbuf_used(out)) {
                    char keystr[1024];
                    memcpy(keystr, key, len < 1024 ? len : 1024);
                    keystr[len] = 0;
                    fprintf(stderr, "Got new data from peer %s : %s => ", peer, keystr);
                    int i;
                    char *datap = fbuf_data(out);
                    size_t datalen = fbuf_used(out);
                    if (datalen > 256)
                        datalen = 256;
                    for (i = 0; i < datalen; i++)
                        fprintf(stderr, "%02x", datap[i]); 
                    if (datalen < fbuf_used(out))
                        fprintf(stderr, "...");
                    fprintf(stderr, "\n");
                }
#endif
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

int stats_from_peer(char *peer, char *auth, char **out, size_t *len, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_STS, NULL, 0, NULL, 0, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
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

int check_peer(char *peer, char *auth, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_CHK, NULL, 0, NULL, 0, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
                if (fbuf_used(&resp) == 2 && memcmp(fbuf_data(&resp), "OK", 2) == 0) {
                    if (should_close)
                        close(fd);
                    return 0;
                }
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

shardcache_storage_index_t *index_from_peer(char *peer, char *auth, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    shardcache_storage_index_t *index = calloc(1, sizeof(shardcache_storage_index_t));
    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_IDG, NULL, 0, NULL, 0, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHARDCACHE_HDR_IDR && rc == 0) {
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

int migrate_peer(char *peer, char *auth, void *msgdata, size_t len, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

#ifdef SHARDCACHE_DEBUG
    fprintf(stderr, "Sending migration_begin command to peer %s\n", peer);
#endif
    if (fd >= 0) {
        int rc = write_message(fd,
                               auth,
                               SHARDCACHE_HDR_MGB,
                               msgdata,
                               len,
                               NULL,
                               0, 0);

        shardcache_hdr_t hdr = 0;
        fbuf_t resp = FBUF_STATIC_INITIALIZER;
        rc = read_message(fd, auth, &resp, &hdr);
        if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
#ifdef SHARDCACHE_DEBUG
            fprintf(stderr, "Got (del) response from peer %s : %s\n",
                    peer, fbuf_data(&resp));
#endif
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

int abort_migrate_peer(char *peer, char *auth, int fd)
{
    int should_close = 0;
    if (fd < 0) {
        fd = connect_to_peer(peer, 30);
        should_close = 1;
    }

    if (fd >= 0) {
        int rc = write_message(fd, auth, SHARDCACHE_HDR_MGA, NULL, 0, NULL, 0, 0);
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            shardcache_hdr_t hdr = 0;
            rc = read_message(fd, auth, &resp, &hdr);
            if (hdr == SHARDCACHE_HDR_RES && rc == 0) {
                if (fbuf_used(&resp) == 2 && memcmp(fbuf_data(&resp), "OK", 2) == 0) {
                    if (should_close)
                        close(fd);
                    return 0;
                }
            }
        }
        if (should_close)
            close(fd);
    }
    return -1;
}

int connect_to_peer(char *address_string, unsigned int timeout)
{
    char *brkt = NULL;
    char *addr = strdup(address_string);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : SHARDCACHE_PORT_DEFAULT;

    int fd = open_connection(host, port, 30);

    free(addr);
    return fd;
}
