#include <fbuf.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "messaging.h"
#include "connections.h"
#include "groupcache.h"

int read_message(int fd, fbuf_t *out, groupcache_hdr_t *hdr) {
    uint16_t chunk_len;
    int reading_message = 0;
    for(;;) {
        int rb;

        if (reading_message == 0 && hdr) {
            rb = read_socket(fd, (char *)hdr, 1);
            if (rb == 0 || (rb == -1 && errno != EINTR && errno != EAGAIN)) {
                return -1;
            }
            if (*hdr != GROUPCACHE_HDR_GET &&
                *hdr != GROUPCACHE_HDR_SET &&
                *hdr != GROUPCACHE_HDR_DEL &&
                *hdr != GROUPCACHE_HDR_RES)
            {
                return -1;
            }
            reading_message = 1;
        }

        rb = read_socket(fd, (char *)&chunk_len, 2);
        if (rb == 2) {
            chunk_len = ntohs(chunk_len);
            if (chunk_len == 0) {
                return fbuf_used(out);
            }

            int initial_len = chunk_len;
            while (chunk_len != 0) {
                char buf[chunk_len];
                rb = read_socket(fd, buf, chunk_len);
                if (rb == -1) {
                    if (errno != EINTR && errno != EAGAIN) {
                        // ERROR 
                        fbuf_set_used(out, fbuf_used(out) - (initial_len - chunk_len));
                        return -1;
                    }
                } else if (rb == 0) {
                    fbuf_set_used(out, fbuf_used(out) - (initial_len - chunk_len));
                    return -1;
                }
                chunk_len -= rb;
                fbuf_add_binary(out, buf, rb);
            }
        } else if (rb == -1 && errno != EINTR && errno != EAGAIN) {
            // ERROR 
            break;
        } else if (rb == 0) {
            break;
        } 
    }
    return -1;
}

int write_message(int fd, char hdr, void *v, size_t vlen)  {
    int wb;

    if (hdr > 0) {
        wb = write_socket(fd, (char *)&hdr, 1);
        if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
            return -1;
        }
    }

    if (v && vlen) {
        do {
            int writelen = (vlen > (size_t)UINT16_MAX) ? UINT16_MAX : vlen;
            vlen -= writelen;
            uint16_t size = htons(writelen);
            wb = write_socket(fd, (char *)&size, 2);
            if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
                return -1;
            } else if (wb == 2) {
                int wrote = 0;
                while (wrote != writelen) {
                    wb = write_socket(fd, v, writelen);
                    if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
                        return -1;
                    }
                    wrote += wb;
                }
            }
        } while (vlen != 0);
    }
    uint16_t terminator = 0;
    wb = write_socket(fd, (char *)&terminator, 2);
    if (wb == 2)
        return 0;
    return -1;
}


int delete_from_peer(char *peer, void *key, size_t klen) {
    char *brkt;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, GROUPCACHE_HDR_DEL, key, klen);
        if (rc != 0) {
            close(fd);
            return -1;
        }

        groupcache_hdr_t hdr = 0;
        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            int rb = read_message(fd, &resp, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rb > 0) {
#ifdef DEBUG_GROUPCACHE
                fprintf(stderr, "Got (set) response from peer %s : %s\n", peer, fbuf_data(&resp));
#endif
                close(fd);
                fbuf_destroy(&resp);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}


int send_to_peer(char *peer, void *key, size_t klen, void *value, size_t vlen) {
    char *brkt;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, GROUPCACHE_HDR_SET, key, klen);
        if (rc != 0) {
            close(fd);
            return -1;
        }

        groupcache_hdr_t hdr = 0;
        rc = write_message(fd, 0, value, vlen);

        if (rc == 0) {
            fbuf_t resp = FBUF_STATIC_INITIALIZER;
            int rb = read_message(fd, &resp, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rb > 0) {
#ifdef DEBUG_GROUPCACHE
                fprintf(stderr, "Got (set) response from peer %s : %s\n", peer, fbuf_data(&resp));
#endif
                close(fd);
                fbuf_destroy(&resp);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}

int fetch_from_peer(char *peer, void *key, size_t len, fbuf_t *out) {
    char *brkt;
    char *addr = strdup(peer);
    char *host = strtok_r(addr, ":", &brkt);
    char *port_string = strtok_r(NULL, ":", &brkt);
    int port = port_string ? atoi(port_string) : GROUPCACHE_PORT_DEFAULT;
    int fd = open_connection(host, port, 30);
    free(addr);

    if (fd >= 0) {
        int rc = write_message(fd, GROUPCACHE_HDR_GET, key, len);
        if (rc == 0) {
            groupcache_hdr_t hdr = 0;
            int rb = read_message(fd, out, &hdr);
            if (hdr == GROUPCACHE_HDR_RES && rb > 0) {
#ifdef DEBUG_GROUPCACHE
                // XXX - casting to (char *) here is dangerous ...
                //       but this would happen only in debugging
                //       so let's assume we know what we are doing
                fprintf(stderr, "Got new data from peer %s : %s => %s \n", peer, key, fbuf_data(out));
#endif
                close(fd);
                return 0;
            } else {
                // TODO - Error messages
            }
        }
        close(fd);
    }
    return -1;
}


