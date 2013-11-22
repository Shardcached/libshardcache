#include <sys/types.h>

typedef enum {
    GROUPCACHE_HDR_GET  = 0x01,
    GROUPCACHE_HDR_SET  = 0x02,
    GROUPCACHE_HDR_DEL  = 0x03,
    GROUPCACHE_HDR_RES  = 0x11
} groupcache_hdr_t;

int read_message(int fd, fbuf_t *out, groupcache_hdr_t *hdr);
int write_message(int fd, char hdr, void *v, size_t vlen);
int delete_from_peer(char *peer, void *key, size_t klen);
int send_to_peer(char *peer, void *key, size_t klen, void *value, size_t vlen);
int fetch_from_peer(char *peer, void *key, size_t len, fbuf_t *out);

