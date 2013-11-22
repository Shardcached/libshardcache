#include <sys/types.h>

typedef enum {
    GROUPCACHE_CMD_GET      = 0x01,
    GROUPCACHE_CMD_SET      = 0x03,
    GROUPCACHE_CMD_DEL      = 0x04,
    GROUPCACHE_CMD_RESPONSE = 0x11
} groupcache_cmd_t;

int read_message(int fd, fbuf_t *out, groupcache_cmd_t *cmd);
int write_message(int fd, char cmd, void *v, size_t vlen);
int delete_from_peer(char *peer, void *key, size_t klen);
int send_to_peer(char *peer, void *key, size_t klen, void *value, size_t vlen);
int fetch_from_peer(char *peer, void *key, size_t len, fbuf_t *out);

