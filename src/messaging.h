#ifndef __MESSAGING_H__
#define __MESSAGING_H__

#include <sys/types.h>

/* protocol specification
 *
 *  GET_MESSAGE      : <AUTH><HDR><KEY>
 *  SET_MESSAGE      : <AUTH><HDR><KEY><VALUE>
 *  DEL_MESSAGE      : <AUTH><HDR><KEY>
 *  EVI_MESSAGE      : <AUTH><HDR><KEY>
 *  RESP_MESSAGE     : <HDR><VALUE>
 *
 *  AUTH      : <BYTE>[20]
 *  HDR       : MSG_GET | MSG_SET | MSG_DEL | MSG_RESP
 *  MSG_GET   : 0x01
 *  MSG_SET   : 0x02
 *  MSG_DEL   : 0x03
 *  MSG_EVI   : 0x04
 *  MSG_RESP  : 0x11
 *  KEY       : <RECORD>
 *  VALUE     : <RECORD>
 *  RECORD    : <SIZE><DATA>[<SIZE><DATA>...]<EOR>
 *  SIZE      : <HIGH_BYTE><LOW_BYTE>
 *  DATA      : <BYTE>...<BYTE>
 *  EOR       : <NULL_BYTE><NULL_BYTE>
 *  HIGH_BYTE : BYTE
 *  LOW_BYTE  : BYTE
 *  NULL_BYTE : 0x00
 *  BYTE      : 0x00 - 0xFF
 */

// in byte
#define SHARDCACHE_MSG_SIG_LEN 8
#define SHARDCACHE_MSG_MAX_RECORD_LEN (1<<28) // 256MB

typedef enum {
    SHARDCACHE_HDR_GET  = 0x01,
    SHARDCACHE_HDR_SET  = 0x02,
    SHARDCACHE_HDR_DEL  = 0x03,
    SHARDCACHE_HDR_EVI  = 0x04,
    SHARDCACHE_HDR_RES  = 0x11
} shardcache_hdr_t;

#define SHARDCACHE_RSEP 0x80

int read_message(int fd, char *auth, fbuf_t *out, shardcache_hdr_t *hdr);
int write_message(int fd, char *auth, char hdr, void *k, size_t klen, void *v, size_t vlen);
int delete_from_peer(char *peer, char *auth, void *key, size_t klen, int owner);
int send_to_peer(char *peer, char *auth, void *key, size_t klen, void *value, size_t vlen);
int fetch_from_peer(char *peer, char *auth, void *key, size_t len, fbuf_t *out);

#endif
