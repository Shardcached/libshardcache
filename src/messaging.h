#ifndef __MESSAGING_H__
#define __MESSAGING_H__

#include <sys/types.h>
#include "shardcache.h"

/* protocol specification
 *
 * MESSAGE      : <MSG>[<SIG>] | <NOOP>
 * MSG          : <HDR><RECORD>[<RSEP><RECORD>...]<EOM>
 * NOOP         : <MSG_NOP>
 * HDR          : <MSG_GET> | <MSG_SET> | <MSG_DEL> | <MSG_EVI> |
 *                <MSG_MGB> | <MSG_MGA> | <MSG_MGE> | <MSG_RES>
 * MSG_GET      : 0x01
 * MSG_SET      : 0x02
 * MSG_DEL      : 0x03
 * MSG_EVI      : 0x04
 * MSG_MGA      : 0x21
 * MSG_MGB      : 0x22
 * MSG_MGE      : 0x23
 * MSG_NOP      : 0x90
 * MSG_RES      : 0x99
 * RSEP         : 0x80
 * RECORD       : <SIZE><DATA>[<SIZE><DATA>...]<EOR>
 * SIZE         : <HIGH_BYTE><LOW_BYTE>
 * EOR          : <NULL_BYTE><NULL_BYTE>
 * HIGH_BYTE    : <BYTE>
 * LOW_BYTE     : <BYTE>
 * DATA         : <BYTE>...<BYTE>
 * BYTE         : 0x00 - 0xFF
 * NULL_BYTE    : 0x00
 * EOM          : <NULL_BYTE>
 * SIG          : <BYTE>[8]
 * KEY          : <RECORD>
 * VALUE        : <RECORD>
 * TTL          : <RECORD>
 * NODES_LIST   : <RECORD>
 * 
 * The only supported messages are :
 * 
 * GET_MESSAGE  : <MSG_GET><KEY><EOM>[<SIG>]
 * SET_MESSAGE  : <MSG_SET><KEY><RSEP><VALUE>[<RSEP><TTL>]<EOM>[<SIG>]
 * DEL_MESSAGE  : <MSG_DEL><KEY><EOM>[<SIG>]
 * EVI_MESSAGE  : <MSG_EVI><KEY><EOM>[<SIG>]
 * RES_MESSAGE  : <MSG_RES><RECORD><EOM>[<SIG>]

 * MGB_MESSAGE  : <MSG_MGB><NODES_LIST><EOM>[<SIG>]
 * MGA_MESSAGE  : <MSG_MGA><EOM>[<SIG>]
 * MGE_MESSAGE  : <MSG_MGE><EOM>[<SIG>]
 * 
 * STS_MESSAGE  : <MSG_STS><EOM>[<SIG>]
 * CHK_MESSAGE  : <MSG_PNG><EOM>[<SIG>]
 * 
 * IDX_MESSAGE  : <MSG_IDX><EOM>[<SIG>]
 */

// in byte
#define SHARDCACHE_MSG_SIG_LEN 8
#define SHARDCACHE_MSG_MAX_RECORD_LEN (1<<28) // 256MB

typedef enum {
    SHARDCACHE_HDR_GET  = 0x01,
    SHARDCACHE_HDR_SET  = 0x02,
    SHARDCACHE_HDR_DEL  = 0x03,
    SHARDCACHE_HDR_EVI  = 0x04,
    SHARDCACHE_HDR_MGA  = 0x21,
    SHARDCACHE_HDR_MGB  = 0x22,
    SHARDCACHE_HDR_MGE  = 0x23,
    SHARDCACHE_HDR_CHK  = 0x31,
    SHARDCACHE_HDR_STS  = 0x32,
    SHARDCACHE_HDR_IDG  = 0x41,
    SHARDCACHE_HDR_IDR  = 0x42,
    SHARDCACHE_HDR_NOP  = 0x90,
    SHARDCACHE_HDR_RES  = 0x99
} shardcache_hdr_t;

#define SHARDCACHE_RSEP 0x80

int read_message(int fd, char *auth, fbuf_t *out, shardcache_hdr_t *hdr);
int write_message(int fd, char *auth, char hdr, void *k, size_t klen, void *v, size_t vlen, uint32_t expire);
int build_message(char hdr, void *k, size_t klen, void *v, size_t vlen, uint32_t expire, fbuf_t *out);
int delete_from_peer(char *peer, char *auth, void *key, size_t klen, int owner, int fd);
int send_to_peer(char *peer, char *auth, void *key, size_t klen, void *value, size_t vlen, uint32_t expire, int fd);
int fetch_from_peer(char *peer, char *auth, void *key, size_t len, fbuf_t *out, int fd);
int stats_from_peer(char *peer, char *auth, char **out, size_t *len, int fd);
int check_peer(char *peer, char *auth, int fd);
int migrate_peer(char *peer, char *auth, void *msgdata, size_t len, int fd);
int connect_to_peer(char *address_string, unsigned int timeout);
// NOTE: caller must use shardcache_free_index() to release memory used
//       by the returned shardcache_storage_index_t pointer
shardcache_storage_index_t *index_from_peer(char *peer, char *auth, int fd);

#endif
