#ifndef __MESSAGING_H__
#define __MESSAGING_H__

#include <sys/types.h>
#include "shardcache.h"

/* protocol specification
 *
 * MESSAGE      : [<SIG_HDR> | <CSIG_HDR>]<MSG>[<SIG>] | <NOOP>
 * SIG_HDR      : 0xF0
 * CSIG_HDR     : 0xF1
 * MSG          : <HDR><RECORD>[<RSEP><RECORD>...]<EOM>
 * NOOP         : <MSG_NOP>
 * HDR          : <MSG_GET> | <MSG_SET> | <MSG_DEL> | <MSG_EVI> |
 *                <MSG_MGB> | <MSG_MGA> | <MSG_MGE> | <MSG_RES>
 * MSG_GET      : 0x01
 * MSG_SET      : 0x02
 * MSG_DEL      : 0x03
 * MSG_EVI      : 0x04
 * MSG_OFX      : 0x05
 * MSG_MGA      : 0x21
 * MSG_MGB      : 0x22
 * MSG_MGE      : 0x23
 * MSG_CHK      : 0x31
 * MSG_STS      : 0x32
 * MSG_IDG      : 0x41
 * MSG_IDR      : 0x42
 * MSG_NOP      : 0x90
 * MSG_RES      : 0x99
 * RSEP         : 0x80
 * NULL_RECORD  : <EOR>
 * RECORD       : <SIZE><DATA>[<SIZE><DATA>...]<EOR>
 * SIZE         : <WORD>
 * WORD         : <HIGH_BYTE><LOW_BYTE>
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
 * INDEX        : <RECORD>
 * 
 * The only valid/supported messages are :
 * 
 * GET_MESSAGE  : <MSG_GET><KEY><EOM>
 * GET_ASYNC    : <MSG_GETA><KEY><EOM>
 * GET_OFFSET   : <MSG_GTO><KEY><LONG_SIZE><LONG_SIZE><EOM>
 * SET_MESSAGE  : <MSG_SET><KEY><RSEP><VALUE>[<RSEP><TTL>]<EOM>
 * DEL_MESSAGE  : <MSG_DEL><KEY><EOM>
 * EVI_MESSAGE  : <MSG_EVI><KEY><EOM>
 * RES_MESSAGE  : <MSG_RES><RECORD><EOM>
 *
 * MGB_MESSAGE  : <MSG_MGB><NODES_LIST><EOM>
 * MGA_MESSAGE  : <MSG_MGA><NULL_RECORD><EOM>
 * MGE_MESSAGE  : <MSG_MGE><NULL_RECORD><EOM>
 * 
 * STS_MESSAGE  : <MSG_STS><NULL_RECORD><EOM>
 * CHK_MESSAGE  : <MSG_PNG><NULL_RECORD><EOM>
 * 
 * IDG_MESSAGE  : <MSG_IDG><NULL_RECORD><EOM>
 * IDR_MESSAGE  : <MSG_IDR><INDEX><EOM>
 */

// in bytes
#define SHARDCACHE_MSG_SIG_LEN 8
#define SHARDCACHE_MSG_MAX_RECORD_LEN (1<<28) // 256MB

typedef enum {
    SHARDCACHE_HDR_GET      = 0x01,
    SHARDCACHE_HDR_SET      = 0x02,
    SHARDCACHE_HDR_DEL      = 0x03,
    SHARDCACHE_HDR_EVI      = 0x04,
    SHARDCACHE_HDR_GTA      = 0x05,
    SHARDCACHE_HDR_GTO      = 0x06,
    SHARDCACHE_HDR_MGA      = 0x21,
    SHARDCACHE_HDR_MGB      = 0x22,
    SHARDCACHE_HDR_MGE      = 0x23,
    SHARDCACHE_HDR_CHK      = 0x31,
    SHARDCACHE_HDR_STS      = 0x32,
    SHARDCACHE_HDR_IDG      = 0x41,
    SHARDCACHE_HDR_IDR      = 0x42,
    SHARDCACHE_HDR_NOP      = 0x90,
    SHARDCACHE_HDR_RES      = 0x99,
    SHARDCACHE_HDR_SIG_SIP  = 0xF0,
    SHARDCACHE_HDR_CSIG_SIP = 0xF1
} shardcache_hdr_t;

#define SHARDCACHE_RSEP 0x80

int read_message(int fd, char *auth, fbuf_t *out, shardcache_hdr_t *hdr);

int write_message(int fd,
                  char *auth,
                  unsigned char sig_hdr,
                  unsigned char hdr,
                  void *k,
                  size_t klen,
                  void *v,
                  size_t vlen,
                  uint32_t expire);

int build_message(char *auth,
                  unsigned char sig_hdr,
                  unsigned char hdr,
                  void *k,
                  size_t klen,
                  void *v,
                  size_t vlen,
                  uint32_t expire,
                  fbuf_t *out);

int delete_from_peer(char *peer,
                     char *auth,
                     unsigned char sig_hdr,
                     void *key,
                     size_t klen,
                     int owner,
                     int fd);

int send_to_peer(char *peer,
                 char *auth,
                 unsigned char sig_hdr,
                 void *key,
                 size_t klen,
                 void *value,
                 size_t vlen,
                 uint32_t expire,
                 int fd);

int fetch_from_peer(char *peer,
                    char *auth,
                    unsigned char sig_hdr,
                    void *key,
                    size_t len,
                    fbuf_t *out,
                    int fd);

int stats_from_peer(char *peer,
                    char *auth,
                    unsigned char sig_hdr,
                    char **out,
                    size_t *len,
                    int fd);

int check_peer(char *peer,
               char *auth,
               unsigned char sig_hdr,
               int fd);

int migrate_peer(char *peer,
                 char *auth,
                 unsigned char sig_hdr,
                 void *msgdata,
                 size_t len,
                 int fd);

int abort_migrate_peer(char *peer, char *auth, unsigned char sig_hdr, int fd);

int connect_to_peer(char *address_string, unsigned int timeout);
// NOTE: caller must use shardcache_free_index() to release memory used
//       by the returned shardcache_storage_index_t pointer
shardcache_storage_index_t *index_from_peer(char *peer,
                                            char *auth,
                                            unsigned char sig_hdr,
                                            int fd);

typedef void (*async_read_callback_t)(void *data,
                                      size_t len,
                                      int  idx,
                                      void *priv);


typedef struct __async_read_ctx_s async_read_ctx_t;

async_read_ctx_t *async_read_context_create(char *auth,
                                            async_read_callback_t cb,
                                            void *priv);
void async_read_context_destroy(async_read_ctx_t *ctx);

typedef enum {
    SHC_STATE_READING_NONE    = 0x00,
    SHC_STATE_READING_HDR     = 0x01,
    SHC_STATE_READING_RECORD  = 0x02,
    SHC_STATE_READING_RSEP    = 0x03,
    SHC_STATE_READING_AUTH    = 0x04,
    SHC_STATE_READING_DONE    = 0x05,
    SHC_STATE_READING_ERR     = 0x06,
    SHC_STATE_AUTH_ERR        = 0x07
} async_read_context_state_t;

int async_read_context_state(async_read_ctx_t *ctx);
shardcache_hdr_t async_read_context_hdr(async_read_ctx_t *ctx);
shardcache_hdr_t async_read_context_sig_hdr(async_read_ctx_t *ctx);

void async_read_context_input_data(void *data, int len, async_read_ctx_t *ctx);

int read_message_async(int fd,
                       char *auth,
                       async_read_callback_t cb,
                       void *priv);

typedef void (*fetch_from_peer_async_cb)(char *peer,
                                         void *key,
                                         size_t klen,
                                         void *data,
                                         size_t len,
                                         void *priv);


int fetch_from_peer_async(char *peer,
                          char *auth,
                          unsigned char sig_hdr,
                          void *key,
                          size_t len,
                          fetch_from_peer_async_cb cb,
                          void *priv,
                          int fd);

#endif
