#ifndef __PROTOCOL_H__
#define __PROTOCOL_H__

/* For the protocol specification check the 'docs/protocol.txt' file
 * in the libshardcache source distribution
 */

// in bytes
#define SHARDCACHE_MSG_SIG_LEN 8
#define SHARDCACHE_MSG_MAX_RECORD_LEN (1<<28) // 256MB

// last byte holds the protocol version
#define SHC_PROTOCOL_VERSION 2
#define SHC_MAGIC 0x73686300 | SHC_PROTOCOL_VERSION

typedef enum {
    // data commands
    SHC_HDR_GET              = 0x01,
    SHC_HDR_SET              = 0x02,
    SHC_HDR_DELETE           = 0x03,
    SHC_HDR_EVICT            = 0x04,
    SHC_HDR_GET_ASYNC        = 0x05,
    SHC_HDR_GET_OFFSET       = 0x06,
    SHC_HDR_ADD              = 0x07,
    SHC_HDR_EXISTS           = 0x08,
    SHC_HDR_TOUCH            = 0x09,
    SHC_HDR_CAS              = 0x0A,

    // multi commands
    SHC_HDR_GET_MULTI        = 0x0B,
    SHC_HDR_SET_MULTI        = 0x0C,
    SHC_HDR_DELETE_MULTI     = 0x0D,
    SHC_HDR_EVICT_MULTI      = 0x0E,

    // atomic commands (assuming that the value is a 64bit integer)
    SHC_HDR_INCREMENT        = 0x10,
    SHC_HDR_DECREMENT        = 0x11,

    // migration commands
    SHC_HDR_MIGRATION_ABORT  = 0x21,
    SHC_HDR_MIGRATION_BEGIN  = 0x22,
    SHC_HDR_MIGRATION_END    = 0x23,

    // administrative commands
    SHC_HDR_CHECK            = 0x31,
    SHC_HDR_STATS            = 0x32,

    // index-related commands
    SHC_HDR_GET_INDEX        = 0x41,
    SHC_HDR_INDEX_RESPONSE   = 0x42,

    // no-op (for ping/health-check)
    SHC_HDR_NOOP             = 0x90,

    // generic response header
    SHC_HDR_RESPONSE         = 0x99,

    SHC_HDR_REPLICA_COMMAND  = 0xA0,
    SHC_HDR_REPLICA_RESPONSE = 0xA1,
    SHC_HDR_REPLICA_PING     = 0xA2,
    SHC_HDR_REPLICA_ACK      = 0xA3,

    // signature headers
    SHC_HDR_SIGNATURE_SIP    = 0xF0,
    SHC_HDR_CSIGNATURE_SIP   = 0xF1

} shardcache_hdr_t;

typedef enum {
    SHC_RES_OK     = 0x00,
    SHC_RES_YES    = 0x01,
    SHC_RES_EXISTS = 0x02,
    SHC_RES_NO     = 0xFE,
    SHC_RES_ERR    = 0xFF
} shardcache_res_t;

typedef struct __shardcache_record_s {
    void *v;
    size_t l;
} shardcache_record_t;

#define SHARDCACHE_EOM 0x00
#define SHARDCACHE_RSEP 0x80

#endif
