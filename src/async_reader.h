#ifndef __ASYNC_READER_H__
#define __ASYNC_READER_H__

#include <sys/types.h>
#include <iomux.h>
#include <rbuf.h>
#include "protocol.h"

// idx = -1 , data == NULL, len = 0 when finished
// idx = -2 , data == NULL, len = 0 if an error occurred
typedef int (*async_read_callback_t)(void *data,
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
    SHC_STATE_READING_MAGIC   = 0x01,
    SHC_STATE_READING_SIG_HDR = 0x02,
    SHC_STATE_READING_HDR     = 0x03,
    SHC_STATE_READING_RECORD  = 0x04,
    SHC_STATE_READING_RSEP    = 0x05,
    SHC_STATE_READING_AUTH    = 0x06,
    SHC_STATE_READING_DONE    = 0x07,
    SHC_STATE_READING_ERR     = 0x08,
    SHC_STATE_AUTH_ERR        = 0x09
} async_read_context_state_t;

int async_read_context_state(async_read_ctx_t *ctx);
shardcache_hdr_t async_read_context_hdr(async_read_ctx_t *ctx);
shardcache_hdr_t async_read_context_sig_hdr(async_read_ctx_t *ctx);
char async_read_context_protocol_version(async_read_ctx_t *ctx);

async_read_context_state_t async_read_context_consume_data(async_read_ctx_t *ctx, rbuf_t *input);
async_read_context_state_t async_read_context_input_data(async_read_ctx_t *ctx, void *data, int len, int *processed);
async_read_context_state_t async_read_context_update(async_read_ctx_t *ctx);

// NOTE: when the async_read_wrk_t param is provided to the following
//       functions accepting it, they will return immediately and 
//       initialize the given pointer so that the caller can
//       use the callbacks and the context to create a new iomux
//       connection and retreive the data asynchronously.
//       If the async_read_wrk_t param is not provided the functions
//       will block until the response has been fully retrieved
#pragma pack(push, 1)
typedef struct {
    async_read_ctx_t *ctx;
    iomux_callbacks_t cbs;
    int fd;
} async_read_wrk_t;
#pragma pack(pop)

int read_async_input_data(iomux_t *iomux, int fd, unsigned char *data, int len, void *priv);
void read_async_input_eof(iomux_t *iomux, int fd, void *priv);
void read_async_timeout(iomux_t *iomux, int fd, void *priv);

#endif
