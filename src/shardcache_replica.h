#ifndef SHARDCACHE_REPLICA_H
#define SHARDCACHE_REPLICA_H

#include "shardcache.h"
#include "messaging.h"

typedef enum {
    SHARDCACHE_REPLICA_OP_SET             = 0x01,
    SHARDCACHE_REPLICA_OP_ADD             = 0x02,
    SHARDCACHE_REPLICA_OP_CAS             = 0x03,
    SHARDCACHE_REPLICA_OP_DELETE          = 0x04,
    SHARDCACHE_REPLICA_OP_EVICT           = 0x05,
    SHARDCACHE_REPLICA_OP_MIGRATION_BEGIN = 0x06,
    SHARDCACHE_REPLICA_OP_MIGRATION_ABORT = 0x07,
    SHARDCACHE_REPLICA_OP_MIGRATION_END   = 0x08
} shardcache_replica_operation_t;

typedef struct _shardcache_replica_s shardcache_replica_t;

typedef struct _shardcache_replica_command_s shardcache_replica_command_t;

shardcache_replica_t *shardcache_replica_create(shardcache_t *shc,
                                                shardcache_node_t *node,
                                                int my_index,
                                                char *wrkdir);

void shardcache_replica_destroy(shardcache_replica_t *replica);

int shardcache_replica_dispatch(shardcache_replica_t *replica,
                                shardcache_replica_operation_t op,
                                void *key,
                                size_t klen,
                                void *data,
                                size_t dlen,
                                void *data2,
                                size_t dlen2,
                                uint32_t expire,
                                uint32_t cexpire);

shardcache_hdr_t
shardcache_replica_received_command(shardcache_replica_t *replica,
                                    shardcache_hdr_t hdr,
                                    void *cmd,
                                    size_t cmdlen,
                                    void **response,
                                    size_t *response_len);

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
