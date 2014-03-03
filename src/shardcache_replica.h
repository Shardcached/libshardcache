#ifndef __SHARDCACHE_REPLICA_H__
#define __SHARDCACHE_REPLICA_H__

#include "shardcache.h"
#include "messaging.h"

typedef enum {
    SHARDCACHE_REPLICA_OP_SET,
    SHARDCACHE_REPLICA_OP_DELETE,
    SHARDCACHE_REPLICA_OP_MIGRATION_BEGIN,
    SHARDCACHE_REPLICA_OP_MIGRATION_ABORT,
    SHARDCACHE_REPLICA_OP_MIGRATION_COMPLETE
} shardcache_replica_operation_t;

typedef enum {
    SHARDCACHE_REPLICA_RECEIVE,
    SHARDCACHE_REPLICA_SEND
} shardcache_replica_event_t;

typedef struct __shardcache_replica_s shardcache_replica_t;

typedef struct __shardcache_replica_command_s shardcache_replica_command_t;

shardcache_replica_t *shardcache_replica_create(shardcache_t *shc,
                                                shardcache_node_t *node,
                                                char *me,
                                                char *wrkdir);

void shardcache_replica_destroy(shardcache_replica_t *replica);
int shardcache_replica_dispatch(shardcache_replica_command_t *cmd);

shardcache_replica_command_t *
shardcache_replica_command_create(shardcache_replica_event_t hdr,
                                  shardcache_replica_operation_t op,
                                  shardcache_record_t *records,
                                  int num_records);

void shardcache_replica_command_destroy(shardcache_replica_command_t *cmd);

typedef enum {
    SHARDCACHE_REPLICA_STATUS_ONLINE = 0,
    SHARDCACHE_REPLICA_STATUS_OFFLINE,
    SHARDCACHE_REPLICA_STATUS_UNKNOWN
} shardcache_replica_status_t;

typedef enum {
    SHARDCACHE_REPLICA_ROLE_ACCEPTOR,
    SHARDCACHE_REPLICA_ROLE_PROPOSER
} shardcache_replica_role_t;

shardcache_replica_status_t  shardcache_replica_status(shardcache_replica_t *replica, char *addr);

shardcache_replica_status_t shardcache_replica_set_status(shardcache_replica_t *replica,
                                                          char *addr,
                                                          shardcache_replica_status_t status);


#endif
