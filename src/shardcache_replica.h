#ifndef __SHARDCACHE_REPLICA_H__
#define __SHARDCACHE_REPLICA_H__

#include "shardcache.h"
#include "messaging.h"

typedef enum {
    SHARDCACHE_REPLICA_SEND,
    SHARDCACHE_REPLICA_RECEIVE
} shardcache_replica_event_t;

typedef struct __shardcache_replica_s shardcache_replica_t;

typedef struct __shardcache_replica_command_s shardcache_replica_command_t;

shardcache_replica_t *shardcache_replica_create(shardcache_node_t *node, char *me);
void shardcache_replica_destroy(shardcache_replica_t *replica);
int shardcache_replica_dispatch(shardcache_replica_command_t *cmd);

shardcache_replica_command_t *
shardcache_replica_command_create(shardcache_replica_event_t hdr, void *msg, size_t len);

void shardcache_replica_command_destroy(shardcache_replica_command_t *cmd);

typedef enum {
    SHARDCACHE_REPLICA_STATUS_ONLINE = 0,
    SHARDCACHE_REPLICA_STATUS_OFFLINE,
    SHARDCACHE_REPLICA_STATUS_UNKNOWN
} shardcache_replica_status_t;

shardcache_replica_status_t  shardcache_replica_status(shardcache_replica_t *replica, char *addr);

shardcache_replica_status_t shardcache_replica_set_status(shardcache_replica_t *replica,
                                                          char *addr,
                                                          shardcache_replica_status_t status);


#endif
