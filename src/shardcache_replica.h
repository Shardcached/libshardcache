#ifndef __SHARDCACHE_REPLICA_H__
#define __SHARDCACHE_REPLICA_H__

#include "shardcache.h"
#include "messaging.h"

typedef struct __shardcache_replica_s shardcache_replica_t;

typedef struct __shardcache_replica_command_s shardcache_replica_command_t;

shardcache_replica_t *shardcache_replica_create(shardcache_node_t *node);
void shardcache_replica_destroy(shardcache_replica_t *replica);
int shardcache_replica_dispatch(shardcache_replica_command_t *cmd);

shardcache_replica_command_t *
shardcache_replica_command_create(shardcache_hdr_t hdr, void *msg, size_t len);

void shardcache_replica_command_destroy(shardcache_replica_command_t *cmd);

#endif
