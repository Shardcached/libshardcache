#include "shardcache_replica.h"
#include "queue.h"
#include "atomic.h"

struct __shardcache_replica_command_s {
    shardcache_hdr_t hdr;
    void *msg;
    size_t len;
};

struct __shardcache_replica_s {
    queue_t *cmd_queue;
    shardcache_node_t *node;
    pthread_t replicator;
    int quit;
};


static void *
replicator(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    while(!ATOMIC_READ(replica->quit)) {
        shardcache_replica_command_t *cmd = queue_pop_left(replica->cmd_queue);
        while (cmd) {

            shardcache_replica_command_t *cmd = queue_pop_left(replica->cmd_queue);
        }
    }
    return NULL;
}

shardcache_replica_t *
shardcache_replica_create(shardcache_node_t *node)
{
    shardcache_replica_t *replica = calloc(1, sizeof(shardcache_replica_t));

    replica->cmd_queue = queue_create();
    queue_set_free_value_callback(replica->cmd_queue,
            (queue_free_value_callback_t)shardcache_replica_command_destroy);

    replica->node = shardcache_node_copy(node);

    int rc = pthread_create(&replica->replicator, NULL, replicator, replica);
    if (rc != 0) {
        queue_destroy(replica->cmd_queue);
        shardcache_node_destroy(replica->node);
        free(replica);
        return NULL;
    }

    return replica;
}

void
shardcache_replica_destroy(shardcache_replica_t *replica)
{
    ATOMIC_INCREMENT(replica->quit);
    pthread_join(replica->replicator, NULL);
    queue_destroy(replica->cmd_queue);
    shardcache_node_destroy(replica->node);
    free(replica);
}


shardcache_replica_command_t *
shardcache_replica_command_create(shardcache_hdr_t hdr, void *msg, size_t len)
{
    shardcache_replica_command_t *cmd = calloc(1, sizeof(shardcache_replica_command_t));
    cmd->hdr = hdr;
    cmd->msg = malloc(len);
    memcpy(cmd->msg, msg, len);
    cmd->len = len;
    return cmd;
}

void
shardcache_replica_command_destroy(shardcache_replica_command_t *cmd)
{
    free(cmd->msg);
    free(cmd);
}
