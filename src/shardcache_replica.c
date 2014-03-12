#include "shardcache_replica.h"
#include <hashtable.h>
#include <linklist.h>
#include <queue.h>
#include <fbuf.h>

#include "atomic.h"
#include "kepaxos.h"
#include "shardcache_internal.h"

#define SHARDCACHE_REPLICA_WRKDIR_DEFAULT "/tmp/shcrpl"
#define KEPAXOS_LOG_FILENAME "kepaxos_log.db"

struct __shardcache_replica_s {
    shardcache_t *shc;
    shardcache_node_t *node;
    char *me;
    int num_replicas;
    kepaxos_t *kepaxos;
};


static int
kepaxos_send(char **recipients,
             int num_recipients,
             void *cmd,
             size_t cmd_len,
             void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    int i;
    for (i = 0; i < num_recipients; i++) {
        // TODO - parallelize
        int fd = connect_to_peer(recipients[i], 2);
        if (fd < 0)
            continue;
        shardcache_record_t record = {
            .v = cmd,
            .l = cmd_len
        };
        int rc = write_message(fd, (char *)replica->shc->auth, 0, SHC_HDR_REPLICA_CMD, &record, 1);
        if (rc == 0) {
            fbuf_t out = FBUF_STATIC_INITIALIZER;
            shardcache_hdr_t hdr;
            rc = read_message(fd, (char *)replica->shc->auth, &out, &hdr);
            if (rc == 0 && hdr == SHC_HDR_REPLICA_CMD) {
                kepaxos_received_command(replica->kepaxos, recipients[i], fbuf_data(&out), fbuf_used(&out));
            }
        }
    }
    return 0;
}

static int
kepaxos_commit(unsigned char type,
               void *key,
               size_t klen,
               void *data,
               size_t dlen,
               void *priv)
{
    return 0;
}

static int
kepaxos_recover(char *peer, void *key, size_t klen, void *priv)
{
    return 0;
}


shardcache_replica_t *
shardcache_replica_create(shardcache_t *shc,
                          shardcache_node_t *node,
                          char *me,
                          char *wrkdir)
{
    shardcache_replica_t *replica = calloc(1, sizeof(shardcache_replica_t));

    replica->node = shardcache_node_copy(node);

    replica->me = strdup(me);

    replica->num_replicas = shardcache_node_num_addresses(node);

    SPIN_INIT(&replica->view_lock);

    replica->shc = shc;

    // TODO - check wrkdir exists and is writeable
    char dbfile[2048];
    snprintf(dbfile, sizeof(dbfile), "%s/%s", wrkdir, KEPAXOS_LOG_FILENAME);

    char **peers = malloc(sizeof(char *) * replica->num_replicas);
    int num_peers = shardcache_node_get_all_addresses(replica->node, peers,  replica->num_replicas);

    kepaxos_callbacks_t kepaxos_callbacks = {
        .send = kepaxos_send,
        .commit = kepaxos_commit,
        .recover = kepaxos_recover
    };
    replica->kepaxos = kepaxos_context_create(dbfile, peers, num_peers, &kepaxos_callbacks);

    return replica;
}

void
shardcache_replica_destroy(shardcache_replica_t *replica)
{
    shardcache_node_destroy(replica->node);
    free(replica->me);
    free(replica);
}

int shardcache_replica_dispatch(shardcache_replica_operation_t op, void *key, size_t klen, void *data, size_t dlen)
{

    return 0;
}

/*
shardcache_replica_status_t
shardcache_replica_status(shardcache_replica_t *replica, char *addr)
{
    shardcache_replica_node_t *node = ht_get(replica->status, addr, strlen(addr), NULL);
    if (!node)
        return SHARDCACHE_REPLICA_STATUS_UNKNOWN;

    return node->status;
}

shardcache_replica_status_t
shardcache_replica_set_status(shardcache_replica_t *replica,
                              char *addr,
                              shardcache_replica_status_t status)
{
    shardcache_replica_node_t *node = ht_get(replica->status, addr, strlen(addr), NULL);
    if (!node)
        return SHARDCACHE_REPLICA_STATUS_UNKNOWN;

    shardcache_replica_status_t st = node->status;
    node->status = status;
    return st;
}
*/



