#include "shardcache_replica.h"

#include <hashtable.h>
#include <linklist.h>
#include <fbuf.h>
#include <pqueue.h>
#include <iomux.h>

#include <atomic_defs.h>
#include "kepaxos.h"
#include "shardcache_internal.h"
#include "counters.h"

#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <errno.h>

#define SHARDCACHE_REPLICA_WRKDIR_DEFAULT "/tmp/shcrpl"
#define KEPAXOS_LOG_FILENAME "kepaxos_log.db"

#define MSG_WRITE_UINT64(__m, __o, __n) \
{ \
    *((uint32_t *)((__m) + (__o))) = htonl((__n) >> 32); \
    (__o) += sizeof(uint32_t); \
    *((uint32_t *)((__m) + (__o))) = htonl((__n) & 0x00000000FFFFFFFF); \
    (__o) += sizeof(uint32_t); \
}

#define MSG_WRITE_UINT32(__m, __o, __n) \
{ \
        *((uint32_t *)((__m) + (__o))) = htonl((__n)); \
        (__o) += sizeof(uint32_t); \
}

#define MSG_WRITE_POINTER(__m, __o, __p, __l) \
{ \
    memcpy((__m) + (__o), (__p), (__l)); \
    (__o) += (__l); \
}

#define MSG_READ_UINT64(__m, __n) { \
    (__n) = ((uint64_t)ntohl(*((uint32_t *)(__m))) << 32) | \
            ntohl(*((uint32_t *)((__m) + sizeof(uint32_t)))); \
    (__m) +=  2 * sizeof(uint32_t); \
}

#define MSG_READ_UINT32(__m, __n) { \
    (__n) = ntohl(*((uint32_t *)(__m))); \
    (__m) += sizeof(uint32_t); \
}

#define MSG_READ_POINTER(__m, __p, __l) { \
    if ((__l) > 0) { \
        (__p) = (__m); \
        (__m) += (__l); \
    } else { \
        (__p) = NULL; \
    } \
}

struct __shardcache_replica_s {
    shardcache_t *shc;        // a valid shardcache instance
    shardcache_node_t *node;  // the shardcache node (union of all replicas)
    char *me;                 // myself (among the node replicas)
    int num_replicas;         // the number of replicase
    kepaxos_t *kepaxos;       // a valid kepaxos context
    hashtable_t *recovery;    // teomporary store for keys being recovered
    pqueue_t *recovery_queue; // priority queue with the items to recover
    struct {
        uint64_t recovering;
        uint64_t ballot;
        uint64_t commits;
        uint64_t commit_fails;
        uint64_t dispached;
        uint64_t responses;
        uint64_t commands;
        uint64_t acks;
    } counters; // counters exported to libshardcache
    int quit; // tells both the recovery and the async-io threads when to exit
    pthread_t recover_th; // the recovery thread
    pthread_t async_io_th; // the async-io thread
    iomux_t *iomux; // the iomux used by the async-io thread
};

typedef struct {
    char *peer;
    void *key;
    size_t klen;
    uint64_t ballot;
    uint64_t seq;
} shardcache_item_to_recover_t;

typedef struct {
    size_t len;
    uint32_t expire;
    char data; // first byte of the data
} kepaxos_data_t;

typedef struct {
    shardcache_replica_t *replica;
    async_read_ctx_t *ctx;
    char *peer;
    int fd;
    fbuf_t input;
    fbuf_t output;
} kepaxos_connection_t;

typedef struct {
    void *key;
    size_t len;
} kepaxos_key_t;

static void
shardcache_replica_received_ack(shardcache_replica_t *replica, void *msg, size_t len);

static int
shardcache_replica_received_ping(shardcache_replica_t *replica,
                                 void *cmd,
                                 size_t cmdlen,
                                 void **response,
                                 size_t *response_len);



static int
kepaxos_connection_append_input_data(void *data,
                                     size_t len,
                                     int  idx,
                                     void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    if (idx == 0) {
        fbuf_add_binary(&connection->input, data, len);
    }
    return 0;
}

static int
kepaxos_connection_input(iomux_t *iomux, int fd, unsigned char *data, int len, void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    shardcache_replica_t *replica = connection->replica;

    int processed = 0;

    int read_state = async_read_context_input_data(connection->ctx, data, len, &processed);
    if (read_state == SHC_STATE_READING_DONE ||
        read_state == SHC_STATE_READING_ERR  ||
        read_state == SHC_STATE_AUTH_ERR)
    {
        shardcache_hdr_t hdr = async_read_context_hdr(connection->ctx);
        if (hdr == SHC_HDR_REPLICA_RESPONSE || hdr == SHC_HDR_REPLICA_ACK)
        {
            if (hdr == SHC_HDR_REPLICA_RESPONSE) {
                ATOMIC_INCREMENT(replica->counters.responses);
                kepaxos_received_response(replica->kepaxos, fbuf_data(&connection->input), fbuf_used(&connection->input));
            } else {
                ATOMIC_INCREMENT(replica->counters.acks);
                shardcache_replica_received_ack(replica, fbuf_data(&connection->input), fbuf_used(&connection->input));
            }
            iomux_remove(iomux, fd);
            shardcache_release_connection_for_peer(replica->shc, connection->peer, fd);
            async_read_context_destroy(connection->ctx);
            free(connection);
        }
        else if (hdr == SHC_HDR_REPLICA_ACK) {
            iomux_remove(iomux, fd);
            shardcache_release_connection_for_peer(replica->shc, connection->peer, fd);
            async_read_context_destroy(connection->ctx);
            free(connection);
        } else {
            // TODO - Error message for unexpected response
            iomux_close(iomux, fd);
        }
    }
    return processed;
}

static void
kepaxos_connection_timeout(iomux_t *iomux, int fd, void *priv)
{
}

static void
kepaxos_connection_eof(iomux_t *iomux, int fd, void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    shardcache_release_connection_for_peer(connection->replica->shc, connection->peer, fd);
    async_read_context_destroy(connection->ctx);
    free(connection);
}

static void
free_item_to_recover(shardcache_item_to_recover_t *item)
{
    free(item->peer);
    free(item->key);
    free(item);
}

static int
kepaxos_recover(char *peer,
                void *key,
                size_t klen,
                uint64_t seq,
                uint64_t ballot,
                void *priv)
{
    if (!peer || !key || !klen)
        return -1;

    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    shardcache_item_to_recover_t *item = calloc(1, sizeof(shardcache_item_to_recover_t));

    item->key = malloc(klen);
    memcpy(item->key, key, klen);
    item->peer = strdup(peer);
    item->klen = klen;
    item->seq = seq;
    item->ballot = ballot;
    ht_set(replica->recovery, key, klen, item, sizeof(shardcache_item_to_recover_t));
    kepaxos_key_t *k = malloc(sizeof(kepaxos_key_t));
    k->key = malloc(klen);
    k->len = klen;
    memcpy(k, k->key, klen);
    pqueue_insert(replica->recovery_queue, ballot, k);
    return 0;
}

static int
kepaxos_commit(unsigned char type,
               void *key,
               size_t klen,
               void *data,
               size_t dlen,
               int leader,
               void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    int rc = -1;

    void *item = NULL;
    ht_delete(replica->recovery, key, klen, &item, NULL);
    if (item)
        free_item_to_recover((shardcache_item_to_recover_t *)item);

    kepaxos_data_t *kdata = (kepaxos_data_t *)data;

    switch(type) {
        case SHARDCACHE_REPLICA_OP_SET:
            rc = shardcache_set_internal(replica->shc,
                                         key,
                                         klen,
                                         &kdata->data,
                                         kdata->len,
                                         kdata->expire,
                                         0,
                                         leader ? 0 : 1,
                                         NULL, NULL);
            break;
        case SHARDCACHE_REPLICA_OP_DELETE:
            rc = shardcache_del_internal(replica->shc, key, klen, leader ? 0 : 1, NULL, NULL);
            break;
        case SHARDCACHE_REPLICA_OP_EVICT:
            rc = shardcache_evict(replica->shc, key, klen);
            break;
        case SHARDCACHE_REPLICA_OP_MIGRATION_BEGIN:
        {
            int num_shards = 0;
            shardcache_node_t **nodes = NULL;
            char *s = (char *)data;
            while (s && *s) {
                char *tok = strsep(&s, ",");
                if(tok) {
                    char *label = strsep(&tok, ":");
                    char *addr = tok;
                    size_t size = (num_shards + 1) * sizeof(shardcache_node_t *);
                    nodes = realloc(nodes, size);
                    shardcache_node_t *node = shardcache_node_create(label, &addr, 1);
                    nodes[num_shards++] = node;
                } 
            }
            rc = shardcache_set_migration_continuum(replica->shc, nodes, num_shards);
            if (rc != 0) {
                // TODO - Error messages
            }
            int i;
            for (i = 0; i < num_shards; i++)
                shardcache_node_destroy(nodes[i]);
            free(nodes);
            break;
        }
        case SHARDCACHE_REPLICA_OP_MIGRATION_ABORT:
            shardcache_migration_abort(replica->shc);
            break;
        case SHARDCACHE_REPLICA_OP_MIGRATION_END:
            shardcache_migration_end(replica->shc);
            break;
        default:
            break;
    }

    ATOMIC_INCREMENT(replica->counters.commits);
    if (rc != 0)
        ATOMIC_INCREMENT(replica->counters.commit_fails);
    
    return rc;
}

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
        int fd = shardcache_get_connection_for_peer(replica->shc, recipients[i]);
        if (fd < 0)
            continue;
        kepaxos_connection_t *connection = calloc(1, sizeof(kepaxos_connection_t));
        connection->replica = replica;
        connection->peer = recipients[i];
        connection->fd = fd;
        connection->ctx = async_read_context_create((char *)replica->shc->auth,
                                                    kepaxos_connection_append_input_data,
                                                    connection);
        shardcache_record_t record = {
            .v = cmd,
            .l = cmd_len
        };
        int rc = build_message((char *)replica->shc->auth, 0, SHC_HDR_REPLICA_COMMAND, &record, 1, &connection->output);
        if (rc == 0) {

            iomux_callbacks_t callbacks = {
                .mux_input = kepaxos_connection_input,
                .mux_output = NULL,
                .mux_timeout = kepaxos_connection_timeout,
                .mux_eof = kepaxos_connection_eof,
                .mux_connection = NULL,
                .priv = connection
            };

            iomux_add(replica->iomux, fd, &callbacks);
            char *data = NULL;
            unsigned int len = fbuf_detach(&connection->output, &data, NULL);
            iomux_write(replica->iomux, fd, (unsigned char *)data, len, 1);
        }
    }
    return 0;
}

static void
shardcache_replica_received_ack(shardcache_replica_t *replica, void *msg, size_t len)
{
    char *p = msg;
    char *peer;
    uint32_t peer_len;
    uint32_t num_items;
    if (len < sizeof(uint32_t)) {
        SHC_ERROR("Buffer underrun in shardcache_replica_received_ack()");
        return;
    }
    MSG_READ_UINT32(p, peer_len);
    if (len < (2 * sizeof(uint32_t)) + peer_len) {
        SHC_ERROR("Buffer underrun in shardcache_replica_received_ack()");
        return;
    }
    MSG_READ_POINTER(p, peer, peer_len);
    if (!peer) {
        SHC_ERROR("No sender in shardcache_replica_received_ack()");
        return;
    }
    MSG_READ_UINT32(p, num_items);

    size_t offset = p - (char *)msg;
    int i;
    for (i = 0; i < num_items; i++) {
        if (len < offset + (sizeof(uint64_t) * 2) + sizeof(uint32_t)) {
            SHC_ERROR("Buffer underrun in shardcache_replica_received_ack()");
            return;
        }
        offset += (sizeof(uint64_t) * 2) + sizeof(uint32_t);
        uint64_t ballot, seq;
        uint32_t klen;
        void *key = NULL;
        MSG_READ_UINT64(p, ballot);
        MSG_READ_UINT64(p, seq);
        MSG_READ_UINT32(p, klen);
        if (len < offset + klen) {
            SHC_ERROR("Buffer underrun in shardcache_replica_received_ack()");
            return;
        }
        MSG_READ_POINTER(p, key, klen);
        uint64_t last_seq = kepaxos_seq(replica->kepaxos, key, klen);
        if (last_seq < seq)
            kepaxos_recover(peer, key, klen, seq, ballot, replica);
    }
}

static void
shardcache_replica_ping(shardcache_replica_t *replica)
{
    char **peers = malloc(sizeof(char *) * replica->num_replicas);
    int num_peers = shardcache_node_get_all_addresses(replica->node, peers,  replica->num_replicas);

    int i;
    for (i = 0; i < num_peers; i++) {
        if (*replica->me != *peers[i] ||
            strcmp(replica->me, peers[i]) != 0)
        {
            int fd = shardcache_get_connection_for_peer(replica->shc, peers[i]);
            if (fd < 0)
                continue;
            kepaxos_connection_t *connection = calloc(1, sizeof(kepaxos_connection_t));
            connection->replica = replica;
            connection->peer = peers[i];
            connection->fd = fd;
            connection->ctx = async_read_context_create((char *)replica->shc->auth,
                                                        kepaxos_connection_append_input_data,
                                                        connection);

            uint64_t ballot = kepaxos_ballot(replica->kepaxos);
            size_t peer_len = strlen(replica->me) + 1;
            uint32_t msg_len = sizeof(uint64_t) + sizeof(uint32_t) + peer_len;

            char *msg = malloc(msg_len);
            size_t offset = 0;
            MSG_WRITE_UINT32(msg, offset, peer_len);
            MSG_WRITE_POINTER(msg, offset, replica->me, peer_len);
            MSG_WRITE_UINT64(msg, offset, ballot);

            shardcache_record_t record = {
                .v = msg,
                .l = msg_len
            };
            int rc = build_message((char *)replica->shc->auth, 0, SHC_HDR_REPLICA_PING, &record, 1, &connection->output);
            if (rc == 0) {

                iomux_callbacks_t callbacks = {
                    .mux_input = kepaxos_connection_input,
                    .mux_output = NULL,
                    .mux_timeout = kepaxos_connection_timeout,
                    .mux_eof = kepaxos_connection_eof,
                    .mux_connection = NULL,
                    .priv = connection
                };
                iomux_add(replica->iomux, fd, &callbacks);
                char *data = NULL;
                unsigned int len = fbuf_detach(&connection->output, &data, NULL);
                iomux_write(replica->iomux, fd, (unsigned char *)data, len, 1);
            } else {
                shardcache_release_connection_for_peer(replica->shc, peers[i], fd);
            }
            free(msg);
        }
    }
    free(peers);
}

static void
kepaxos_key_destroy(kepaxos_key_t *k)
{
    free(k->key);
    free(k);
}

/* TODO - parallelize recovery */
static void *
shardcache_replica_recover(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;

    while (!ATOMIC_READ(replica->quit)) {
        struct timespec timeout = { 0, 500 * 1e6 };
        struct timespec remainder = { 0, 0 };

        kepaxos_key_t *k = NULL;
        uint64_t prio = 0;

        ATOMIC_SET(replica->counters.recovering, pqueue_count(replica->recovery_queue));
        ATOMIC_SET(replica->counters.ballot, kepaxos_ballot(replica->kepaxos));

        int rc = pqueue_pull_highest(replica->recovery_queue, (void **)&k, &prio);
        if (rc != 0 || !k) {
            shardcache_replica_ping(replica);
            do {
                rc = nanosleep(&timeout, &remainder);
                if (ATOMIC_READ(replica->quit))
                    break;
                memcpy(&timeout, &remainder, sizeof(struct timespec));
                memset(&remainder, 0, sizeof(struct timespec));
            } while (rc != 0);
            continue;
        }
        shardcache_item_to_recover_t *item = ht_get(replica->recovery, k->key, k->len, NULL);
        kepaxos_key_destroy(k);

        if (!item)
            continue;

        int fd = shardcache_get_connection_for_peer(replica->shc, item->peer);
        if (fd < 0) {
            kepaxos_key_t *k = malloc(sizeof(kepaxos_key_t));
            k->key = malloc(item->klen);
            k->len = item->klen;
            memcpy(k->key, item->key, item->klen);
            pqueue_insert(replica->recovery_queue, prio, k);
            if (pqueue_count(replica->recovery_queue) == 1) {
                do {
                    rc = nanosleep(&timeout, &remainder);
                    if (ATOMIC_READ(replica->quit))
                        break;
                    memcpy(&timeout, &remainder, sizeof(struct timespec));
                    memset(&remainder, 0, sizeof(struct timespec));
                } while (rc != 0);
            }
            continue;
        }

        fbuf_t data = FBUF_STATIC_INITIALIZER;
        // TODO - use fetch_from_peer_async() so that the download
        //        can be stopped earlier if the recovery is aborted
        rc = fetch_from_peer(item->peer, (char *)replica->shc->auth, 0, item->key, item->klen, &data, fd);
        if (rc == 0) {
            void *check = NULL;
            rc = ht_delete(replica->recovery, item->key, item->klen, &check, NULL);
            if (rc != 0) {
                SHC_ERROR("Can't delete item from the recovery table");
            }

            if (check == item || (check && ((shardcache_item_to_recover_t *)check)->seq == item->seq))
            {
                rc = kepaxos_recovered(replica->kepaxos,
                                       item->key,
                                       item->klen,
                                       item->ballot,
                                       item->seq);
                if (rc == 0 && fbuf_used(&data)) {
                    rc = shardcache_set_internal(replica->shc,
                                                 item->key,
                                                 item->klen,
                                                 fbuf_data(&data),
                                                 fbuf_used(&data),
                                                 0, 0, 0, NULL, NULL);
                    if (rc != 0) {
                        SHC_ERROR("Can't set value for the recovered item");
                    }
                }
                free_item_to_recover((shardcache_item_to_recover_t *)check);
                continue;
            } else if (check) {
                // put it back
                int rc = ht_set_if_not_exists(replica->recovery,
                                              ((shardcache_item_to_recover_t *)check)->key,
                                              ((shardcache_item_to_recover_t *)check)->klen,
                                              (shardcache_item_to_recover_t *)check,
                                              sizeof(shardcache_item_to_recover_t));
                if (rc == 1) {
                    // a new entry has been added to the recovery table in the meanwhile
                    // we can drop this one
                    free_item_to_recover((shardcache_item_to_recover_t *)check);
                }
            }
        } else {
            // retry
            kepaxos_key_t *k = malloc(sizeof(kepaxos_key_t));
            k->key = malloc(item->klen);
            k->len = item->klen;
            memcpy(k, item->key, item->klen);
            pqueue_insert(replica->recovery_queue, prio, k);
        }
    }
    return NULL;
}

void *shardcache_replica_async_io(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    while (!ATOMIC_READ(replica->quit)) {
        struct timeval timeout = { 0, 500 };
        iomux_run(replica->iomux, &timeout);
    }
    return NULL;
}

static void
shardcache_replica_register_counters(shardcache_replica_t *replica)
{
    shardcache_counter_add(replica->shc->counters,
                           "replica_recovering",
                           &replica->counters.recovering);

    shardcache_counter_add(replica->shc->counters,
                           "replica_ballot",
                           &replica->counters.ballot);

    shardcache_counter_add(replica->shc->counters,
                           "replica_commits",
                           &replica->counters.commits);

    shardcache_counter_add(replica->shc->counters,
                           "replica_commit_fails",
                           &replica->counters.commit_fails);

    shardcache_counter_add(replica->shc->counters,
                           "replica_dispached",
                           &replica->counters.dispached);

    shardcache_counter_add(replica->shc->counters,
                           "replica_responses",
                           &replica->counters.responses);

    shardcache_counter_add(replica->shc->counters,
                           "replica_commands",
                           &replica->counters.commands);

    shardcache_counter_add(replica->shc->counters,
                           "replica_acks",
                           &replica->counters.acks);

}

shardcache_replica_t *
shardcache_replica_create(shardcache_t *shc,
                          shardcache_node_t *node,
                          int my_index,
                          char *wrkdir)
{
    shardcache_replica_t *replica = calloc(1, sizeof(shardcache_replica_t));

    replica->node = shardcache_node_copy(node);

    replica->me = shardcache_node_get_address_at_index(node, my_index);

    replica->num_replicas = shardcache_node_num_addresses(node);

    replica->shc = shc;

    // TODO - check wrkdir exists and is writeable
    if (!wrkdir)
        wrkdir = SHARDCACHE_REPLICA_WRKDIR_DEFAULT;

    char dbfile[2048];
    snprintf(dbfile, sizeof(dbfile), "%s/%s", wrkdir, KEPAXOS_LOG_FILENAME);

    struct stat s;
    if (stat(wrkdir, &s) != 0) {
        if (mkdir(wrkdir, S_IRWXU) != 0) {
            SHC_ERROR("Can't create the workdir %s to store the replica log: %s", wrkdir, strerror(errno));
            shardcache_replica_destroy(replica);
            return NULL;
        }
    }
    char **peers = malloc(sizeof(char *) * replica->num_replicas);
    int num_peers = shardcache_node_get_all_addresses(replica->node, peers,  replica->num_replicas);

    kepaxos_callbacks_t kepaxos_callbacks = {
        .send = kepaxos_send,
        .commit = kepaxos_commit,
        .recover = kepaxos_recover,
        .priv = replica
    };
    replica->kepaxos = kepaxos_context_create(dbfile, peers, num_peers, my_index, 10, &kepaxos_callbacks);
    if (!replica->kepaxos) {
        shardcache_replica_destroy(replica);
        free(peers);
        return NULL;
    }

    replica->recovery = ht_create(128, 1024, NULL);

    replica->recovery_queue = pqueue_create(PQUEUE_MODE_LOWEST, 1<<20,
                                            (pqueue_free_value_callback)kepaxos_key_destroy);

    replica->iomux = iomux_create(0, 1);

    if (pthread_create(&replica->recover_th, NULL, shardcache_replica_recover, replica) != 0)
    {
        shardcache_replica_destroy(replica); 
        free(peers);
        return NULL;
    }

    if (pthread_create(&replica->async_io_th, NULL, shardcache_replica_async_io, replica) != 0) {
        shardcache_replica_destroy(replica); 
        free(peers);
        return NULL;
    }

    shardcache_replica_register_counters(replica);

    free(peers);
    return replica;
}

void
shardcache_replica_destroy(shardcache_replica_t *replica)
{
    if (replica->recover_th) {
        ATOMIC_INCREMENT(replica->quit);
        pthread_join(replica->recover_th, NULL);
        pthread_join(replica->async_io_th, NULL);
    }

    shardcache_node_destroy(replica->node);

    if (replica->recovery)
        ht_destroy(replica->recovery);
    if (replica->recovery_queue)
        pqueue_destroy(replica->recovery_queue);

    free(replica);
}

static int
shardcache_replica_received_ping(shardcache_replica_t *replica,
                                 void *cmd,
                                 size_t cmdlen,
                                 void **response,
                                 size_t *response_len)
{
    if (cmdlen < sizeof(uint64_t) + 1)
        return -1;

    char *p = cmd;

    uint32_t peer_len;
    uint64_t ballot;
    char *peer = NULL;
    MSG_READ_UINT32(p, peer_len);
    MSG_READ_POINTER(p, peer, peer_len);
    if (!peer) {
        SHC_ERROR("No sender in shardcache_replica_received_ping()");
        return -1;
    }
    MSG_READ_UINT64(p, ballot);

    kepaxos_diff_item_t *items = NULL;
    int num_items = 0;
    kepaxos_get_diff(replica->kepaxos, ballot, &items, &num_items);

    size_t myname_len = strlen(replica->me) + 1;
    size_t outlen = (sizeof(uint32_t) * 2) + myname_len;
    char *out = malloc(outlen);
    size_t offset = 0;
    MSG_WRITE_UINT32(out, offset, myname_len);
    MSG_WRITE_POINTER(out, offset, replica->me, myname_len);
    MSG_WRITE_UINT32(out, offset, num_items);

    int i;
    for (i = 0; i < num_items; i++) {
        kepaxos_diff_item_t *item = &items[i];
        int offset = outlen;
        outlen += sizeof(uint64_t) * 2 + sizeof(uint32_t) + item->klen;
        out = realloc(out, outlen);
        MSG_WRITE_UINT64(out, offset, item->ballot);
        MSG_WRITE_UINT64(out, offset, item->seq);
        MSG_WRITE_UINT32(out, offset, item->klen);
        MSG_WRITE_POINTER(out, offset, item->key, item->klen);
    }

    kepaxos_diff_release(items, num_items);

    *response = out;
    *response_len = outlen;

    return 0;
}

shardcache_hdr_t
shardcache_replica_received_command(shardcache_replica_t *replica,
                                    shardcache_hdr_t hdr,
                                    void *cmd,
                                    size_t cmdlen,
                                    void **response,
                                    size_t *response_len)
{
    int rc;
    shardcache_hdr_t ret = SHC_HDR_RESPONSE;
    ATOMIC_INCREMENT(replica->counters.commands);
    switch (hdr) {
        case SHC_HDR_REPLICA_COMMAND:
            rc = kepaxos_received_command(replica->kepaxos,
                                            cmd,
                                            cmdlen,
                                            response,
                                            response_len);
            if (rc == 0 && response_len)
                ret = SHC_HDR_REPLICA_RESPONSE;
            break;
        case SHC_HDR_REPLICA_PING:
            rc = shardcache_replica_received_ping(replica,
                                                  cmd,
                                                  cmdlen,
                                                  response,
                                                  response_len);
            if (rc == 0 && response_len)
                ret = SHC_HDR_REPLICA_ACK;
            break;
        default:
            break;
    }
    return ret;
}

int
shardcache_replica_dispatch(shardcache_replica_t *replica,
                            shardcache_replica_operation_t op,
                            void *key,
                            size_t klen,
                            void *data,
                            size_t dlen,
                            uint32_t expire)
{
    //       (and later for the 0-key in using kepaxos)
    //       We still want migration commands to run into
    //       kepaxos to ensure accepting a migration request
    //       only if at least N/2 + 1 replicas are aware of it
    if (op == SHARDCACHE_REPLICA_OP_MIGRATION_BEGIN ||
        op == SHARDCACHE_REPLICA_OP_MIGRATION_ABORT ||
        op == SHARDCACHE_REPLICA_OP_MIGRATION_END)
    {
        key = "";
        klen = 1;
    } else if (klen == 0 || !key) {
        return -1;
    }

    void *item = NULL;
    // XXX - big hack : special meaning for the NULL key
    // stop any recovery process for this key if in progress
    ht_delete(replica->recovery, key, klen, &item, NULL);
    if (item)
        free_item_to_recover((shardcache_item_to_recover_t *)item);

    size_t kdlen = sizeof(kepaxos_data_t) + dlen;
    kepaxos_data_t *kdata = malloc(kdlen);
    kdata->len = dlen;
    kdata->expire = expire;
    memcpy(&kdata->data, data, dlen);

    ATOMIC_INCREMENT(replica->counters.dispached);

    int rc = kepaxos_run_command(replica->kepaxos,
                                 (unsigned char)op,
                                 key,
                                 klen,
                                 kdata,
                                 kdlen);

    free(kdata);
    return rc;
}

