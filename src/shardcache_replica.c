#include "shardcache_replica.h"

#include <hashtable.h>
#include <linklist.h>
#include <fbuf.h>
#include <pqueue.h>
#include <iomux.h>

#include "atomic.h"
#include "kepaxos.h"
#include "shardcache_internal.h"
#include "counters.h"

#include <unistd.h>
#include <sys/time.h>
#include <arpa/inet.h>

#define SHARDCACHE_REPLICA_WRKDIR_DEFAULT "/tmp/shcrpl"
#define KEPAXOS_LOG_FILENAME "kepaxos_log.db"

#define MSG_WRITE_UINT64(__p, __o, __n) \
{ \
    uint32_t __low = htonl((__n) & 0x00000000FFFFFFFF); \
    uint32_t __high = htonl((__n) >> 32); \
    memcpy((__p) + (__o), &__high, sizeof(uint32_t)); \
    (__o) += sizeof(uint32_t); \
    memcpy((__p) + (__o), &__low, sizeof(uint32_t)); \
    (__o) += sizeof(uint32_t); \
}

#define MSG_WRITE_UINT32(__p, __o, __n) \
{ \
        uint32_t __nbo = htonl((__n)); \
        memcpy((__p) + (__o), &__nbo, sizeof(uint32_t)); \
        (__o) += sizeof(uint32_t); \
}

#define MSG_READ_UINT64(__p, __n) { \
    uint32_t __high = ntohl(*((uint32_t *)(__p))); \
    (__p) += sizeof(uint32_t); \
    uint32_t __low = ntohl(*((uint32_t *)(__p))); \
    (__p) += sizeof(uint32_t); \
    (__n) = ((uint64_t)__high) << 32 | __low; \
}

#define MSG_READ_UINT32(__p, __n) { \
    (__n) = ntohl(*((uint32_t *)(__p))); \
    (__p) += sizeof(uint32_t); \
}

struct __shardcache_replica_s {
    shardcache_t *shc;       //!< a valid shardcache instance
    shardcache_node_t *node; //!< the shardcache node (union of all replicas)
    char *me;                //!< myself (among the node replicas)
    int num_replicas;        //!< the number of replicase
    kepaxos_t *kepaxos;      //!< a valid kepaxos context
    hashtable_t *recovery;   //!< teomporary store for keys being recovered
    pqueue_t *recovery_queue;
    struct {
        uint32_t recovering;
        uint32_t ballot;
        uint32_t commits;
        uint32_t commit_fails;
        uint32_t dispached;
        uint32_t responses;
        uint32_t commands;
        uint32_t acks;
    } counters;
    int quit;
    pthread_t recover_th;
    pthread_t async_io_th;
    iomux_t *iomux;
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

static void
kepaxos_connection_input(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    shardcache_replica_t *replica = connection->replica;

    fbuf_t out = FBUF_STATIC_INITIALIZER;
    int rc = async_read_context_input_data(data, len, connection->ctx);
    if (rc != 0) {
    }

    int read_state = async_read_context_state(connection->ctx);
    if (read_state == SHC_STATE_READING_DONE ||
        read_state == SHC_STATE_READING_ERR  ||
        read_state == SHC_STATE_AUTH_ERR)
    {
        shardcache_hdr_t hdr = async_read_context_hdr(connection->ctx);
        if (hdr == SHC_HDR_REPLICA_RESPONSE || hdr == SHC_HDR_REPLICA_ACK)
        {
            if (hdr == SHC_HDR_REPLICA_RESPONSE) {
                ATOMIC_INCREMENT(replica->counters.responses);
                kepaxos_received_response(replica->kepaxos, fbuf_data(&out), fbuf_used(&out));
            } else {
                ATOMIC_INCREMENT(replica->counters.acks);
                shardcache_replica_received_ack(replica, fbuf_data(&out), fbuf_used(&out));
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

}

static void 
kepaxos_connection_output(iomux_t *iomux, int fd, void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    if (fbuf_used(&connection->output)) {
        int wb = iomux_write(iomux, fd, fbuf_data(&connection->output), fbuf_used(&connection->output));
        fbuf_remove(&connection->output, wb);
    } else {
        iomux_callbacks_t *cbs = iomux_callbacks(iomux, fd);
        cbs->mux_output = NULL;
    }
}

static void
kepaxos_connection_timeout(iomux_t *iomux, int fd, void *priv)
{
}

static void
kepaxos_connection_eof(iomux_t *iomux, int fd, void *priv)
{
    kepaxos_connection_t *connection = (kepaxos_connection_t *)priv;
    async_read_context_destroy(connection->ctx);
    free(connection);
    close(fd);
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
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    shardcache_item_to_recover_t *item = calloc(1, sizeof(shardcache_item_to_recover_t));

    item->key = malloc(klen);
    memcpy(item->key, key, klen);
    item->peer = strdup(peer);
    item->klen = klen;
    item->seq = seq;
    item->ballot = ballot;
    ht_set(replica->recovery, key, klen, item, sizeof(shardcache_item_to_recover_t));
    void *k = malloc(klen);
    memcpy(k, key, klen);
    pqueue_insert(replica->recovery_queue, ballot, k, klen);
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

    shardcache_item_to_recover_t *item = NULL;
    ht_delete(replica->recovery, key, klen, (void **)&item, NULL);
    if (item)
        free_item_to_recover(item);

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
                                         leader ? 0 : 1);
            break;
        case SHARDCACHE_REPLICA_OP_DELETE:
            rc = shardcache_del_internal(replica->shc, key, klen, leader ? 0 : 1);
            break;
        case SHARDCACHE_REPLICA_OP_EVICT:
            rc = shardcache_evict(replica->shc, key, klen);
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
                .mux_output = kepaxos_connection_output,
                .mux_timeout = kepaxos_connection_timeout,
                .mux_eof = kepaxos_connection_eof,
                .mux_connection = NULL,
                .priv = connection
            };

            iomux_add(replica->iomux, fd, &callbacks);
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
    peer = p;
    p += peer_len;
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
        if (klen)
            key = p;
        kepaxos_recover(peer, key, klen, seq, ballot, replica);
        offset += klen;
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
            memcpy(msg + offset, replica->me, peer_len);
            offset += peer_len;
            MSG_WRITE_UINT64(msg, offset, ballot);

            shardcache_record_t record = {
                .v = msg,
                .l = msg_len
            };
            int rc = build_message((char *)replica->shc->auth, 0, SHC_HDR_REPLICA_PING, &record, 1, &connection->output);
            if (rc == 0) {

                iomux_callbacks_t callbacks = {
                    .mux_input = kepaxos_connection_input,
                    .mux_output = kepaxos_connection_output,
                    .mux_timeout = kepaxos_connection_timeout,
                    .mux_eof = kepaxos_connection_eof,
                    .mux_connection = NULL,
                    .priv = connection
                };
                iomux_add(replica->iomux, fd, &callbacks);
            }
            free(msg);
        }
    }

    free(peers);
}

/* TODO - parallelize recovery */
static void *
shardcache_replica_recover(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;

    while (!ATOMIC_READ(replica->quit)) {
        struct timespec timeout = { 0, 500 * 1e6 };
        struct timespec remainder = { 0, 0 };

        void *key = NULL;
        size_t klen = 0;
        uint64_t prio = 0;

        ATOMIC_SET(replica->counters.recovering, pqueue_count(replica->recovery_queue));
        ATOMIC_SET(replica->counters.ballot, kepaxos_ballot(replica->kepaxos));

        int rc = pqueue_pull_highest(replica->recovery_queue, (void **)&key, &klen, &prio);
        if (rc != 0) {
            SHC_ERROR("replica_recover: Can't get the top item from the priority queue");
        }

        if (!key) {
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
        shardcache_item_to_recover_t *item = ht_get(replica->recovery, key, klen, NULL);
        free(key);

        if (!item)
            continue;

        int fd = shardcache_get_connection_for_peer(replica->shc, item->peer);
        if (fd < 0) {
            void *k = malloc(item->klen);
            memcpy(k, item->key, klen);
            pqueue_insert(replica->recovery_queue, prio, k, item->klen);
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
            shardcache_item_to_recover_t *check = NULL;
            rc = ht_delete(replica->recovery, item->key, item->klen, (void **)&check, NULL);
            if (rc != 0) {
                SHC_ERROR("Can't delete item from the recovery table");
            }

            if (check == item || (check && check->seq == item->seq))
            {
                rc = kepaxos_recovered(replica->kepaxos,
                                       item->key,
                                       item->klen,
                                       item->ballot,
                                       item->seq);
                if (rc == 0) {
                    rc = shardcache_set_internal(replica->shc,
                                                 item->key,
                                                 item->klen,
                                                 fbuf_data(&data),
                                                 fbuf_used(&data),
                                                 0, 0, 0);
                    if (rc != 0) {
                        SHC_ERROR("Can't set value for the recovered item");
                    }
                }
                free_item_to_recover(check);
                continue;
            } else if (check) {
                // put it back
                int rc = ht_set_if_not_exists(replica->recovery,
                                              check->key,
                                              check->klen,
                                              check,
                                              sizeof(shardcache_item_to_recover_t));
                if (rc == 1) {
                    // a new entry has been added to the recovery table in the meanwhile
                    // we can drop this one
                    free_item_to_recover(check);
                }
            }
        } else {
            // retry
            void *k = malloc(item->klen);
            memcpy(k, item->key, item->klen);
            pqueue_insert(replica->recovery_queue, prio, k, item->klen);
        }
    }
    return NULL;
}

void *shardcache_replica_async_io(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    while (!replica->quit) {
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
    char dbfile[2048];
    snprintf(dbfile, sizeof(dbfile), "%s/%s",
             wrkdir ? wrkdir : SHARDCACHE_REPLICA_WRKDIR_DEFAULT, KEPAXOS_LOG_FILENAME);

    char **peers = malloc(sizeof(char *) * replica->num_replicas);
    int num_peers = shardcache_node_get_all_addresses(replica->node, peers,  replica->num_replicas);

    kepaxos_callbacks_t kepaxos_callbacks = {
        .send = kepaxos_send,
        .commit = kepaxos_commit,
        .recover = kepaxos_recover
    };
    replica->kepaxos = kepaxos_context_create(dbfile, peers, num_peers, my_index, 10, &kepaxos_callbacks);

    replica->recovery = ht_create(128, 1024, NULL);

    replica->recovery_queue = pqueue_create(PQUEUE_MODE_LOWEST, 1<<20, free);

    if (pthread_create(&replica->recover_th, NULL, shardcache_replica_recover, replica) != 0)
    {
        shardcache_replica_destroy(replica); 
        free(peers);
        return NULL;
    }

    replica->iomux = iomux_create();
    iomux_set_threadsafe(replica->iomux, 1);

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
    }
    shardcache_node_destroy(replica->node);
    ht_destroy(replica->recovery);
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
    MSG_READ_UINT32(p, peer_len);
    //char *peer = p;
    p += peer_len;
    MSG_READ_UINT64(p, ballot);

    kepaxos_diff_item_t *items = NULL;
    int num_items = 0;
    int rc = kepaxos_get_diff(replica->kepaxos, ballot, &items, &num_items);
    if (rc != 0)
        return -1; 

    size_t myname_len = strlen(replica->me) + 1;
    size_t outlen = (sizeof(uint32_t) * 2) + myname_len;
    char *out = malloc(outlen);
    size_t offset = 0;
    MSG_WRITE_UINT32(out, offset, myname_len);
    memcpy(out + offset, replica->me, myname_len);
    offset += myname_len;
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
        memcpy(out + offset, item->key, item->klen);
    }

    kepaxos_diff_release(items, num_items);

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
    // stop any recovery process if in progress
    shardcache_item_to_recover_t *item = NULL;
    ht_delete(replica->recovery, key, klen, (void **)&item, NULL);
    if (item)
        free_item_to_recover(item);

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

