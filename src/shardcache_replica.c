#include "shardcache_replica.h"

#include <hashtable.h>
#include <linklist.h>
#include <fbuf.h>
#include <pqueue.h>
#include <iomux.h>

#include "atomic.h"
#include "kepaxos.h"
#include "shardcache_internal.h"

#include <unistd.h>
#include <sys/time.h>

#define SHARDCACHE_REPLICA_WRKDIR_DEFAULT "/tmp/shcrpl"
#define KEPAXOS_LOG_FILENAME "kepaxos_log.db"

struct __shardcache_replica_s {
    shardcache_t *shc;       //!< a valid shardcache instance
    shardcache_node_t *node; //!< the shardcache node (union of all replicas)
    char *me;                //!< myself (among the node replicas)
    int num_replicas;        //!< the number of replicase
    kepaxos_t *kepaxos;      //!< a valid kepaxos context
    hashtable_t *recovery;   //!< teomporary store for keys being recovered
    pqueue_t *recovery_queue;
    int quit;
    pthread_t recover_th;
    pthread_t async_io_th;
    iomux_t *iomux;
};

typedef struct {
    char *peer;
    void *key;
    size_t klen;
    uint32_t seq;
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
free_item_to_recover(shardcache_item_to_recover_t *item)
{
    free(item->peer);
    free(item->key);
    free(item);
}

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
        if (hdr == SHC_HDR_REPLICA_RESPONSE) {
            kepaxos_received_command(replica->kepaxos, connection->peer, fbuf_data(&out), fbuf_used(&out));
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

static void *
shardcache_replica_recover(void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;

    while (!ATOMIC_READ(replica->quit)) {
        struct timespec timeout = { 0, 500 * 1e6 };
        struct timespec remainder = { 0, 0 };

        shardcache_item_to_recover_t *item = NULL;
        int32_t prio = 0;

        int rc = pqueue_pull_highest(replica->recovery_queue, (void **)&item, NULL, &prio);
        if (!item) {
            do {
                rc = nanosleep(&timeout, &remainder);
                if (ATOMIC_READ(replica->quit))
                    break;
                memcpy(&timeout, &remainder, sizeof(struct timespec));
                memset(&remainder, 0, sizeof(struct timespec));
            } while (rc != 0);
            continue;
        }

        int fd = shardcache_get_connection_for_peer(replica->shc, item->peer);
        if (fd < 0) {
            pqueue_insert(replica->recovery_queue, prio, item, sizeof(shardcache_item_to_recover_t));
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
        rc = fetch_from_peer(item->peer, (char *)replica->shc->auth, 0, item->key, item->klen, &data, fd);
        if (rc == 0) {
            shardcache_item_to_recover_t *check = NULL;
            rc = ht_delete(replica->recovery, item->key, item->klen, (void **)&check, NULL);
            if (check == item || (check && check->seq == item->seq)) {
                rc = shardcache_set_internal(replica->shc, item->key, item->klen, fbuf_data(&data), fbuf_used(&data), 0, 0, 0);
                if (rc != 0) {
                    // TODO - Error messages
                }
                free_item_to_recover(item);

                if (check != item)
                    free_item_to_recover(check);

            } else if (check) {
                // put it back
                ht_set(replica->recovery, check->key, check->klen, check, sizeof(shardcache_item_to_recover_t));

            }
        }
    }
    return NULL;
}

static int
kepaxos_recover(char *peer, void *key, size_t klen, uint32_t seq, int32_t prio, void *priv)
{
    shardcache_replica_t *replica = (shardcache_replica_t *)priv;
    shardcache_item_to_recover_t *item = calloc(1, sizeof(shardcache_item_to_recover_t));

    item->key = malloc(klen);
    memcpy(item->key, key, klen);
    item->peer = strdup(peer);
    item->klen = klen;
    item->seq = seq;
    ht_set(replica->recovery, key, klen, item, sizeof(shardcache_item_to_recover_t));
    pqueue_insert(replica->recovery_queue, prio, item, sizeof(shardcache_item_to_recover_t));
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

    kepaxos_data_t *kdata = (kepaxos_data_t *)data;

    switch(type) {
        case SHARDCACHE_REPLICA_OP_SET:
            rc = shardcache_set_internal(replica->shc, key, klen, &kdata->data, kdata->len, kdata->expire, 0, leader ? 0 : 1);
        case SHARDCACHE_REPLICA_OP_DELETE:
            rc = shardcache_del_internal(replica->shc, key, klen, leader ? 0 : 1);
        case SHARDCACHE_REPLICA_OP_EVICT:
            rc = shardcache_evict(replica->shc, key, klen);
        default:
            break;
    }

    return rc;
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
    replica->kepaxos = kepaxos_context_create(dbfile, peers, num_peers, &kepaxos_callbacks);

    replica->recovery = ht_create(128, 1024, NULL);

    replica->recovery_queue = pqueue_create(PQUEUE_MODE_LOWEST, 1<<20, (pqueue_free_value_callback)free_item_to_recover);

    if (pthread_create(&replica->recover_th, NULL, shardcache_replica_recover, replica) != 0) {
        shardcache_replica_destroy(replica); 
        return NULL;
    }

    replica->iomux = iomux_create();
    iomux_set_threadsafe(replica->iomux, 1);
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
    free(replica->me);
    free(replica);
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
    ht_delete(replica->recovery, key, klen, NULL, NULL);

    size_t kdlen = sizeof(kepaxos_data_t) + dlen;
    kepaxos_data_t *kdata = malloc(kdlen);
    kdata->len = dlen;
    kdata->expire = expire;
    memcpy(&kdata->data, data, dlen);

    int rc = kepaxos_run_command(replica->kepaxos,
                                 replica->me,
                                 (unsigned char)op,
                                 key,
                                 klen,
                                 kdata,
                                 kdlen);

    free(kdata);
    return rc;
}

