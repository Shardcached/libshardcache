#include "sqlite3.h"
#include "kepaxos.h"
#include "atomic.h"
#include <pqueue.h>
#include <hashtable.h>
#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

#include <unistd.h>
#include <stdio.h>
#include <arpa/inet.h>

#include "shardcache.h" // for SHC_DEBUG*()
#include "shardcache_internal.h" // for KEY2STR()

#define MAX(a, b) ( (a) > (b) ? (a) : (b) )
#define MIN(a, b) ( (a) < (b) ? (a) : (b) )

#define KEPAXOX_CMD_TTL 30 // default to 30 seconds

typedef enum {
    KEPAXOS_CMD_STATUS_NONE=0,
    KEPAXOS_CMD_STATUS_PRE_ACCEPTED,
    KEPAXOS_CMD_STATUS_ACCEPTED,
    KEPAXOS_CMD_STATUS_COMMITTED,
} kepaxos_cmd_status_t;

typedef enum {
    KEPAXOS_MSG_TYPE_PRE_ACCEPT,
    KEPAXOS_MSG_TYPE_PRE_ACCEPT_RESPONSE,
    KEPAXOS_MSG_TYPE_ACCEPT,
    KEPAXOS_MSG_TYPE_ACCEPT_RESPONSE,
    KEPAXOS_MSG_TYPE_COMMIT,
    KEPAXOS_MSG_TYPE_RECOVERY,
    KEPAXOS_MSG_TYPE_RECOVERY_RESPONSE,
} kepaxos_msg_type_t;

typedef struct {
    char *peer;
    uint32_t ballot;
    void *key;
    size_t klen;
    uint32_t seq;
} kepaxos_vote_t;

struct __kepaxos_cmd_s {
    unsigned char type;
    kepaxos_msg_type_t msg;
    kepaxos_cmd_status_t status;
    uint32_t seq;
    void *key;
    size_t klen;
    void *data;
    size_t dlen;
    kepaxos_vote_t *votes;
    uint16_t num_votes;
    uint32_t max_seq;
    char *max_voter;
    uint32_t ballot;
    time_t timestamp;
    pthread_mutex_t condition_lock;
    pthread_cond_t condition;
};

struct __kepaxos_s {
    sqlite3 *log;
    hashtable_t *commands; // key => cmd 
    char **peers;
    int num_peers;
    unsigned char my_index;
    kepaxos_callbacks_t callbacks;
    pthread_mutex_t lock;
    uint32_t ballot;
    sqlite3_stmt *select_seq_stmt;
    sqlite3_stmt *select_ballot_stmt;
    sqlite3_stmt *insert_stmt;
    pqueue_t *recovery_queue;
    pthread_t expirer;
    int quit;
};

static void
kepaxos_compute_key_hashes(void *key, size_t klen, uint64_t *hash1, uint64_t *hash2)
{
    unsigned char auth1[16] = "0123456789ABCDEF";
    unsigned char auth2[16] = "ABCDEF0987654321";

    *hash1 = sip_hash24(auth1, key, klen);
    *hash2 = sip_hash24(auth2, key, klen);
}

static uint32_t
kepaxos_max_ballot(kepaxos_t *ke)
{
    uint32_t ballot = 0;

    int rc = sqlite3_reset(ke->select_ballot_stmt);

    rc = sqlite3_step(ke->select_ballot_stmt);
    if (rc == SQLITE_ROW)
        ballot = sqlite3_column_int(ke->select_ballot_stmt, 0);

    return ballot;
}

static uint32_t
last_seq_for_key(kepaxos_t *ke, void *key, size_t klen)
{
    uint64_t keyhash1, keyhash2;
    uint32_t seq = 0;

    int rc = sqlite3_reset(ke->select_seq_stmt);

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    rc = sqlite3_bind_int64(ke->select_seq_stmt, 1, keyhash1);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    rc = sqlite3_bind_int64(ke->select_seq_stmt, 2, keyhash2);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    //int cnt = 0;
    rc = sqlite3_step(ke->select_seq_stmt);
    if (rc == SQLITE_ROW)
        seq = sqlite3_column_int(ke->select_seq_stmt, 0);

    return seq;
}

static void
set_last_seq_for_key(kepaxos_t *ke, void *key, size_t klen, uint32_t ballot, uint32_t seq)
{
    uint64_t keyhash1, keyhash2;
    //uint64_t last_seq = 0;

    int rc = sqlite3_reset(ke->insert_stmt);

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    rc = sqlite3_bind_int(ke->insert_stmt, 1, ballot);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 2, keyhash1);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 3, keyhash2);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    rc = sqlite3_bind_int(ke->insert_stmt, 4, seq);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    rc = sqlite3_step(ke->insert_stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Insert Failed! %d\n", rc);
        return;
    }
}


static void
kepaxos_command_destroy(kepaxos_cmd_t *c)
{
    MUTEX_LOCK(&c->condition_lock);
    pthread_cond_signal(&c->condition);
    MUTEX_UNLOCK(&c->condition_lock);
    free(c->key);
    if (c->data)
        free(c->data);
    free(c);
}

static int
kepaxos_expire_command(hashtable_t *table,
                                  void *key,
                                  size_t klen,
                                  void *value,
                                  size_t vlen,
                                  void *user)
{
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)value;
    if (time(NULL) > cmd->timestamp + KEPAXOX_CMD_TTL) {
        return -1; // if expired we want to remove this item from the table
    }
    return 1;
}

static void *
kepaxos_expire_commands(void *priv)
{
    kepaxos_t *ke = (kepaxos_t *)priv;
    while (!ATOMIC_READ(ke->quit)) {
        ht_foreach_pair(ke->commands, kepaxos_expire_command, ke);
        sleep(1);
    }
    return NULL;
}

kepaxos_t *
kepaxos_context_create(char *dbfile, char **peers, int num_peers, kepaxos_callbacks_t *callbacks)
{
    kepaxos_t *ke = calloc(1, sizeof(kepaxos_t));

    int rc = sqlite3_open(dbfile, &ke->log);
    if (rc != SQLITE_OK) {
        // TODO - Error messages
        free(ke);
        return NULL;
    }

    const char *create_table_sql = "CREATE TABLE IF NOT EXISTS ReplicaLog (ballot int, keyhash1 int, keyhash2 int, seq int, PRIMARY KEY(keyhash1, keyhash2))";
    rc = sqlite3_exec(ke->log, create_table_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        // TODO - Errors
        sqlite3_close(ke->log);
        free(ke);
    }

    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT MAX(seq) FROM ReplicaLog WHERE keyhash1=? AND keyhash2=?");
    const char *tail = NULL;
    rc = sqlite3_prepare_v2(ke->log, sql, sizeof(sql), &ke->select_seq_stmt, &tail);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "SELECT MAX(ballot) FROM ReplicaLog");
    rc = sqlite3_prepare_v2(ke->log, sql, sizeof(sql), &ke->select_ballot_stmt, &tail);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }

    snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO ReplicaLog VALUES(?, ?, ?, ?)");
    rc = sqlite3_prepare_v2(ke->log, sql, sizeof(sql), &ke->insert_stmt, &tail);
    if (rc != SQLITE_OK) {
        // TODO - Errors
    }


    ke->peers = malloc(sizeof(char *) * num_peers);
    int i;
    for (i = 0; i < num_peers; i++)
        ke->peers[i] = strdup(peers[i]);

    if (callbacks)
        memcpy(&ke->callbacks, callbacks, sizeof(kepaxos_callbacks_t));

    ke->commands = ht_create(128, 1024, (ht_free_item_callback_t)kepaxos_command_destroy);

    uint32_t max_ballot = kepaxos_max_ballot(ke);
    ke->ballot = (max_ballot & 0xFFFFFF00) | ke->my_index;

    SHC_DEBUG("Replica context created: %d replicas, starting ballot: %lu",
              ke->num_peers, ke->ballot);

    MUTEX_INIT(&ke->lock);

    if (pthread_create(&ke->expirer, NULL, kepaxos_expire_commands, ke) != 0) {
        return NULL;
    }
    return ke;
}

void
kepaxos_context_destroy(kepaxos_t *ke)
{
    ATOMIC_SET(ke->quit, 1);
    pthread_join(ke->expirer, NULL);

    sqlite3_finalize(ke->select_seq_stmt);
    sqlite3_finalize(ke->select_ballot_stmt);
    sqlite3_finalize(ke->insert_stmt);
    sqlite3_close(ke->log);

    int i;
    for (i = 0; i < ke->num_peers; i++)
        free(ke->peers[i]);
    free(ke->peers);

    ht_destroy(ke->commands);

    MUTEX_DESTROY(&ke->lock);

    free(ke);
}

static size_t
kepaxos_build_message(char **out,
                      kepaxos_msg_type_t mtype,
                      unsigned char ctype, 
                      uint32_t ballot,
                      void *key,
                      uint32_t klen,
                      void *data,
                      uint32_t dlen,
                      uint32_t seq,
                      int committed)
{
    size_t msglen = klen + dlen + 3 + (sizeof(uint32_t) * 4);
    char *msg = malloc(msglen);
    unsigned char committed_byte = committed ? 1 : 0;
    unsigned char mtype_byte = (unsigned char)mtype;
    unsigned char ctype_byte = (unsigned char)ctype;
    uint32_t nbo = htonl(ballot);
    memcpy(msg, &nbo, sizeof(uint32_t));

    nbo = htonl(seq);
    memcpy(msg + sizeof(uint32_t), &nbo, sizeof(uint32_t));

    memcpy(msg + (2*sizeof(uint32_t)), &mtype_byte, 1);
    memcpy(msg + (2*sizeof(uint32_t)) + 1, &ctype_byte, 1);
    memcpy(msg + (2*sizeof(uint32_t)) + 2, &committed_byte, 1);

    nbo = htonl(klen);
    memcpy(msg + (2*sizeof(uint32_t)) + 3, &nbo, sizeof(uint32_t));
    memcpy(msg + (3*sizeof(uint32_t)) + 3, key, klen);

    nbo = htonl(dlen);
    memcpy(msg + (3*sizeof(uint32_t)) + 3 + klen, &nbo, sizeof(uint32_t));
    memcpy(msg + (4*sizeof(uint32_t)) + 3 + klen, data, dlen);

    *out = msg;
    return msglen;
}

#if 0
static void *
kepaxos_recovery(void *arg)
{
    kepaxos_t *ke = (kepaxos_t *)arg;
    int i;

    while(!ATOMIC_READ(ke->quit)) {
        for (i = 0; i < ke->num_peers; i++) {
            if (i == ke->my_index)
                continue;
            uint32_t max_ballot = kepaxos_max_ballot(ke);
            char *msg = NULL;
            size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_RECOVERY, 0, max_ballot, NULL, 0, NULL, 0, 0, 0);
            kepaxos_response_t *responses = NULL;
            int num_responses = 0;
            int rc = ke->callbacks.send(&ke->peers[i], 1, (void *)msg, msglen, &responses, &num_responses);
            if (rc != 0 ) {
            }
        }
    }
    return NULL;
}
#endif

static int
kepaxos_send_preaccept(kepaxos_t *ke, uint32_t ballot, void *key, size_t klen, uint32_t seq)
{
    char *receivers[ke->num_peers-1];
    int i, n = 0;
    for (i = 0; i < ke->num_peers; i++) {
        if (i == ke->my_index)
            continue;
        receivers[n++] = ke->peers[i];
    }

    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_PRE_ACCEPT, 0, ballot, key, klen, NULL, 0, seq, 0);
    int rc = ke->callbacks.send(receivers, ke->num_peers-1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("pre_accept sent to %d peers for key %s (cmd: %02x, seq: %lu, ballot: %lu)",
                  n, keystr, seq, ballot);
    }

    return rc;
}

int
kepaxos_run_command(kepaxos_t *ke,
                    char *peer,
                    unsigned char type,
                    void *key,
                    size_t klen,
                    void *data,
                    size_t dlen)
{
    // Replica R1 receives a new set/del/evict request for key K
    MUTEX_LOCK(&ke->lock);
    uint32_t seq = last_seq_for_key(ke, key, klen);
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
    if (cmd) {
        MUTEX_LOCK(&cmd->condition_lock);
        pthread_cond_signal(&cmd->condition);
        MUTEX_UNLOCK(&cmd->condition_lock);
        if (cmd->key)
            free(cmd->key);
        if (cmd->data)
            free(cmd->data);
    } else {
        cmd = calloc(1, sizeof(kepaxos_cmd_t));
        MUTEX_INIT(&cmd->condition_lock);
        CONDITION_INIT(&cmd->condition);
        ht_set(ke->commands, key, klen, cmd, sizeof(kepaxos_cmd_t));
    }
    uint32_t interfering_seq = cmd ? cmd->seq : 0; 
    seq = MAX(seq, interfering_seq);
    // an eventually uncommitted command for K would be overwritten here
    // hence it will be ignored and will fail silently
    // (NOTE: in libshardcache we only care about the most recent command for a key 
    //        and not about the entire sequence of commands)
    cmd->seq = seq;
    cmd->type = type;
    cmd->key = malloc(klen);
    memcpy(cmd->key, key, klen);
    cmd->klen = klen;
    cmd->data = malloc(dlen);
    memcpy(cmd->data, data, dlen);
    cmd->dlen = dlen;
    cmd->status = KEPAXOS_CMD_STATUS_PRE_ACCEPTED;
    cmd->timestamp = time(NULL);
    uint32_t ballot = (ATOMIC_READ(ke->ballot)|0xFFFFFF00) >> 1;
    ballot++;
    ballot = (ballot << 1) | ke->my_index;
    ATOMIC_SET_IF(ke->ballot, <, ballot, uint32_t);
    MUTEX_LOCK(&cmd->condition_lock);
    MUTEX_UNLOCK(&ke->lock);
    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("New kepaxos command for key %s (cmd: %02x, seq: %lu, ballot: %lu)",
                  keystr, type, seq, ballot);
    }
    int rc = kepaxos_send_preaccept(ke, ballot, key, klen, seq);
    pthread_cond_wait(&cmd->condition, &cmd->condition_lock);
    MUTEX_UNLOCK(&cmd->condition_lock);
    return rc;
}

static int
kepaxos_send_pre_accept_response(kepaxos_t *ke,
                                 char *peer,
                                 uint32_t ballot,
                                 void *key,
                                 size_t klen,
                                 uint32_t seq,
                                 unsigned char committed)
{
    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_PRE_ACCEPT_RESPONSE,
                                          0, ballot, key, klen, NULL, 0, seq, committed);

    int rc = ke->callbacks.send(&peer, 1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

static int
kepaxos_send_commit(kepaxos_t *ke, kepaxos_cmd_t *cmd)
{
    char *receivers[ke->num_peers-1];
    int i, n = 0;
    for (i = 0; i < ke->num_peers; i++) {
        if (i == ke->my_index)
            continue;
        receivers[n++] = ke->peers[i];
    }

    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_COMMIT, cmd->type, cmd->ballot,
                                          cmd->key, cmd->klen, cmd->data, cmd->dlen, cmd->seq, 1);

    
    int rc =  ke->callbacks.send(receivers, ke->num_peers-1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

static int
kepaxos_commit(kepaxos_t *ke, kepaxos_cmd_t *cmd)
{
    ke->callbacks.commit(cmd->type, cmd->key, cmd->klen, cmd->data, cmd->dlen, 1, ke->callbacks.priv);
    set_last_seq_for_key(ke, cmd->key, cmd->klen, cmd->ballot, cmd->seq);
    int rc = kepaxos_send_commit(ke, cmd);
    kepaxos_command_destroy(cmd);
    return rc;
}

static int
kepaxos_send_accept(kepaxos_t *ke, uint32_t ballot, void *key, size_t klen, uint32_t seq)
{
    char *receivers[ke->num_peers-1];
    int i, n = 0;
    for (i = 0; i < ke->num_peers; i++) {
        if (i == ke->my_index)
            continue;
        receivers[n++] = ke->peers[i];
    }

    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_ACCEPT, 0, ballot, key, klen, NULL, 0, seq, 0);
    int rc = ke->callbacks.send(receivers, ke->num_peers-1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

static int
kepaxos_send_accept_response(kepaxos_t *ke,
                                 char *peer,
                                 uint32_t ballot,
                                 void *key,
                                 size_t klen,
                                 uint32_t seq,
                                 unsigned char committed)
{
    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, KEPAXOS_MSG_TYPE_ACCEPT_RESPONSE,
                                          0, ballot, key, klen, NULL, 0, seq, committed);
    int rc = ke->callbacks.send(&peer, 1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

int
kepaxos_received_command(kepaxos_t *ke, char *peer, void *cmd, size_t cmdlen)
{
    if (cmdlen < sizeof(uint32_t) * 4)
        return -1;

    // parse the message

    char *p = cmd;

    uint32_t ballot = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    uint32_t seq = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    unsigned char mtype = *p++;
    unsigned char ctype = *p++;
    unsigned char committed = *p++;

    uint32_t klen = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);
    void *key = p;
    p += klen;

    uint32_t dlen = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);
    void *data = dlen ? p : NULL;
    p += dlen;

    // done with parsing

    // update the ballot if the current ballot number is bigger
    uint32_t updated_ballot = (ballot&0xFFFFFF00) >> 1;
    updated_ballot++;
    ATOMIC_SET_IF(ke->ballot, <, (updated_ballot << 1) | ke->my_index, uint32_t);

    switch(mtype) {
        case KEPAXOS_MSG_TYPE_PRE_ACCEPT:
        {
            // Any replica R receiving a PRE_ACCEPT(BALLOT, K, SEQ) from R1
            MUTEX_LOCK(&ke->lock);
            uint32_t local_seq = last_seq_for_key(ke, key, klen);
            kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
            uint32_t interfering_seq = 0;
            if (cmd) {
                if (ballot < cmd->ballot) {
                    // ignore this message ... the ballot is too old
                    MUTEX_UNLOCK(&ke->lock);
                    return -1;
                }
                cmd->ballot = MAX(ballot, cmd->ballot);
                interfering_seq = cmd->seq;
            }
            interfering_seq = MAX(local_seq, interfering_seq);
            uint32_t max_seq = MAX(seq, interfering_seq);
            if (max_seq == seq)
                cmd->status = KEPAXOS_CMD_STATUS_PRE_ACCEPTED;
            committed = (max_seq == local_seq);
            MUTEX_UNLOCK(&ke->lock);
            return kepaxos_send_pre_accept_response(ke, peer, ATOMIC_READ(ke->ballot), key, klen, max_seq, (int)committed);
            break;
        }
        case KEPAXOS_MSG_TYPE_PRE_ACCEPT_RESPONSE:
        {
            MUTEX_LOCK(&ke->lock);
            kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
            if (cmd) {
                if (ballot < cmd->ballot) {
                    MUTEX_UNLOCK(&ke->lock);
                    return -1;
                }
                if (cmd->status != KEPAXOS_CMD_STATUS_PRE_ACCEPTED) {
                    MUTEX_UNLOCK(&ke->lock);
                    return -1;
                }
                cmd->votes = realloc(cmd->votes, sizeof(kepaxos_vote_t) * ++cmd->num_votes);
                cmd->votes[cmd->num_votes].seq = seq;
                cmd->votes[cmd->num_votes].ballot = ballot;
                cmd->votes[cmd->num_votes].peer = peer;
                cmd->max_seq = MAX(cmd->max_seq, seq);
                if (cmd->max_seq == seq)
                    cmd->max_voter = peer;
                if (cmd->num_votes < ke->num_peers/2)
                    return 0; // we don't have a quorum yet
                if (cmd->seq >= cmd->max_seq) {
                    // commit (short path)
                    ht_delete(ke->commands, key, klen, (void **)&cmd, NULL);
                    MUTEX_UNLOCK(&ke->lock);
                    return kepaxos_commit(ke, cmd);
                } else {
                    if (committed) {
                        ke->callbacks.recover(cmd->max_voter, key, klen, seq, ballot, ke->callbacks.priv);
                    } else {
                        // run the paxos-like protocol (long path)
                        free(cmd->votes);
                        cmd->votes = NULL;
                        cmd->num_votes = 0;
                        cmd->seq = cmd->max_seq + 1;
                        cmd->max_seq = 0;
                        cmd->max_voter = NULL;
                        uint32_t new_seq = cmd->seq;
                        MUTEX_UNLOCK(&ke->lock);
                        return kepaxos_send_accept(ke, ballot, key, klen, new_seq);
                    }
                }
            }
            MUTEX_UNLOCK(&ke->lock);
            break;
        }
        case KEPAXOS_MSG_TYPE_ACCEPT:
        {
            // Any replica R receiving an ACCEPT(BALLOT, K, SEQ) from R1
            int accepted_ballot = ballot;
            int accepted_seq = seq;
            MUTEX_LOCK(&ke->lock);
            kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
            if (cmd) {
                if (ballot < cmd->ballot) {
                    // ignore this message
                    MUTEX_UNLOCK(&ke->lock);
                    return 0;
                }
                if (seq < cmd->seq) {
                    accepted_ballot = cmd->ballot;
                    accepted_seq = cmd->seq;
                }
            } else {
                cmd = calloc(1, sizeof(kepaxos_cmd_t));
                cmd->key = malloc(klen);
                memcpy(cmd->key, key, klen);
                cmd->klen = klen;
                ht_set(ke->commands, key, klen, cmd, sizeof(kepaxos_cmd_t));
            }
            if (seq >= cmd->seq) {
                cmd->seq = seq;
                cmd->ballot = ballot;
                cmd->status = KEPAXOS_CMD_STATUS_ACCEPTED;
                accepted_ballot = ballot;
                accepted_seq = seq;
            }
            MUTEX_UNLOCK(&ke->lock);
            return kepaxos_send_accept_response(ke, peer, accepted_ballot, key, klen, accepted_seq, 0);
            break;
        }
        case KEPAXOS_MSG_TYPE_ACCEPT_RESPONSE:
        {
            if (shardcache_log_level() >= LOG_DEBUG) {
                char keystr[1024];
                KEY2STR(key, klen, keystr, sizeof(keystr));
                SHC_DEBUG("pre_accept response received for key %s (seq: %lu, ballot: %lu)",
                          keystr, seq, ballot);
            }

            MUTEX_LOCK(&ke->lock);
            kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
            if (cmd) {
                if (ballot < cmd->ballot) {
                    MUTEX_UNLOCK(&ke->lock);
                    return -1;
                }
                if (cmd->status != KEPAXOS_CMD_STATUS_ACCEPTED) {
                    MUTEX_UNLOCK(&ke->lock);
                    return -1;
                }
                cmd->votes = realloc(cmd->votes, sizeof(kepaxos_vote_t) * ++cmd->num_votes);
                cmd->votes[cmd->num_votes].seq = seq;
                cmd->votes[cmd->num_votes].ballot = ballot;
                cmd->votes[cmd->num_votes].peer = peer;
                cmd->max_seq = MAX(cmd->max_seq, seq);
                if (cmd->max_seq == seq)
                    cmd->max_voter = peer;
                int i;
                int count_ok = 0;
                for (i = 0; i < cmd->num_votes; i++)
                    if (cmd->votes[i].seq == seq && cmd->votes[i].ballot == ballot)
                        count_ok++;

                if (count_ok < ke->num_peers/2) {
                    if (cmd->num_votes >= ke->num_peers/2) {
                        // we need to retry paxos increasing the ballot number

                        if (cmd->seq <= cmd->max_seq)
                            cmd->seq++;

                        uint32_t new_ballot = ATOMIC_READ(ke->ballot);
                        cmd->ballot = new_ballot;
                        free(cmd->votes);
                        cmd->votes = NULL;
                        cmd->num_votes = 0;
                        cmd->max_seq = 0;
                        cmd->max_voter = NULL;
                        uint32_t new_seq = cmd->seq;
                        MUTEX_UNLOCK(&ke->lock);
                        return kepaxos_send_accept(ke, new_ballot, key, klen, new_seq);
                    }
                    MUTEX_UNLOCK(&ke->lock);
                    return 0; // we don't have a quorum yet
                }
                // the command has been accepted by a quorum
                ht_delete(ke->commands, key, klen, (void **)&cmd, NULL);
                MUTEX_UNLOCK(&ke->lock);
                return kepaxos_commit(ke, cmd);
            }
            break;
        }
        case KEPAXOS_MSG_TYPE_COMMIT:
        {
            MUTEX_LOCK(&ke->lock);
            // Any replica R on receiving a COMMIT(BALLOT, K, SEQ, CMD, DATA) message
            kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
            if (cmd && cmd->seq == seq && cmd->ballot > ballot) {
                // ignore this message ... the ballot is too old
                MUTEX_UNLOCK(&ke->lock);
                return -1;
            }
            uint32_t last_recorded_seq = last_seq_for_key(ke, key, klen);
            if (seq < last_recorded_seq) {
                // ignore this commit message (it's too old)
                MUTEX_UNLOCK(&ke->lock);
                return 0;
            }
            ke->callbacks.commit(ctype, key, klen, data, dlen, 0, ke->callbacks.priv);
            set_last_seq_for_key(ke, key, klen, ballot, seq);
            if (cmd && cmd->seq == seq)
                ht_delete(ke->commands, key, klen, NULL, NULL);
            MUTEX_UNLOCK(&ke->lock);
            break;
        }
        default:
            break;
    }
    return 0;
}

