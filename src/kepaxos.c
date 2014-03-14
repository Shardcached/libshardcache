#include "sqlite3.h"
#include "kepaxos.h"
#include "atomic.h"
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

#define KEPAXOS_CMD_TTL 30 // default to 30 seconds

#define BALLOT2NODE(__k, __b) (__k)->peers[ (__b) & 0x00000000000000FF ]

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
} kepaxos_msg_type_t;

typedef struct {
    char *peer;
    uint64_t ballot;
    uint64_t seq;
    unsigned char mtype;
    unsigned char ctype;
    unsigned char committed;
    void *key;
    uint32_t klen;
    void *data;
    uint32_t dlen;
} kepaxos_msg_t;

typedef struct {
    char *peer;
    uint64_t ballot;
    void *key;
    size_t klen;
    uint64_t seq;
} kepaxos_vote_t;

struct __kepaxos_cmd_s {
    unsigned char type;
    kepaxos_msg_type_t msg;
    kepaxos_cmd_status_t status;
    uint64_t seq;
    void *key;
    size_t klen;
    void *data;
    size_t dlen;
    kepaxos_vote_t *votes;
    uint16_t num_votes;
    uint64_t max_seq;
    uint64_t max_seq_committed;
    char *max_voter;
    uint64_t ballot;
    time_t timestamp;
    int timeout;
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
    uint64_t ballot;
    sqlite3_stmt *select_seq_stmt;
    sqlite3_stmt *select_ballot_stmt;
    sqlite3_stmt *insert_stmt;
    pthread_t expirer;
    int quit;
    int timeout;
};

static inline void
kepaxos_compute_key_hashes(void *key, size_t klen, uint64_t *hash1, uint64_t *hash2)
{
    unsigned char auth1[16] = "0123456789ABCDEF";
    unsigned char auth2[16] = "ABCDEF0987654321";

    *hash1 = sip_hash24(auth1, key, klen);
    *hash2 = sip_hash24(auth2, key, klen);
}

static inline uint64_t
kepaxos_max_ballot(kepaxos_t *ke)
{
    uint64_t ballot = 0;

    int rc = sqlite3_reset(ke->select_ballot_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the select-ballot prepared statement: %s",
                  sqlite3_errmsg(ke->log));
        return 0;
    }

    rc = sqlite3_step(ke->select_ballot_stmt);
    if (rc == SQLITE_ROW)
        ballot = sqlite3_column_int64(ke->select_ballot_stmt, 0);

    return ballot;
}

static inline uint64_t
last_seq_for_key(kepaxos_t *ke, void *key, size_t klen, uint64_t *ballot)
{
    uint64_t keyhash1, keyhash2;
    uint64_t seq = 0;

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    int rc = sqlite3_reset(ke->select_seq_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the select-seq prepared statement: %s",
                  sqlite3_errmsg(ke->log));
        return 0;
    }

    rc = sqlite3_bind_int64(ke->select_seq_stmt, 1, keyhash1);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash1 column (select_seq): %s",
                  sqlite3_errmsg(ke->log));
        return 0;
    }

    rc = sqlite3_bind_int64(ke->select_seq_stmt, 2, keyhash2);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash2 column (select_seq): %s",
                  sqlite3_errmsg(ke->log));
        return seq;
    }

    rc = sqlite3_step(ke->select_seq_stmt);
    if (rc == SQLITE_ROW) {
        seq = sqlite3_column_int64(ke->select_seq_stmt, 0);
        if (ballot)
            *ballot = sqlite3_column_int64(ke->select_seq_stmt, 1);
    } else {
        SHC_ERROR("Can't execut the select-seq statement: %s",
                  sqlite3_errmsg(ke->log));
    }

    return seq;
}

static inline void
set_last_seq_for_key(kepaxos_t *ke, void *key, size_t klen, uint64_t ballot, uint64_t seq)
{
    uint64_t keyhash1, keyhash2;

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    int rc = sqlite3_reset(ke->insert_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the insert statement: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 1, ballot);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the ballot column: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 2, keyhash1);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash1 column: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 3, keyhash2);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash2 column: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }

    rc = sqlite3_bind_int64(ke->insert_stmt, 4, seq);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the seq column: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }

    rc = sqlite3_step(ke->insert_stmt);
    if (rc != SQLITE_DONE) {
        SHC_ERROR("Can't execute the insert statement: %s",
                  sqlite3_errmsg(ke->log));
        return;
    }
}


static void
kepaxos_command_destroy(kepaxos_cmd_t *c)
{
    MUTEX_LOCK(&c->condition_lock);
    pthread_cond_broadcast(&c->condition);
    MUTEX_UNLOCK(&c->condition_lock);
}

static void
kepaxos_command_free(kepaxos_cmd_t *c)
{
    free(c->key);
    if (c->data)
        free(c->data);

    if (c->votes)
        free(c->votes);

    MUTEX_DESTROY(&c->condition_lock);
    CONDITION_DESTROY(&c->condition);
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
    kepaxos_t *ke = (kepaxos_t *)user;
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)value;
    if (cmd->timeout > 0 && time(NULL) > (cmd->timestamp + cmd->timeout)) {
        if (cmd->status == KEPAXOS_CMD_STATUS_PRE_ACCEPTED ||
            cmd->status == KEPAXOS_CMD_STATUS_ACCEPTED)
        {
            ke->callbacks.recover(BALLOT2NODE(ke, cmd->ballot),
                    key, klen, cmd->seq, cmd->ballot, ke->callbacks.priv);
        }
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
        usleep(50000);
    }
    return NULL;
}

static inline void
kepaxos_reset_ballot(kepaxos_t *ke)
{
    MUTEX_LOCK(&ke->lock);
    // TODO - implement logic to handle the edge case of consuming all available
    //        ballot numbers so we need to restart from 1
    MUTEX_UNLOCK(&ke->lock);
}

static inline uint64_t
update_ballot(kepaxos_t *ke, uint64_t ballot)
{
    // update the ballot if the current ballot number is bigger
    uint64_t real_ballot = (ballot&0xFFFFFF00) >> 8;
    uint64_t updated_ballot = real_ballot + 1;
    if (real_ballot == 0) {
        kepaxos_reset_ballot(ke);
    } else if (updated_ballot == 0) {
        ATOMIC_SET(ke->ballot, (uint64_t)ke->my_index);
    } else {
        ATOMIC_SET_IF(ke->ballot, <, (updated_ballot << 8) | ke->my_index, uint64_t);
    }
    return ATOMIC_READ(ke->ballot);
}

kepaxos_t *
kepaxos_context_create(char *dbfile,
                       char **peers,
                       int num_peers,
                       int my_index,
                       int timeout,
                       kepaxos_callbacks_t *callbacks)
{
    kepaxos_t *ke = calloc(1, sizeof(kepaxos_t));

    ke->timeout = timeout > 0 ? timeout : KEPAXOS_CMD_TTL;
    ke->my_index = my_index;
    ke->ballot = (1 << 8) | ke->my_index;

    int rc = sqlite3_open(dbfile, &ke->log);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't open the sqlite dbfile: %s",
                  sqlite3_errmsg(ke->log));
        free(ke);
        return NULL;
    }

    const char *create_table_sql = "CREATE TABLE IF NOT EXISTS ReplicaLog "
                                   "(ballot int, keyhash1 int, keyhash2 int, seq int,"
                                   "PRIMARY KEY(keyhash1, keyhash2))";
    rc = sqlite3_exec(ke->log, create_table_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't create the log table: %s",
                  sqlite3_errmsg(ke->log));
        sqlite3_close(ke->log);
        free(ke);
        return NULL;
    }

    const char *create_index_sql = "CREATE INDEX IF NOT EXISTS ballot_index ON ReplicaLog (ballot DESC)";
    rc = sqlite3_exec(ke->log, create_index_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't create the ballot index on log table: %s",
                  sqlite3_errmsg(ke->log));
        sqlite3_close(ke->log);
        free(ke);
        return NULL;
    }

    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT seq, ballot FROM ReplicaLog WHERE keyhash1=? AND keyhash2=?");
    const char *tail = NULL;
    rc = sqlite3_prepare_v2(ke->log, sql, -1, &ke->select_seq_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the select-seq prepared statement: %s",
                  sqlite3_errmsg(ke->log));
        sqlite3_close(ke->log);
        free(ke);
        return NULL;
    }

    snprintf(sql, sizeof(sql), "SELECT MAX(ballot) FROM ReplicaLog");
    rc = sqlite3_prepare_v2(ke->log, sql, -1, &ke->select_ballot_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the select-max-ballot prepared statement: %s",
                  sqlite3_errmsg(ke->log));
        sqlite3_finalize(ke->select_seq_stmt);
        sqlite3_close(ke->log);
        free(ke);
        return NULL;
    }

    snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO ReplicaLog VALUES(?, ?, ?, ?)");
    rc = sqlite3_prepare_v2(ke->log, sql, -1, &ke->insert_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the insert-or-replace prepared statement: %s",
                  sqlite3_errmsg(ke->log));
        sqlite3_finalize(ke->select_seq_stmt);
        sqlite3_finalize(ke->select_ballot_stmt);
        sqlite3_close(ke->log);
        free(ke);
        return NULL;
    }

    ke->peers = malloc(sizeof(char *) * num_peers);
    ke->num_peers = num_peers;

    int i;
    for (i = 0; i < num_peers; i++)
        ke->peers[i] = strdup(peers[i]);

    if (callbacks)
        memcpy(&ke->callbacks, callbacks, sizeof(kepaxos_callbacks_t));

    ke->commands = ht_create(128, 1024, (ht_free_item_callback_t)kepaxos_command_destroy);

    uint64_t max_ballot = kepaxos_max_ballot(ke) >> 8;
    max_ballot++;
    update_ballot(ke, max_ballot);

    SHC_DEBUG("Replica context created: %d replicas, starting ballot: %lu",
              ke->num_peers, ke->ballot);

    MUTEX_INIT(&ke->lock);

    if (pthread_create(&ke->expirer, NULL, kepaxos_expire_commands, ke) != 0) {
        sqlite3_finalize(ke->select_seq_stmt);
        sqlite3_finalize(ke->select_ballot_stmt);
        sqlite3_close(ke->log);
        for (i = 0; i < num_peers; i++)
            free(ke->peers[i]);
        free(ke->peers);
        ht_destroy(ke->commands);
        MUTEX_DESTROY(&ke->lock);
        free(ke);
        return NULL;
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

static inline size_t
kepaxos_build_message(char **out,
                      char *sender,
                      kepaxos_msg_type_t mtype,
                      unsigned char ctype, 
                      uint64_t ballot,
                      void *key,
                      uint32_t klen,
                      void *data,
                      uint32_t dlen,
                      uint64_t seq,
                      int committed)
{
    size_t sender_len = strlen(sender) + 1; // include the terminating null byte
    size_t msglen = klen + dlen + 3 + (sizeof(uint32_t) * 6) + sizeof(uint16_t) + sender_len;
    char *msg = malloc(msglen);
    unsigned char committed_byte = committed ? 1 : 0;
    unsigned char mtype_byte = (unsigned char)mtype;
    unsigned char ctype_byte = (unsigned char)ctype;

    char *p = msg;

    uint16_t slen_nbo = htons(sender_len);
    memcpy(p, &slen_nbo, sizeof(uint16_t));
    p += sizeof(uint16_t);
    memcpy(p, sender, sender_len);
    p += sender_len;

    uint32_t ballot_low = ballot & 0x00000000FFFFFFFF;
    uint32_t ballot_high = ballot >> 32;
    uint32_t nbo = htonl(ballot_high);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);

    nbo = htonl(ballot_low);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);


    uint32_t seq_low = seq & 0x00000000FFFFFFFF;
    uint32_t seq_high = seq >> 32;
    nbo = htonl(seq_high);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);

    nbo = htonl(seq_low);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);

    *p++ = mtype_byte;
    *p++ = ctype_byte;
    *p++ = committed_byte;

    nbo = htonl(klen);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);

    if (klen) {
        memcpy(p, key, klen);
        p += klen;
    }

    nbo = htonl(dlen);
    memcpy(p, &nbo, sizeof(uint32_t));
    p += sizeof(uint32_t);

    if (dlen)
        memcpy(p, data, dlen);

    *out = msg;
    return msglen;
}

static int
kepaxos_send_preaccept(kepaxos_t *ke, uint64_t ballot, void *key, size_t klen, uint64_t seq)
{
    char *receivers[ke->num_peers-1];
    int i, n = 0;
    for (i = 0; i < ke->num_peers; i++) {
        if (i == ke->my_index)
            continue;
        receivers[n++] = ke->peers[i];
    }

    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, ke->peers[ke->my_index], KEPAXOS_MSG_TYPE_PRE_ACCEPT,
                                          0, ballot, key, klen, NULL, 0, seq, 0);
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

static kepaxos_cmd_t *
kepaxos_command_create(kepaxos_t *ke,
                       uint64_t seq,
                       unsigned char type,
                       void *key,
                       size_t klen,
                       void *data,
                       size_t dlen)
{
    kepaxos_cmd_t *cmd = calloc(1, sizeof(kepaxos_cmd_t));
    MUTEX_INIT(&cmd->condition_lock);
    CONDITION_INIT(&cmd->condition);

    // this will release/abort the previous command on the same key(if any)
    kepaxos_cmd_t *prev_cmd =  NULL;
    ht_get_and_set(ke->commands, key, klen, cmd, sizeof(kepaxos_cmd_t), (void **)&prev_cmd, NULL);
    if (prev_cmd) {
        uint64_t interfering_seq = prev_cmd->seq; 
        seq = MAX(seq, interfering_seq) + 1;
        kepaxos_command_destroy(prev_cmd);
    } else {
        seq++;
    }

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
    cmd->timeout = ke->timeout;
    cmd->ballot = ATOMIC_READ(ke->ballot);
    return cmd;
}

int
kepaxos_run_command(kepaxos_t *ke,
                    unsigned char type,
                    void *key,
                    size_t klen,
                    void *data,
                    size_t dlen)
{
    // Replica R1 receives a new set/del/evict request for key K
    MUTEX_LOCK(&ke->lock);
    uint64_t last_seq = last_seq_for_key(ke, key, klen, NULL);

    kepaxos_cmd_t *cmd = kepaxos_command_create(ke, last_seq, type, key, klen, data, dlen);

    uint64_t seq = cmd->seq;
    uint64_t ballot = cmd->ballot;
    MUTEX_UNLOCK(&ke->lock);

    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("New kepaxos command for key %s (cmd: %02x, seq: %lu, ballot: %lu)",
                  keystr, type, seq, ballot);
    }

    int rc = kepaxos_send_preaccept(ke, ballot, key, klen, seq);

    if (rc >= 0) {
        MUTEX_LOCK(&ke->lock);
        kepaxos_cmd_t *now_cmd = (kepaxos_cmd_t *)ht_get(ke->commands, key, klen, NULL);
        if (now_cmd == cmd) {
            // our command wasn't invalidated in the meanwhile
            // let's wait for its completion (either success or failure)
            MUTEX_LOCK(&cmd->condition_lock);
            MUTEX_UNLOCK(&ke->lock);
            pthread_cond_wait(&cmd->condition, &cmd->condition_lock);
            MUTEX_UNLOCK(&cmd->condition_lock);
        } else {
            // some other thread invalidated our command, let's forget about it
            MUTEX_UNLOCK(&ke->lock);
        }
        kepaxos_command_free(cmd);
    }

    // here the command have either succeeded or failed, we can
    // determine it by checking if the current committed seq is 
    // equal or greater than the seq we tried to commit
    uint64_t current_seq = last_seq_for_key(ke, key, klen, NULL);
    return (current_seq >= seq) ? 0 : -1;
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
    size_t msglen = kepaxos_build_message(&msg, ke->peers[ke->my_index], KEPAXOS_MSG_TYPE_COMMIT, cmd->type,
                                          cmd->ballot, cmd->key, cmd->klen, cmd->data, cmd->dlen, cmd->seq, 1);

    
    int rc =  ke->callbacks.send(receivers, ke->num_peers-1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

static inline int
kepaxos_commit(kepaxos_t *ke, kepaxos_cmd_t *cmd)
{
    int rc = ke->callbacks.commit(cmd->type, cmd->key, cmd->klen, cmd->data, cmd->dlen, 1, ke->callbacks.priv);
    if (rc == 0) {
        set_last_seq_for_key(ke, cmd->key, cmd->klen, cmd->ballot, cmd->seq);
        rc = kepaxos_send_commit(ke, cmd);
    }
    kepaxos_command_destroy(cmd);
    // TODO - recovery if commit failed? try again?
    return rc;
}

static int
kepaxos_send_accept(kepaxos_t *ke, uint64_t ballot, void *key, size_t klen, uint64_t seq)
{
    char *receivers[ke->num_peers-1];
    int i, n = 0;
    for (i = 0; i < ke->num_peers; i++) {
        if (i == ke->my_index)
            continue;
        receivers[n++] = ke->peers[i];
    }

    char *msg = NULL;
    size_t msglen = kepaxos_build_message(&msg, ke->peers[ke->my_index], KEPAXOS_MSG_TYPE_ACCEPT,
                                          0, ballot, key, klen, NULL, 0, seq, 0);
    int rc = ke->callbacks.send(receivers, ke->num_peers-1, (void *)msg, msglen, ke->callbacks.priv);
    free(msg);
    return rc;
}

static inline int
kepaxos_parse_message(char *msg,
                      size_t msglen,
                      kepaxos_msg_t *msg_struct)
{
    size_t expected_len = (sizeof(uint32_t) * 6) + 3 + sizeof(uint16_t);
    if (msglen < expected_len)
        return -1;

    char *p = msg;

    uint16_t sender_len = ntohs(*((uint16_t *)p));
    p += sizeof(uint16_t);
    msg_struct->peer = p;
    p += sender_len;

    expected_len += sender_len;
    if (msglen < expected_len)
        return -1;

    uint32_t ballot_high = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    uint32_t ballot_low = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    msg_struct->ballot = ((uint64_t)ballot_high << 32) | ((uint64_t)ballot_low);

    uint32_t seq_high = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    uint32_t seq_low = ntohl(*((uint32_t *)p));
    p += sizeof(uint32_t);

    msg_struct->seq = ((uint64_t)seq_high << 32) | ((uint64_t)seq_low);

    msg_struct->mtype = *p++;
    msg_struct->ctype = *p++;
    msg_struct->committed = *p++;

    msg_struct->klen = ntohl(*((uint32_t *)p));

    expected_len += msg_struct->klen;
    if (msglen < expected_len)
        return -1;

    p += sizeof(uint32_t);
    if (msg_struct->klen) {
        msg_struct->key = p;
        p += msg_struct->klen;
    } else {
        msg_struct->key = NULL;
    }

    msg_struct->dlen = ntohl(*((uint32_t *)p));

    expected_len += msg_struct->dlen;
    if (msglen < expected_len)
        return -1;

    if (msg_struct->dlen) {
        p += sizeof(uint32_t);
        msg_struct->data = p;
    } else {
        msg_struct->data = NULL;
    }

    return 0;
}

static inline int
kepaxos_handle_preaccept(kepaxos_t *ke, kepaxos_msg_t *msg, void **response, size_t *response_len)
{
    // Any replica R receiving a PRE_ACCEPT(BALLOT, K, SEQ) from R1
    MUTEX_LOCK(&ke->lock);
    uint64_t local_ballot = 0;
    uint64_t local_seq = last_seq_for_key(ke, msg->key, msg->klen, &local_ballot);

    if (local_seq == msg->seq && local_ballot == msg->ballot) {
        // ignore this message ... we already have committed this command
        MUTEX_UNLOCK(&ke->lock);
        return -1;
    }

    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, msg->key, msg->klen, NULL);
    uint64_t interfering_seq = 0;
    if (cmd) {
        if (msg->ballot < cmd->ballot) {
            // ignore this message ... the ballot is too old
            MUTEX_UNLOCK(&ke->lock);
            return -1;
        }
        cmd->ballot = MAX(msg->ballot, cmd->ballot);
        interfering_seq = cmd->seq;
    } else {
        cmd = calloc(1, sizeof(kepaxos_cmd_t));
        cmd->key = malloc(msg->klen);
        memcpy(cmd->key, msg->key, msg->klen);
        cmd->klen = msg->klen;
        cmd->seq = msg->seq;
        cmd->ballot = msg->ballot;
        cmd->timestamp = time(NULL);
        cmd->timeout = ke->timeout;
        ht_set(ke->commands, msg->key, msg->klen, cmd, sizeof(kepaxos_cmd_t));
    }
    interfering_seq = MAX(local_seq, interfering_seq);
    uint64_t max_seq = MAX(msg->seq, interfering_seq);
    if (msg->seq >= interfering_seq) {
        if (cmd->status == KEPAXOS_CMD_STATUS_ACCEPTED) {
            ke->callbacks.recover(BALLOT2NODE(ke, cmd->ballot),
                    msg->key, msg->klen, cmd->seq, cmd->ballot, ke->callbacks.priv);
        }
        cmd->status = KEPAXOS_CMD_STATUS_PRE_ACCEPTED;
        cmd->seq = interfering_seq;
    }
    int committed = (max_seq == local_seq);
    uint64_t ballot = cmd->ballot;
    MUTEX_UNLOCK(&ke->lock);

    *response_len = kepaxos_build_message((char **)response, ke->peers[ke->my_index], KEPAXOS_MSG_TYPE_PRE_ACCEPT_RESPONSE,
                                          0, ballot, msg->key, msg->klen, NULL, 0, max_seq, committed);
    return 0;
}

static inline int
kepaxos_handle_preaccept_response(kepaxos_t *ke, kepaxos_msg_t *msg)
{
    MUTEX_LOCK(&ke->lock);
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, msg->key, msg->klen, NULL);
    if (cmd) {
        if (msg->ballot < cmd->ballot) {
            MUTEX_UNLOCK(&ke->lock);
            return -1;
        }
        if (cmd->status != KEPAXOS_CMD_STATUS_PRE_ACCEPTED) {
            MUTEX_UNLOCK(&ke->lock);
            return -1;
        }
        cmd->votes = realloc(cmd->votes, sizeof(kepaxos_vote_t) * (cmd->num_votes + 1));
        cmd->votes[cmd->num_votes].seq = msg->seq;
        cmd->votes[cmd->num_votes].ballot = msg->ballot;
        cmd->votes[cmd->num_votes].peer = msg->peer;
        cmd->num_votes++;
        if (msg->seq != cmd->max_seq) {
            cmd->max_seq = MAX(cmd->max_seq, msg->seq);
            cmd->max_seq_committed = (msg->committed && cmd->max_seq == msg->seq);
        } else if (msg->committed) {
            cmd->max_seq_committed = 1;
        }

        if (cmd->max_seq == msg->seq)
            cmd->max_voter = msg->peer;

        if (cmd->num_votes < ke->num_peers/2) {
            MUTEX_UNLOCK(&ke->lock);
            return 0; // we don't have a quorum yet
        }
        if (cmd->seq > cmd->max_seq || (cmd->seq == cmd->max_seq && !cmd->max_seq_committed))
        {
            // commit (short path)
            ht_delete(ke->commands, msg->key, msg->klen, (void **)&cmd, NULL);
            MUTEX_UNLOCK(&ke->lock);
            return kepaxos_commit(ke, cmd);
        } else {
            // run the paxos-like protocol (long path)
            free(cmd->votes);
            cmd->votes = NULL;
            cmd->num_votes = 0;
            cmd->seq = cmd->max_seq + 1;
            cmd->max_seq = 0;
            cmd->max_voter = NULL;
            uint64_t ballot = ATOMIC_READ(ke->ballot);
            cmd->ballot = ballot;
            uint64_t new_seq = cmd->seq;
            cmd->status = KEPAXOS_CMD_STATUS_ACCEPTED;
            MUTEX_UNLOCK(&ke->lock);
            return kepaxos_send_accept(ke, ballot, msg->key, msg->klen, new_seq);
        }
    }
    MUTEX_UNLOCK(&ke->lock);
    return 0;
}

static inline int
kepaxos_handle_accept(kepaxos_t *ke, kepaxos_msg_t *msg, void *response, size_t *response_len)
{
    // Any replica R receiving an ACCEPT(BALLOT, K, SEQ) from R1
    uint64_t accepted_ballot = msg->ballot;
    uint64_t accepted_seq = msg->seq;
    MUTEX_LOCK(&ke->lock);

    uint64_t local_ballot = 0;
    uint64_t local_seq = last_seq_for_key(ke, msg->key, msg->klen, &local_ballot);

    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, msg->key, msg->klen, NULL);
    if (cmd) {
        if (msg->ballot < cmd->ballot) {
            // ignore this message
            MUTEX_UNLOCK(&ke->lock);
            return 0;
        }
        if (msg->seq < cmd->seq) {
            accepted_ballot = cmd->ballot;
            accepted_seq = cmd->seq;
        }
    } else {
        cmd = calloc(1, sizeof(kepaxos_cmd_t));
        cmd->key = malloc(msg->klen);
        memcpy(cmd->key, msg->key, msg->klen);
        cmd->klen = msg->klen;
        ht_set(ke->commands, msg->key, msg->klen, cmd, sizeof(kepaxos_cmd_t));
    }
    if (msg->seq >= cmd->seq) {
        cmd->seq = msg->seq;
        cmd->ballot = msg->ballot;
        cmd->status = KEPAXOS_CMD_STATUS_ACCEPTED;
        cmd->timestamp = time(NULL);
        cmd->timeout = ke->timeout;
        accepted_ballot = msg->ballot;
        accepted_seq = msg->seq;
    }
    // inform the sender if we have already committed this seq
    int committed = (accepted_seq == local_seq);
    MUTEX_UNLOCK(&ke->lock);
    if (shardcache_log_level() >= LOG_DEBUG && msg->key) {
        char keystr[1024];
        KEY2STR(msg->key, msg->klen, keystr, sizeof(keystr));
        SHC_DEBUG("%s returns %llu (%d) ballot: %llu for key %s to peer %s\n",
                  ke->peers[ke->my_index], accepted_seq, committed, accepted_ballot, keystr, msg->peer);
    }
    *response_len = kepaxos_build_message((char **)response, ke->peers[ke->my_index], KEPAXOS_MSG_TYPE_ACCEPT_RESPONSE,
                                          0, accepted_ballot, msg->key, msg->klen, NULL, 0, accepted_seq, committed);
    return 0;
}

static inline int
kepaxos_handle_accept_response(kepaxos_t *ke, kepaxos_msg_t *msg)
{
    if (shardcache_log_level() >= LOG_DEBUG && msg->key) {
        char keystr[1024];
        KEY2STR(msg->key, msg->klen, keystr, sizeof(keystr));
        SHC_DEBUG("pre_accept response received for key %s (seq: %lu, ballot: %lu)",
                  keystr, msg->seq, msg->ballot);
    }

    MUTEX_LOCK(&ke->lock);
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, msg->key, msg->klen, NULL);
    if (cmd) {
        if (msg->ballot < cmd->ballot) {
            MUTEX_UNLOCK(&ke->lock);
            return -1;
        }
        if (cmd->status != KEPAXOS_CMD_STATUS_ACCEPTED) {
            MUTEX_UNLOCK(&ke->lock);
            return -1;
        }

        if (cmd->seq == msg->seq && msg->committed) {
            uint64_t new_ballot = ATOMIC_READ(ke->ballot);
            cmd->seq++;
            cmd->ballot = new_ballot;
            free(cmd->votes);
            cmd->votes = NULL;
            cmd->num_votes = 0;
            cmd->max_seq = 0;
            cmd->max_voter = NULL;
            uint64_t new_seq = cmd->seq;
            MUTEX_UNLOCK(&ke->lock);
            return kepaxos_send_accept(ke, new_ballot, msg->key, msg->klen, new_seq);
        }
        cmd->votes = realloc(cmd->votes, sizeof(kepaxos_vote_t) * (cmd->num_votes + 1));
        cmd->votes[cmd->num_votes].seq = msg->seq;
        cmd->votes[cmd->num_votes].ballot = msg->ballot;
        cmd->votes[cmd->num_votes].peer = msg->peer;
        cmd->num_votes++;
        cmd->max_seq = MAX(cmd->max_seq, msg->seq);
        if (cmd->max_seq == msg->seq)
            cmd->max_voter = msg->peer;
        int i;
        int count_ok = 0;
        for (i = 0; i < cmd->num_votes; i++)
            if (cmd->votes[i].seq == msg->seq && cmd->votes[i].ballot == msg->ballot)
                count_ok++;

        if (count_ok < ke->num_peers/2) {
            if (cmd->num_votes >= ke->num_peers/2) {
                // we need to retry paxos increasing the ballot number

                if (cmd->seq <= cmd->max_seq)
                    cmd->seq++;

                uint64_t new_ballot = ATOMIC_READ(ke->ballot);
                cmd->ballot = new_ballot;
                free(cmd->votes);
                cmd->votes = NULL;
                cmd->num_votes = 0;
                cmd->max_seq = 0;
                cmd->max_voter = NULL;
                uint64_t new_seq = cmd->seq;
                MUTEX_UNLOCK(&ke->lock);
                return kepaxos_send_accept(ke, new_ballot, msg->key, msg->klen, new_seq);
            }
            MUTEX_UNLOCK(&ke->lock);
            return 0; // we don't have a quorum yet
        }
        // the command has been accepted by a quorum
        ht_delete(ke->commands, msg->key, msg->klen, (void **)&cmd, NULL);
        MUTEX_UNLOCK(&ke->lock);
        return kepaxos_commit(ke, cmd);
    }
    MUTEX_UNLOCK(&ke->lock);
    return 0;
}

static inline int
kepaxos_handle_commit(kepaxos_t *ke, kepaxos_msg_t *msg)
{
    MUTEX_LOCK(&ke->lock);
    // Any replica R on receiving a COMMIT(BALLOT, K, SEQ, CMD, DATA) message
    kepaxos_cmd_t *cmd = (kepaxos_cmd_t *)ht_get(ke->commands, msg->key, msg->klen, NULL);
    if (cmd && cmd->seq == msg->seq && cmd->ballot > msg->ballot) {
        // ignore this message ... the ballot is too old
        SHC_DEBUG("Ignoring commit message, ballot too old: (%lld -- %lld)",
                  cmd->ballot, msg->ballot);
        MUTEX_UNLOCK(&ke->lock);
        return -1;
    }
    uint64_t last_recorded_seq = last_seq_for_key(ke, msg->key, msg->klen, NULL);
    if (msg->seq < last_recorded_seq) {
        // ignore this commit message (it's too old)
        if (shardcache_log_level() >= LOG_DEBUG && msg->key) {
            char keystr[1024];
            KEY2STR(msg->key, msg->klen, keystr, sizeof(keystr));
            SHC_DEBUG("Ignoring commit message, seq too old for key %s: (%lld -- %lld)",
                      keystr, msg->seq, last_recorded_seq);
        }
        MUTEX_UNLOCK(&ke->lock);
        return 0;
    }

    if (shardcache_log_level() >= LOG_DEBUG && msg->key) {
        char keystr[1024];
        KEY2STR(msg->key, msg->klen, keystr, sizeof(keystr));
        SHC_DEBUG("Committing key %s (seq: %llu, ballot: %llu)\n",
                  keystr, msg->seq, msg->ballot);
    }

    ke->callbacks.commit(msg->ctype, msg->key, msg->klen,
                         msg->data, msg->dlen, 0, ke->callbacks.priv);

    set_last_seq_for_key(ke, msg->key, msg->klen, msg->ballot, msg->seq);

    if (cmd && cmd->seq == msg->seq) {
        ht_delete(ke->commands, msg->key, msg->klen, NULL, NULL);
        kepaxos_command_free(cmd);
    }
    MUTEX_UNLOCK(&ke->lock);
    return 0;
}

int
kepaxos_received_response(kepaxos_t *ke, void *res, size_t reslen)
{
    if (reslen < sizeof(uint32_t) * 4)
        return -1;

    kepaxos_msg_t msg;

    int rc = kepaxos_parse_message(res, reslen, &msg);

    if (rc != 0)
        return -1;

    update_ballot(ke, msg.ballot);

    switch(msg.mtype) {
         case KEPAXOS_MSG_TYPE_PRE_ACCEPT_RESPONSE:
            return kepaxos_handle_preaccept_response(ke, &msg);
        case KEPAXOS_MSG_TYPE_ACCEPT_RESPONSE:
            return kepaxos_handle_accept_response(ke, &msg);
        default:
            break;
    }

    return -1;
}

int
kepaxos_received_command(kepaxos_t *ke,
                         void *cmd,
                         size_t cmdlen,
                         void **response,
                         size_t *response_len)
{
    if (cmdlen < sizeof(uint32_t) * 4)
        return -1;

    kepaxos_msg_t msg;

    int rc = kepaxos_parse_message(cmd, cmdlen, &msg);

    if (rc != 0)
        return -1;

    update_ballot(ke, msg.ballot);

    switch(msg.mtype) {
        case KEPAXOS_MSG_TYPE_PRE_ACCEPT:
            return kepaxos_handle_preaccept(ke, &msg, response, response_len);
        case KEPAXOS_MSG_TYPE_ACCEPT:
            return kepaxos_handle_accept(ke, &msg, response, response_len);
        case KEPAXOS_MSG_TYPE_COMMIT:
            return kepaxos_handle_commit(ke, &msg);
        default:
            break;
    }
    return -1;
}

uint64_t kepaxos_ballot(kepaxos_t *ke)
{
    return ATOMIC_READ(ke->ballot);
}
