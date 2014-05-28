#include "kepaxos_log.h"
#include "sqlite3.h"
#include "shardcache.h"

#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

struct __kepaxos_log_s {
    sqlite3 *dbh;
    sqlite3_stmt *select_seq_stmt;
    sqlite3_stmt *select_ballot_stmt;
    sqlite3_stmt *insert_stmt;
};

kepaxos_log_t *
kepaxos_log_create(char *dbfile)
{
    kepaxos_log_t *log = calloc(1, sizeof(kepaxos_log_t));

    int rc = sqlite3_open(dbfile, &log->dbh);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't open the sqlite dbfile: %s",
                  sqlite3_errmsg(log->dbh));
        free(log);
        return NULL;
    }

    const char *create_table_sql = "CREATE TABLE IF NOT EXISTS ReplicaLog "
                                   "(ballot int, keyhash1 int, keyhash2 int, seq int, key blob,"
                                   "PRIMARY KEY(keyhash1, keyhash2))";
    rc = sqlite3_exec(log->dbh, create_table_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't create the log table: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_close(log->dbh);
        free(log);
        return NULL;
    }

    const char *create_index_sql = "CREATE INDEX IF NOT EXISTS ballot_index ON ReplicaLog (ballot DESC)";
    rc = sqlite3_exec(log->dbh, create_index_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't create the ballot index on log table: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_close(log->dbh);
        free(log);
        return NULL;
    }

    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT seq, ballot FROM ReplicaLog WHERE keyhash1=? AND keyhash2=?");
    const char *tail = NULL;
    rc = sqlite3_prepare_v2(log->dbh, sql, -1, &log->select_seq_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the select-seq prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_close(log->dbh);
        free(log);
        return NULL;
    }

    snprintf(sql, sizeof(sql), "SELECT MAX(ballot) FROM ReplicaLog");
    rc = sqlite3_prepare_v2(log->dbh, sql, -1, &log->select_ballot_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the select-max-ballot prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_finalize(log->select_seq_stmt);
        sqlite3_close(log->dbh);
        free(log);
        return NULL;
    }

    snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO ReplicaLog VALUES(?, ?, ?, ?, ?)");
    rc = sqlite3_prepare_v2(log->dbh, sql, -1, &log->insert_stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the insert-or-replace prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_finalize(log->select_seq_stmt);
        sqlite3_finalize(log->select_ballot_stmt);
        sqlite3_close(log->dbh);
        free(log->dbh);
        free(log);
        return NULL;
    }

    return log;
}

void
kepaxos_log_destroy(kepaxos_log_t *log)
{
    sqlite3_finalize(log->select_seq_stmt);
    sqlite3_finalize(log->select_ballot_stmt);
    sqlite3_finalize(log->insert_stmt);
    sqlite3_close(log->dbh);
    free(log);
}

static inline void
kepaxos_compute_key_hashes(void *key, size_t klen, uint64_t *hash1, uint64_t *hash2)
{
    unsigned char auth1[16] = "0123456789ABCDEF";
    unsigned char auth2[16] = "ABCDEF0987654321";

    *hash1 = sip_hash24(auth1, key, klen);
    *hash2 = sip_hash24(auth2, key, klen);
}

uint64_t
kepaxos_max_ballot(kepaxos_log_t *log)
{
    uint64_t ballot = 0;

    int rc = sqlite3_reset(log->select_ballot_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the select-ballot prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        return 0;
    }

    rc = sqlite3_step(log->select_ballot_stmt);
    if (rc == SQLITE_ROW)
        ballot = sqlite3_column_int64(log->select_ballot_stmt, 0);

    return ballot;
}

uint64_t
kepaxos_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t *ballot)
{
    uint64_t keyhash1, keyhash2;
    uint64_t seq = 0;

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    int rc = sqlite3_reset(log->select_seq_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the select-seq prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        return 0;
    }

    rc = sqlite3_bind_int64(log->select_seq_stmt, 1, keyhash1);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash1 column (select_seq): %s",
                  sqlite3_errmsg(log->dbh));
        return 0;
    }

    rc = sqlite3_bind_int64(log->select_seq_stmt, 2, keyhash2);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash2 column (select_seq): %s",
                  sqlite3_errmsg(log->dbh));
        return seq;
    }

    rc = sqlite3_step(log->select_seq_stmt);
    if (rc == SQLITE_ROW) {
        seq = sqlite3_column_int64(log->select_seq_stmt, 0);
        if (ballot)
            *ballot = sqlite3_column_int64(log->select_seq_stmt, 1);
    } else if (!rc == SQLITE_DONE) {
        SHC_ERROR("Can't execut the select-seq statement: %s",
                  sqlite3_errmsg(log->dbh));
    }

    return seq;
}

void
kepaxos_set_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t ballot, uint64_t seq)
{
    uint64_t keyhash1, keyhash2;

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);

    int rc = sqlite3_reset(log->insert_stmt);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't reset the insert statement: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_bind_int64(log->insert_stmt, 1, ballot);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the ballot column: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_bind_int64(log->insert_stmt, 2, keyhash1);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash1 column: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_bind_int64(log->insert_stmt, 3, keyhash2);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the keyhash2 column: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_bind_int64(log->insert_stmt, 4, seq);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the seq column: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_bind_blob(log->insert_stmt, 5, key, klen, SQLITE_TRANSIENT);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the seq column: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }

    rc = sqlite3_step(log->insert_stmt);
    if (rc != SQLITE_DONE) {
        SHC_ERROR("Can't execute the insert statement: %s",
                  sqlite3_errmsg(log->dbh));
        return;
    }
}


int 
kepaxos_diff_from_ballot(kepaxos_log_t *log, uint64_t ballot, kepaxos_log_item_t **items, int *num_items)
{
    const char *tail = NULL;
    sqlite3_stmt *stmt = NULL;

    int rc = sqlite3_prepare_v2(log->dbh,
                                "SELECT ballot, seq, key FROM ReplicaLog WHERE ballot > ? ORDER BY ballot ASC",
                                -1, &stmt, &tail);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't initialize the select-seq prepared statement: %s",
                  sqlite3_errmsg(log->dbh));
        sqlite3_close(log->dbh);
        return -1;
    }

    rc = sqlite3_bind_int64(stmt, 1, ballot);
    if (rc != SQLITE_OK) {
        SHC_ERROR("Can't bind the ballot column (select_diff): %s",
                  sqlite3_errmsg(log->dbh));
        return 0;
    }

    int nitems = 0;
    kepaxos_log_item_t *itms = NULL;
    while(sqlite3_step(stmt) == SQLITE_ROW) {
        itms = realloc(itms, sizeof(kepaxos_log_item_t) * (nitems + 1));
        kepaxos_log_item_t *item = &itms[++nitems];
        item->ballot = sqlite3_column_int64(stmt, 0);
        item->seq = sqlite3_column_int64(stmt, 1);
        item->klen = sqlite3_column_bytes(stmt, 2); 
        item->key = malloc(item->klen);
        memcpy(item->key, sqlite3_column_blob(stmt, 2), item->klen); 
    } 
    
    sqlite3_finalize(stmt);
    *items = itms;
    *num_items = nitems;

    return 0;
}

void kepaxos_release_diff_items(kepaxos_log_item_t *items, int num_items)
{
    int i;
    for (i = 0; i < num_items; i++)
        free(items[i].key);
    free(items);
}
