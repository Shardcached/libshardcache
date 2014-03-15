#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <ut.h>
#include <libgen.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

#include <sqlite3.h>

#define HAVE_UINT64_T
#include <siphash.h>

#include <kepaxos.h>

static int total_messages_sent = 0;
static int total_values_committed = 0;

typedef struct {
    kepaxos_t *ke;
    int online;
} kepaxos_node;

typedef struct {
    kepaxos_node *contexts;
    char *me;
} callback_argument;

static int send_callback(char **recipients,
                         int num_recipients,
                         void *cmd,
                         size_t cmd_len,
                         void *priv)
{
    callback_argument *arg = (callback_argument *)priv;
    __sync_add_and_fetch(&total_messages_sent, num_recipients);

    char *shuffled[num_recipients];
    memcpy(shuffled, recipients, sizeof(char *) * num_recipients);

    int i;
    for (i = num_recipients - 1; i > 0; i--) {
        int j = rand()%i;
        char *tmp = shuffled[i];
        shuffled[i] = shuffled[j];
        shuffled[j] = tmp;
    }

    for (i = 0; i < num_recipients; i++) {
        char *node = shuffled[i];
        node += 4;
        int index = strtol(node, NULL, 10) - 1;
        if (arg->contexts[index].online) {
            void *response = NULL;
            size_t response_len = 0;
            int rc = kepaxos_received_command(arg->contexts[index].ke, cmd, cmd_len, &response, &response_len);
            if (rc == 0 && response_len) {
                node = arg->me + 4;
                index = strtol(node, NULL, 10) - 1;
                kepaxos_received_response(arg->contexts[index].ke, response, response_len);
                free(response);
            }
        }
    }
    return 0;
}

static int commit_callback(unsigned char type,
                           void *key,
                           size_t klen,
                           void *data,
                           size_t dlen,
                           int leader,
                           void *priv)
{
    __sync_add_and_fetch(&total_values_committed, 1);
    return 0;
}

static int recover_callback(char *peer,
                            void *key,
                            size_t klen,
                            uint64_t seq,
                            int32_t prio,
                            void *priv)
{
    return 0;
}

typedef struct {
    uint32_t seq;
    uint32_t ballot;
} kepaxos_log_item;

int fetch_log(char *dbfile, void *key, size_t klen, kepaxos_log_item *item)
{
    sqlite3 *log;
    int rc = sqlite3_open(dbfile, &log);
    if (rc != SQLITE_OK) {
        return -1;
    }

    const char *tail = NULL;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(log, "SELECT seq, ballot FROM ReplicaLog WHERE keyhash1=? AND keyhash2=?", -1, &stmt, &tail);
    if (rc != SQLITE_OK) {
        sqlite3_close(log);
        return -1;
    }

    uint64_t keyhash1 = sip_hash24((unsigned char *)"0123456789ABCDEF", key, klen);
    uint64_t keyhash2 = sip_hash24((unsigned char *)"ABCDEF0987654321", key, klen);

    rc = sqlite3_bind_int64(stmt, 1, keyhash1);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(log);
        return -1;
    }
    rc = sqlite3_bind_int64(stmt, 2, keyhash2);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(log);
        return -1;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        item->seq = sqlite3_column_int(stmt, 0);
        item->ballot = sqlite3_column_int(stmt, 1);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(log);
    return rc;
}

int check_log_consistency(kepaxos_node *contexts, int start_index, int end_index)
{
    int i;
    int check = 1;
    kepaxos_log_item prev_item = { 0, 0 };
    for (i = start_index; i <= end_index; i++) {
        char dbfile[2048];
        snprintf(dbfile, sizeof(dbfile), "/tmp/kepaxos_test%d.db", i);
        kepaxos_log_item item;
        fetch_log(dbfile, "test_key", 8, &item);
        if (i > 0 && memcmp(&prev_item, &item, sizeof(prev_item)) != 0) {
            check = 0;
            break;
        }
        memcpy(&prev_item, &item, sizeof(prev_item));
    }
    return check;
}

void *repeated_command(void *priv)
{
    kepaxos_node *contexts = (kepaxos_node *)priv;
    int i;
    for (i = 0; i < 10; i++) {
        // send the command to a random replica
        int j = rand()%5;
        kepaxos_run_command(contexts[j].ke, 0x00, "test_key", 8, "test_value", 10);
    }
    return NULL;
}

int main(int argc, char **argv)
{
    srand(time(NULL));
    ut_init(basename(argv[0]));

    char *nodes[] = { "node1", "node2", "node3", "node4", "node5" };

    kepaxos_node contexts[5];

    ut_testing("kepaxos_context_create(\"/tmp/kepaxos_test.db\", nodes, 5, 1, &callbacks)");
    int i;
    callback_argument arg[5];
    for (i = 0; i < 5; i++) {
        arg[i].contexts = contexts;
        arg[i].me = nodes[i];
        kepaxos_callbacks_t callbacks = {
            .send = send_callback,
            .commit = commit_callback,
            .recover = recover_callback,
            .priv = &arg[i]
        };
        char dbfile[2048];
        snprintf(dbfile, sizeof(dbfile), "/tmp/kepaxos_test%d.db", i);
        contexts[i].ke = kepaxos_context_create(dbfile, nodes, 5, i, 1, &callbacks);
        if (!contexts[i].ke) {
            ut_failure("Can't create a kepaxos instance");
            goto __exit;
        }
        contexts[i].online = 0;
    }
    ut_success();

    contexts[0].online = 1; // start by bringing online only 1 replica
    ut_testing("kepaxos_run_command() timeouts after 1 second");
    int rc = kepaxos_run_command(contexts[0].ke, 0x00, "test_key", 8, "test_value", 10);
    ut_validate_int(rc, -1);

    ut_testing("kepaxos_run_command() triggered 4 messages");
    ut_validate_int(total_messages_sent, 4);

    // bring up all the other replicas
    for (i = 1; i < 5; i++)
        contexts[i].online = 1;

    ut_testing("kepaxos_run_command() propagates to all replicas");
    rc = kepaxos_run_command(contexts[0].ke, 0x00, "test_key", 8, "test_value", 10);
    ut_validate_int(total_values_committed, 5);

    ut_testing("log is consistent on all replicas");
    
    int check = check_log_consistency(contexts, 0, 4);
    if (check)
        ut_success();
    else
        ut_failure("Log is not aligned on all replicas");


    // bring down 2 replicas (node4 and node5) , 3 should be enough to keep working
    contexts[3].online = 0;
    contexts[4].online = 0;
    rc = kepaxos_run_command(contexts[0].ke, 0x00, "test_key", 8, "test_value", 10);
    ut_testing("kepaxos_run_command() succeeds with only N/2+1 active replicas");
    check = check_log_consistency(contexts, 0, 2);
    if (check) {
        check = check_log_consistency(contexts, 0, 4);
        if (!check) {
            ut_success();
        } else {
            ut_failure("Log doesn't differ on the offline replicas");
        }
    } else {
        ut_failure("Log is not aligned on the active replicas");
    }


    int committed = total_values_committed;
    contexts[2].online = 0; // replica 2 crashes as well
    ut_testing("kepaxos_run_command() fails with less than N/2+1 active replicas");
    rc = kepaxos_run_command(contexts[0].ke, 0x00, "test_key2", 8, "test_value2", 10);
    ut_validate_int(committed, total_values_committed);

    ut_testing("offline replicas come back online and a new value is set using one of them");

    // bring all replicas back online
    contexts[2].online = 1;
    contexts[3].online = 1;
    contexts[4].online = 1;

    // the following will trigger the long path (paxos-like instance) to align
    // the crashed replicas (3 and 4) which are behind (they missed a command)
    rc = kepaxos_run_command(contexts[3].ke, 0x00, "test_key", 8, "test_value", 10);
    // now all replicas should be aligned
    check = check_log_consistency(contexts, 0, 4);
    if (check)
        ut_success();
    else
        ut_failure("Log is not aligned on all the replicas");

    // now let's test concurrency when the same key is tried to be changed
    // by multiple replicas at the same time
    ut_testing("concurrent kepaxos_run_command() from random replicas");
    pthread_t threads[2];
    for (i = 0; i < 2; i++) {
        pthread_create(&threads[i], NULL, repeated_command, contexts);
    }

    for (i = 0; i < 2; i++) {
        pthread_join(threads[i], NULL);
    }

    check = check_log_consistency(contexts, 0, 4);
    if (check)
        ut_success();
    else
        ut_failure("Log is not aligned on all the replicas");

    for (i = 0; i < 5; i++) {
        kepaxos_context_destroy(contexts[i].ke);
        char dbfile[2048];
        snprintf(dbfile, sizeof(dbfile), "/tmp/kepaxos_test%d.db", i);
        unlink(dbfile);
    }
__exit:
    ut_summary();
    exit(ut_failed); 
}

