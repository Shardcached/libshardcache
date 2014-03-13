#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <ut.h>
#include <libgen.h>
#include <stdio.h>

#include <kepaxos.h>


static int total_messages_sent = 0;

typedef struct {
    kepaxos_t **contexts;
    char *me;
} callback_argument;

static int send_callback(char **recipients,
                         int num_recipients,
                         void *cmd,
                         size_t cmd_len,
                         void *priv)
{
    callback_argument *arg = (callback_argument *)priv;
    total_messages_sent += num_recipients;
    if (total_messages_sent > 4) {
        // now let's start forwarding messages
        int i;
        for (i = 0; i < num_recipients; i++) {
            char *node = recipients[i];
            node += 4;
            int index = strtol(node, NULL, 10) - 1;
            kepaxos_received_command(arg->contexts[index], arg->me, cmd, cmd_len);
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
    return 0;
}

static int recover_callback(char *peer,
                            void *key,
                            size_t klen,
                            uint32_t seq,
                            int32_t prio,
                            void *priv)
{
    return 0;
}


int main(int argc, char **argv)
{
    ut_init(basename(argv[0]));

    char *nodes[] = { "node1", "node2", "node3", "node4", "node5" };

    kepaxos_t *contexts[5];


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
        contexts[i] = kepaxos_context_create(dbfile, nodes, 5, i, 1, &callbacks);
        if (!contexts[i]) {
            ut_failure("Can't create a kepaxos instance");
            goto __exit;
        }
    }
    ut_success();

    ut_testing("kepaxos_run_command() timeouts after 1 second");
    int rc = kepaxos_run_command(contexts[0], "node1", 0x00, "test_key", 8, "test_value", 10);
    ut_validate_int(rc, -1);

    ut_testing("kepaxos_run_command() triggered 4 messages");
    ut_validate_int(total_messages_sent, 4);

    rc = kepaxos_run_command(contexts[0], "node1", 0x00, "test_key", 8, "test_value", 10);
    for (i = 0; i < 5; i++) {
    }

    for (i = 0; i < 5; i++) {
        kepaxos_context_destroy(contexts[i]);
    }
__exit:
    ut_summary();
    exit(ut_failed); 
}

