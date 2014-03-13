#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <ut.h>
#include <libgen.h>
#include <stdio.h>

#include <kepaxos.h>


static int total_messages_sent = 0;

static int send_callback(char **recipients,
                         int num_recipients,
                         void *cmd,
                         size_t cmd_len,
                         void *priv)
{
    total_messages_sent += num_recipients;
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

    kepaxos_callbacks_t callbacks = {
        .send = send_callback,
        .commit = commit_callback,
        .recover = recover_callback,
        .priv = NULL
    };

    ut_testing("kepaxos_context_create(\"/tmp/kepaxos_test.db\", nodes, 5, 1, &callbacks)");
    kepaxos_t *ke = kepaxos_context_create("/tmp/kepaxos_test.db", nodes, 5, 1, &callbacks);
    if (ke) {
        ut_success();
    } else {
        ut_failure("Can't create a kepaxos instance");
        goto __exit;
    }

    ut_testing("kepaxos_run_command() timeouts after 1 second");
    int rc = kepaxos_run_command(ke, "node1", 0x00, "test_key", 8, "test_value", 10);
    ut_validate_int(rc, -1);

    ut_testing("kepaxos_run_command() triggered 4 messages");
    ut_validate_int(total_messages_sent, 4);

__exit:
    ut_summary();
    exit(ut_failed); 
}

