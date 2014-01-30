
#include <shardcache_client.h>
#include <linklist.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <testing.h>

int quit = 0;
static void stop_node(int sig)
{
    quit = 1;
}

static void start_nodes(shardcache_node_t *nodes, int num_nodes, linked_list_t *children)
{
    int pid;
    int i;
    for (i = 0; i < num_nodes; i++) {
        pid = fork();
        if (pid) {
            int *child = malloc(sizeof(int));
            *child = pid;
            push_value(children, child);
        } else {
            // child
            signal(SIGQUIT, stop_node);

            shardcache_t *cache = shardcache_create(nodes[i].label,
                                                    nodes,
                                                    num_nodes,
                                                    NULL,
                                                    NULL,
                                                    5,
                                                    1<<29);


            while (!quit)
                sleep(1);

            shardcache_destroy(cache);
            exit(0);
        }
    }
}

static void stop_nodes(linked_list_t *children)
{
    int *child;
    while ((child = shift_value(children))) {
        kill(*child, 3);
        waitpid(*child, NULL, 0);
        printf("child %d exited\n", *child);
        free(child);
    }
}

int main(int argc, char **argv)
{
    int i;
    int num_nodes = 2;
    linked_list_t *children = create_list();
    shardcache_node_t nodes[num_nodes];

    shardcache_log_init("shardcached", LOG_DEBUG+4);

    for (i = 0; i < num_nodes; i++) {
        sprintf(nodes[i].label, "peer%d", i);
        sprintf(nodes[i].address, "localhost:975%d", i);
    }

    t_init();

    t_testing("start_nodes(nodes, num_nodes, children)");
    start_nodes(nodes, num_nodes, children);
    t_validate_int(list_count(children), 2);

    sleep(1);

    t_testing("shardcache_client_create(nodes, num_nodes, NULL)");
    shardcache_client_t *client = shardcache_client_create(nodes, num_nodes, NULL);
    t_validate_int((client != NULL), 1);

    char key[32];
    char val[32];
    void *value = NULL;
    size_t size;

    sprintf(key, "test_key1");
    sprintf(val, "test_value1");

    t_testing("shardcache_client_set(client, test_key1, 9, test_value1, 11, 0) == 0");
    int ret = shardcache_client_set(client, key, strlen(key), val, strlen(val), 0);
    t_validate_int(ret, 0);

    t_testing("shardcache_client_get(client, test_key1, 9, &value) == test_value1");
    size = shardcache_client_get(client, key, strlen(key), (void **)&value);
    t_validate_buffer(value, size, val, strlen(val));
    free(value);

    t_testing("shardcache_client_del(client, test_key1, 9) == 0");
    ret = shardcache_client_del(client, "test_key1", 9);
    t_validate_int(ret, 0);

    t_testing("shardcache_client_get(client, test_key1, 9, &value) == NULL");
    size = shardcache_client_get(client, "test_key1", 9, &value);
    t_validate_int((size == 0 && value == NULL), 1);

    t_testing("shardcache_client_exists(client, test_key1, 9) == 0");
    ret = shardcache_client_exists(client, "test_key1", 9);
    t_validate_int(ret, 0);

    shardcache_client_del(client, "test_key2", 9);
    t_testing("shardcache_client_add(client, test_key2, 9, test_value2, 11, 0) == 0");
    ret = shardcache_client_add(client, "test_key2", 9, "test_value2", 11, 0);
    t_validate_int(ret, 0);

    t_testing("shardcache_client_exists(client, test_key2, 9) == 1");
    ret = shardcache_client_exists(client, "test_key2", 9);
    t_validate_int(ret, 1);

    t_testing("shardcache_client_get(client, test_key2, 9, &value) == test_value2");
    size = shardcache_client_get(client, "test_key2", 9, &value);
    t_validate_buffer(value, size, "test_value2", 11);
    free(value);

    t_testing("shardcache_client_add(client, test_key2, 9, test_value_modified, 19, 0) == 1");
    ret = shardcache_client_add(client, "test_key2", 9, "test_value_modified", 19, 0);
    t_validate_int(ret, 1);

    // the value is unchanged because already existing
    t_testing("shardcache_client_get(client, test_key2, 9, &value) == test_value2 (unchanged)");
    size = shardcache_client_get(client, "test_key2", 9, &value);
    t_validate_buffer(value, size, "test_value2", 11);
    free(value);

    // the value is unchanged because already existing
    t_testing("shardcache_client_offset(client, test_key2, 9, 5, &partial, 5) == value");
    char partial[4];
    size = shardcache_client_offset(client, "test_key2", 9, 5, &partial, 5);
    t_validate_buffer(partial, 5, "value", 5);

    shardcache_client_t *client1 = shardcache_client_create(&nodes[0], 1, NULL);
    shardcache_client_t *client2 = shardcache_client_create(&nodes[1], 1, NULL);

    // the following tests communication among peers
    // (sets 100 keys using one node and reads them using the other node,
    //  so some keys will be owned from the node used for writing while other
    //  ones will be owned by the node used for reading)
    t_testing("shardcache_client_set(c1, k, kl, &v) == shardcache_client_get(c2, k, kl)");
    int failed = 0;
    for (i = 100; i < 200; i++) {
        char k[64];
        char v[64];
        char *vv;

        sprintf(k, "test_key%d", i);
        sprintf(v, "test_value%d", i);
        shardcache_client_set(client1, k, strlen(k), v, strlen(v), 0);
        size_t s = shardcache_client_get(client2, k, strlen(k), (void **)&vv);
        if (s == 0) {
            t_failure("no data for key %s", k);
            failed = 1;
        } else if(strcmp(vv, v) != 0) {
            t_failure("%s != %s", vv, v);
            failed = 1;
        }
        free(vv);
    }
    if (!failed)
        t_success();

    stop_nodes(children);

    destroy_list(children);

    shardcache_client_destroy(client);
    shardcache_client_destroy(client1);
    shardcache_client_destroy(client2);

    exit(0);
}
