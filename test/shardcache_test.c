#include <shardcache.h>
#include <shardcache_client.h>
#include <linklist.h>
#include <unistd.h>
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

int main(int argc, char **argv)
{
    int i;
    int num_nodes = 2;
    linked_list_t *children = create_list();
    shardcache_node_t nodes[num_nodes];

    shardcache_log_init("shardcached", LOG_WARNING);

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

    t_testing("shardcache_client_add(client, test_key2, 9, test_value2, 11, 0) == 0");
    ret = shardcache_client_add(client, "test_key2", 9, "test_value2", 11, 0);
    t_validate_int(ret, 0);

    t_testing("shardcache_client_exists(client, test_key2, 9) == 0");
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

    int *child;
    while ((child = shift_value(children))) {
        kill(*child, 3);
        waitpid(*child, NULL, 0);
        printf("child %d exited\n", *child);
        free(child);
    }

    exit(0);
}
