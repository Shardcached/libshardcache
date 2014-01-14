#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include <chash.h>
#include <shardcache_client.h>

void usage(char *prgname) {
    printf("Usage: %s <Command> <Key>\n"
           "   Commands: \n"
           "        get        <Key>\n"
           "        get_async <Key>\n"
           "        set       <Key> [ <Expire> ] (gets value on stdin)\n"
           "        add       <Key> [ <Expire> ] (gets value on stdin)\n"
           "        exists    <Key>\n"
           "        touch     <Key>\n"
           "        del       <Key>\n"
           "        evict     <Key>\n"
           "        index     [ <node> ]\n"
           "        stats     [ <node> ]\n"
           "        check     [ <node> ]\n\n", prgname);
    exit(-2);
}

int num_nodes = 0;
shardcache_node_t *nodes = NULL;

static int parse_nodes_string(char *str)
{
    char *copy = strdup(str);
    char *s = copy;

    while (s && *s) {
        char *tok = strsep(&s, ",");
        if(tok) {
            char *label = strsep(&tok, ":");
            char *addr = tok;
            if (!addr) {
                free(copy);
                return -1;
            }
            num_nodes++;
            nodes = realloc(nodes, num_nodes * sizeof(shardcache_node_t));
            shardcache_node_t *node = &nodes[num_nodes-1];
            snprintf(node->label, sizeof(node->label), "%s", label);
            snprintf(node->address, sizeof(node->address), "%s", addr);
        } 
    }
    free(copy);
    return 0;
}

int print_chunk(char *peer,
                 void *key,
                 size_t klen,
                 void *data,
                 size_t len,
                 void *priv)
{
    fwrite(data, 1, len, stdout);
    return 0;
}

int main (int argc, char **argv) {
    if ((argc < 3) && (argc != 2 ||
        (strcmp(argv[1], "stats") != 0 && 
         strcmp(argv[1], "check") != 0 &&
         strcmp(argv[1], "index") != 0)))
    {
        usage(argv[0]);
    }

    char *shc_hosts = getenv("SHC_HOSTS");
    if (!shc_hosts) {
        fprintf(stderr, "SHC_HOSTS environment variable not found!\n");
        exit(-1);
    }
    char *secret = getenv("SHC_SECRET");

    if (parse_nodes_string(shc_hosts) != 0) {
        fprintf(stderr, "Can't parse the nodes string : %s!\n", shc_hosts);
        exit(-1);
    }

    shardcache_client_t *client = shardcache_client_create(nodes, num_nodes, secret);

    int rc = 0;
    int is_boolean = 0;

    char *cmd = argv[1];
    if (strcasecmp(cmd, "get") == 0) {
        void *out = NULL;
        size_t len = shardcache_client_get(client, argv[2], strlen(argv[2]), &out); 
        if (len && out) {
            print_chunk(NULL, NULL, 0, out, len, NULL);
        }
    } else if (strcasecmp(cmd, "geta") == 0 || strcasecmp(cmd, "get_async") == 0)
    {
        rc = shardcache_client_get_async(client, argv[2], strlen(argv[2]), print_chunk, NULL); 
    } else if (strcasecmp(cmd, "set") == 0 ||
               strcasecmp(cmd, "add") == 0)
    {
        char *in = NULL;
        size_t s = 0;
        char buf[1024];
        int rb = fread(buf, 1, 1024, stdin);
        while (rb > 0) {
            in = realloc(in, s+rb);
            memcpy(in + s, buf, rb);
            s += rb;
            rb = fread(buf, 1, 1024, stdin);
        }
        if (strcasecmp(cmd, "set") == 0) {
            rc = shardcache_client_set(client, argv[2], strlen(argv[2]), in, s, argc > 3 ? strtol(argv[3], NULL, 10) : 0);
        } else {
            rc = shardcache_client_add(client, argv[2], strlen(argv[2]), in, s, argc > 3 ? strtol(argv[3], NULL, 10) : 0);
            if (rc == 1)
                printf("Already exists!\n");
        }
    } else if (strcasecmp(cmd, "del") == 0 || strcasecmp(cmd, "delete") == 0) {
        rc = shardcache_client_del(client, argv[2], strlen(argv[2]));
    } else if (strcasecmp(cmd, "evict") == 0) {
        rc = shardcache_client_evict(client, argv[2], strlen(argv[2]));
    } else if (strcasecmp(cmd, "exists") == 0) {
        rc = shardcache_client_exists(client, argv[2], strlen(argv[2]));
        is_boolean = 1;
    } else if (strcasecmp(cmd, "touch") == 0) {
        rc = shardcache_client_touch(client, argv[2], strlen(argv[2]));
    } else if (strcasecmp(cmd, "stats") == 0) {
        int found = 0;
        char *selected_node = NULL;
        if (argc > 2)
            selected_node = argv[2];

        char *stats = NULL;
        size_t len;
        int i;
        for (i = 0; i < num_nodes; i++) {
            if (selected_node && strcmp(nodes[i].label, selected_node) != 0)
                continue;
            found++;
            printf("* Stats for node: %s (%s)\n\n", nodes[i].label, nodes[i].address);
            int rc = shardcache_client_stats(client, nodes[i].label, &stats, &len); 
            if (rc == 0)
                printf("%s\n", stats);
            else
                printf("Error querying node: %s (%s)\n", nodes[i].label, nodes[i].address);
            if (stats)
                free(stats);
            printf("\n");
        }
        if (found == 0 && selected_node)
            fprintf(stderr, "Error: Unknown node %s\n", selected_node);
    } else if (strcasecmp(cmd, "check") == 0) {
        int found = 0;
        char *selected_node = NULL;
        if (argc > 2)
            selected_node = argv[2];

        int i;
        for (i = 0; i < num_nodes; i++) {
            if (selected_node && strcmp(nodes[i].label, selected_node) != 0)
                continue;
            found++;
            int rc = shardcache_client_check(client, nodes[i].label);
            if (rc == 0)
                printf("%s OK\n", nodes[i].label);
            else
                printf("%s NOT OK\n", nodes[i].label);
        }
        if (found == 0 && selected_node)
            fprintf(stderr, "Error: Unknown node %s\n", selected_node);
    } else if (strcasecmp(cmd, "index") == 0) {
        int found = 0;
        char *selected_node = NULL;
        if (argc > 2)
            selected_node = argv[2];

        int i;
        for (i = 0; i < num_nodes; i++) {
            if (selected_node && strcmp(nodes[i].label, selected_node) != 0)
                continue;
            found++;
            printf("* Index for node: %s (%s)\n\n", nodes[i].label, nodes[i].address);
            shardcache_storage_index_t *index = shardcache_client_index(client, nodes[i].label);
            if (index) {
                int n;
                for (n = 0; n < index->size; n ++) {
                    shardcache_storage_index_item_t *item = &index->items[n];
                    char keystr[item->klen+1];
                    snprintf(keystr, sizeof(keystr), "%s", (char *)item->key);
                    printf("%s => %u\n", keystr, (uint32_t)item->vlen);
                }
                shardcache_free_index(index);
            } else {
                printf("%s NOT OK\n", nodes[i].label);
            }
            printf("\n");
        }
        if (found == 0 && selected_node)
            fprintf(stderr, "Error: Unknown node %s\n", selected_node);
    } else {
        usage(argv[0]);
    }

    if (is_boolean) {
        if (rc == 0) {
            printf("NO\n");
        } else if (rc == 1) {
            printf("YES\n");
            rc = 0;
        } else {
            printf("ERR\n");
        }
    } else {
        if (rc == 0)
            printf("OK\n");
        else
            printf("ERR\n");
    }

    if (shardcache_client_errno(client) != SHARDCACHE_CLIENT_OK) {
        fprintf(stderr, "errorno: %d, errstr: %s\n",
                shardcache_client_errno(client), shardcache_client_errstr(client));
    }

    shardcache_client_destroy(client);
    exit(rc);
}

