#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include <chash.h>
#include <shardcache_client.h>

void usage(char *prgname) {
    printf("Usage: %s <Command> <Key>\n"
           "   Commands: \n"
           "        get   <Key>\n"
           "        set   <Key> [ <Expire> ] (gets value on stdin)\n"
           "        del   <Key>\n"
           "        evict <Key>\n\n", prgname);
    exit(-1);
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

int main (int argc, char **argv) {
    if (argc < 3) {
        usage(argv[0]);
    }

    char *shc_hosts = getenv("SHC_HOSTS");
    if (!shc_hosts) {
        fprintf(stderr, "SHC_HOSTS environment variable not found!\n");
        exit(-1);
    }

    if (parse_nodes_string(shc_hosts) != 0) {
        fprintf(stderr, "Can't parse the nodes string : %s!\n", shc_hosts);
        exit(-1);
    }

    shardcache_client_t *client = shardcache_client_create(nodes, num_nodes, NULL);

    int rc = 0;
    char *cmd = argv[1];
    if (strcasecmp(cmd, "get") == 0) {
        void *v = NULL;
        size_t s = shardcache_client_get(client, argv[2], strlen(argv[2]), &v); 
        if (s) {
            fwrite(v, 1, s, stdout);
        }
    } else if (strcasecmp(cmd, "set") == 0) {
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
        rc = shardcache_client_set(client, argv[2], strlen(argv[2]), in, s, argc > 3 ? strtol(argv[3], NULL, 10) : 0);
    } else if (strcasecmp(cmd, "del") == 0) {
        rc = shardcache_client_del(client, argv[2], strlen(argv[2]));
    } else if (strcasecmp(cmd, "evict") == 0) {
        rc = shardcache_client_evict(client, argv[2], strlen(argv[2]));
    } else {
        usage(argv[0]);
    }

    if (rc != 0 || shardcache_client_errno(client) != SHARDCACHE_CLIENT_OK) {
        fprintf(stderr, "%s\n", shardcache_client_errstr(client));
    }

    shardcache_client_destroy(client);
    exit(rc);
}

