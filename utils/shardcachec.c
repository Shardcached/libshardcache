#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include <chash.h>
#include <shardcache_client.h>
#include <pthread.h>

void usage(char *prgname) {
    printf("Usage: %s <Command> <Key>\n"
           "   Commands: \n"
           "        get       <key> [ <output_file> (defaults to stdout) ]\n"
           "        get_async <key> [ <output_file> (defaults to stdout) ]\n"
           "        get_multi <key> [ <key> ... ] [ -o <output_dir>   (defaults to stdout) ]\n"
           "        offset    <key> <offset> <kength> [ <output_file> (defaults to stdout)\n"
           "        set       <key> [ -e <expire> ] [ -i <input_file> (defaults to stdin) ]\n"
           "        add       <key> [ -e <expire> ] [ -i <input_file> (defaults to stdin) ]\n"
           "        exists    <key>\n"
           "        touch     <key>\n"
           "        del       <key>\n"
           "        evict     <key>\n"
           "        index   [ <node> ]\n"
           "        stats   [ <node> ]\n"
           "        check   [ <node> ]\n\n", prgname);
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
                 int error,
                 void *priv)
{
    FILE *out = (FILE *)priv;
    if (data && len)
        fwrite(data, 1, len, out ? out : stdout);
    return error;
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

    FILE *output_file = NULL;

    char *cmd = argv[1];
    if (strcasecmp(cmd, "get") == 0) {
        void *out = NULL;
        size_t len = shardcache_client_get(client, argv[2], strlen(argv[2]), &out); 
        if (len && out) {
            if (argc > 3) {
                output_file = fopen(argv[3], "w");
                if (!output_file) {
                    fprintf(stderr, "Can't open file %s for writing : %s\n",
                            argv[3], strerror(errno));
                    exit(-1);
                }
            }
            print_chunk(NULL, NULL, 0, out, len, 0, output_file);
        }
    } else if (strcasecmp(cmd, "offset") == 0) {
        if (argc < 5)
            usage(argv[0]);
        int offset = strtol(argv[3], NULL, 10);
        int size = strtol(argv[4], NULL, 10);
        char out[size];
        size_t len = shardcache_client_offset(client, argv[2], strlen(argv[2]), offset, out, size); 

        if (argc > 5) {
            output_file = fopen(argv[5], "w");
            if (!output_file) {
                fprintf(stderr, "Can't open file %s for writing : %s\n",
                        argv[5], strerror(errno));
                exit(-1);
            }
        }

        if (len) {
            print_chunk(NULL, NULL, 0, out, len, 0, output_file);
        }
    } else if (strcasecmp(cmd, "geta") == 0 || strcasecmp(cmd, "get_async") == 0) {
        if (argc > 3) {
            output_file = fopen(argv[3], "w");
            if (!output_file) {
                fprintf(stderr, "Can't open file %s for writing : %s\n",
                        argv[3], strerror(errno));
                exit(-1);
            }
        }
        rc = shardcache_client_get_async(client, argv[2], strlen(argv[2]), print_chunk, output_file); 
    } else if (strcasecmp(cmd, "get_multi") == 0) {
        char **keys = &argv[2];
        int num_keys = argc - 2;

        char *outdir = NULL;
        if (argc > 3) {
            if (strncmp(argv[argc-1], "-o", 2) == 0) {
                char *dir = argv[argc-1] + 2;
                if (*dir) {
                    outdir = dir;
                    num_keys--;
                }
            } else if (strncmp(argv[argc-2], "-o", 2) == 0) {
                outdir = argv[argc-1];
                num_keys -= 2;
            }
            if (outdir) {
                int olen = strlen(outdir) -1;
                while (olen >= 0 && outdir[olen] == '/')
                    outdir[olen--] = 0;
            }
        }

        shc_multi_item_t *items[num_keys+1];
        int i;
        for (i = 0; i < num_keys; i++) {
            items[i] = shc_multi_item_create(client, keys[i], strlen(keys[i]), NULL, 0);
        }
        items[num_keys] = NULL;

        rc = shardcache_client_get_multi(client, items);

        for (i = 0; i < num_keys; i++) {
            size_t klen = items[i]->klen;
            char *keystr = malloc(klen+1);
            memcpy(keystr, items[i]->key, klen);
            keystr[klen] = 0;
            FILE *out = NULL;
            char *outname = NULL;
            if (outdir) {
                int len = strlen(outdir) + klen + 2;
                outname = malloc(len);
                snprintf(outname, len, "%s/%s", outdir, keystr);
                out = fopen(outname, "w");
                if (!out) {
                    fprintf(stderr, "Can't open file %s for writing : %s\n",
                            outname, strerror(errno));
                    exit(-1);
                }
            }
            if (out)
                printf("Saving key %s to file %s\n", keystr, outname);
            else
                printf("Value for key: %s\n", keystr);

            print_chunk(NULL, NULL, 0, items[i]->data, items[i]->dlen, 0, out);
            printf("\n");
            shc_multi_item_destroy(items[i]);
            if (out)
                fclose(out);
            if (outname)
                free(outname);
        }
    } else if (strcasecmp(cmd, "set") == 0 ||
               strcasecmp(cmd, "add") == 0)
    {
        FILE *infile = NULL;
        uint32_t expire = 0;
        char *inpath;
        while (argc > 3) {
            if (strncmp(argv[argc-1], "-i", 2) == 0) {
                char *dir = argv[argc-1] + 2;
                if (*dir) {
                    inpath = dir;
                    argc--;
                }
            } else if (strncmp(argv[argc-1], "-e", 2) == 0) {
                char *expire_str = argv[argc-1] + 2;
                if (*expire_str) {
                    expire = strtol(expire_str, NULL, 10);
                    argc--;
                }
            } else if (strncmp(argv[argc-2], "-i", 2) == 0) {
                inpath = argv[argc-1];
                argc -= 2;
            } else if (strncmp(argv[argc-2], "-e", 2) == 0) {
                expire = strtol(argv[argc-1], NULL, 10);
                argc -= 2;
            } else {
                fprintf(stderr, "Unknown option %s\n", argv[3]);
                exit(-1);
            }
        }

        if (inpath) {
            infile = fopen(inpath, "r");
            if (!infile) {
                fprintf(stderr, "Can't open file for %s reading: %s\n",
                        inpath, strerror(errno));
                exit(-1);
            }
        }
        char *in = NULL;
        size_t s = 0;
        char buf[1024];
        int rb = fread(buf, 1, 1024, stdin);
        while (rb > 0) {
            in = realloc(in, s+rb);
            memcpy(in + s, buf, rb);
            s += rb;
            rb = fread(buf, 1, 1024, infile ? infile : stdin);
        }

        if (strcasecmp(cmd, "set") == 0) {
            rc = shardcache_client_set(client, argv[2], strlen(argv[2]), in, s, expire);
        } else {
            rc = shardcache_client_add(client, argv[2], strlen(argv[2]), in, s, expire);
            if (rc == 1)
                printf("Already exists!\n");
        }
        if (infile)
            fclose(infile);
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

    if (output_file)
        fclose(output_file);

    if (strncasecmp(cmd, "get", 3) != 0 &&
        strncasecmp(cmd, "offset", 6) != 0)
    {
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
    }

    if (shardcache_client_errno(client) != SHARDCACHE_CLIENT_OK) {
        fprintf(stderr, "errorno: %d, errstr: %s\n",
                shardcache_client_errno(client), shardcache_client_errstr(client));
    }

    shardcache_client_destroy(client);
    exit(rc);
}

