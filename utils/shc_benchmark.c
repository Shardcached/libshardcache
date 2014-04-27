#define _GNU_SOURCE
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <regex.h>
#include <pthread.h>
#include <iomux.h>
#include <fbuf.h>

#include <shardcache_client.h>
#include <counters.h>
#include <messaging.h>

static int quit = 0;
static shardcache_node_t **hosts = NULL;
static int num_hosts = 0;
static int num_clients = 1;
static int num_threads = 1;
static int use_index = 0;
static int print_stats = 0;
static shardcache_storage_index_t *keys_index = NULL;
static int num_keys = 1000;
static char *prefix = "shc_bench";
static char *hosts_string = NULL;
static int verbose = 0;
static int wrate = 0;
static char *secret = NULL;
static uint32_t num_gets = 0;
static uint32_t num_sets = 0;
static uint32_t num_responses = 0;
shardcache_counters_t *counters = NULL;

typedef struct {
    fbuf_t *output;
    async_read_ctx_t *reader; 
    uint32_t num_requests;
    uint32_t num_responses;
} client_ctx;

static void usage(char *progname, int rc, char *msg, ...)
{
    if (msg) {
        va_list arg;
        va_start(arg, msg);
        vprintf(msg, arg);
        printf("\n");
    }

    printf("Usage: %s [OPTION]...\n"
           "    -c <num_clients>  The number of clients per thread (defaults to: %d)\n"
           "    -t <num_threads>  The number of threads (defaults to: %d)\n"
           "    -h                Print this message and exit\n"
           "    -H <hosts_string> A shardcache hosts string (defaults to: $SHC_HOSTS)\n"
           "    -i                Use the index instead of generating test keys\n"
           "    -k <num_keys>     The number of keys to use during the test (defaults to: %d)\n"
           "    -p <prefix>       A custom prefix to use for generated keys (defaults to: %s)\n"
           "    -P                Print stats to stdout every second\n"
           "    -v                Be verbose\n"
           , progname
           , num_clients
           , num_threads
           , num_keys
           , prefix);
    exit(rc);
}

static void stop(int sig)
{
    __sync_fetch_and_add(&quit, 1);
}


void close_connection(iomux_t *iomux, int fd, void *priv)
{
    client_ctx *ctx = (client_ctx *)priv;
    char label[256];
    snprintf(label, sizeof(label), "[client %p] requests", ctx);
    shardcache_counter_remove(counters, label);
    snprintf(label, sizeof(label), "[client %p] responses", ctx);
    shardcache_counter_remove(counters, label);
    fprintf(stderr, "Client %p closed\n", ctx);
    free(ctx);
    close(fd);
}

int discard_response(iomux_t *iomux, int fd, unsigned char *data, int len, void *priv)
{
    client_ctx *ctx = (client_ctx *)priv;
    int processed = 0;
    
    //printf("received %d\n", len);
    async_read_context_state_t state = async_read_context_input_data(ctx->reader, data, len, &processed);
    while (state == SHC_STATE_READING_DONE) {
        __sync_add_and_fetch(&num_responses, 1);
        __sync_add_and_fetch(&ctx->num_responses, 1);
        state = async_read_context_update(ctx->reader);
    }
    if (state == SHC_STATE_READING_ERR) {
        fprintf(stderr, "Async context returned error\n");
    }
    return len;
}

void send_command(iomux_t *iomux, int fd, unsigned char *data, int *len, void *priv)
{
    client_ctx *ctx = (client_ctx *)priv;
    fbuf_t *output_buffer = ctx->output;


    // don't pipeline more than 1024 requests ahead
    if (__sync_fetch_and_add(&ctx->num_requests, 0) - __sync_fetch_and_add(&ctx->num_responses, 0) < 1024)
    {
        int idx = rand() % num_keys;
        while (use_index && keys_index->items[idx].vlen == 0)
            idx = rand() % num_keys;

        shardcache_record_t record[2] = {
            {
                .v = keys_index->items[idx].key,
                .l = keys_index->items[idx].klen
            },
            {
                .v = NULL,
                .l = 0
            }
        };
        int num_records = 1;
        unsigned char hdr = SHC_HDR_GET;
        unsigned char sig_hdr = secret ? SHC_HDR_SIGNATURE_SIP : 0;

        if (wrate && rand()%100 > wrate) {
            char value[256];
            snprintf(value, sizeof(value), "TEST%d", (int)time(NULL));
            record[1].v = value;
            record[1].l = strlen(value);
            num_records = 2;
            hdr = SHC_HDR_SET;
        }

        if (build_message(secret, sig_hdr, hdr, record, num_records, output_buffer) != 0)
        {
            fprintf(stderr, "Can't create new command!\n");
        }
        if (hdr == SHC_HDR_GET)
            __sync_add_and_fetch(&num_gets, 1);
        else
            __sync_add_and_fetch(&num_sets, 1);
        __sync_fetch_and_add(&ctx->num_requests, 1);
    }
    if (fbuf_used(output_buffer)) {
        if (*len > fbuf_used(output_buffer)) {
            *len = fbuf_used(output_buffer);
            memcpy(data, fbuf_data(output_buffer), *len);
            fbuf_clear(output_buffer);
        } else {
            memcpy(data, fbuf_data(output_buffer), *len);
            fbuf_remove(output_buffer, *len);
        }
    } else {
        *len = 0;
    }
    //printf("sent %d\n", *len);
}

static void *worker(void *priv)
{
    iomux_t *iomux = (iomux_t *)priv;

#if 0
    shardcache_client_t *c = shardcache_client_create(hosts, num_hosts, secret);
    if (!c) {
        // TODO - Error message
        return NULL;
    }
    while(!__sync_add_and_fetch(&quit, 0)) {
        int idx = rand() % num_keys;
        if (wrate && rand()%100 > wrate) {
            char value[256];
            snprintf(value, sizeof(value), "TEST%d", (int)time(NULL));
            shardcache_client_set(c, keys_index->items[idx].key, keys_index->items[idx].klen, value, strlen(value), 0);
        } else {
            shardcache_client_get(c, keys_index->items[idx].key, keys_index->items[idx].klen, NULL);
        }
    }
#else
    int i,n;
    for (i = 0; i < num_hosts; i++) {
        char *addr = shardcache_node_get_address(hosts[i]);
        for (n = 0; n < num_clients; n++) {
            int fd = connect_to_peer(addr, 5000);
            if (fd < 0) {
                fprintf(stderr, "Can't connect to %s: %s\n", addr, strerror(errno));
                exit(-99);
            }

            client_ctx *ctx = calloc(1, sizeof(client_ctx));
            ctx->reader = async_read_context_create(secret, NULL, NULL);
            ctx->output = fbuf_create(0);
            iomux_callbacks_t cbs = {
                .mux_output = send_command,
                .mux_timeout = NULL,
                .mux_input = discard_response,
                .mux_eof = close_connection,
                .priv = ctx
            };

            char label[256];
            snprintf(label, sizeof(label), "[client %p] requests", ctx);
            shardcache_counter_add(counters, label, &ctx->num_requests);
            snprintf(label, sizeof(label), "[client %p] responses", ctx);
            shardcache_counter_add(counters, label, &ctx->num_responses);
            iomux_add(iomux, fd, &cbs);
        }
    }

    while(!__sync_add_and_fetch(&quit, 0)) {
        struct timeval tv = { 1, 0 };
        iomux_run(iomux, &tv);
    }

#endif
    return NULL;
}

#define ADDR_REGEXP "^([a-z0-9_\\.\\-]+|\\*)(:[0-9]+)?$"

static int check_address_string(char *str)
{
    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        SHC_ERROR("Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        return -1;
    }

    int matched = regexec(&addr_regexp, str, 0, NULL, 0);
    regfree(&addr_regexp);

    if (matched != 0) {
        return -1;
    }

    return 0;
}

static int parse_hosts_string(char *str)
{
    char *copy = strdup(str);
    char *s = copy;

    while (s && *s) {
        char *tok = strsep(&s, ",");
        if(tok) {
            char *label = strsep(&tok, ":");
            char *addr = tok;
            if (!addr || check_address_string(addr) != 0) {
                SHC_ERROR("Bad address format for peer: '%s'", addr);
                free(copy);
                if (hosts)
                    shardcache_free_nodes(hosts, num_hosts);
                return -1;
            }
            num_hosts++;
            hosts = realloc(hosts, num_hosts * sizeof(shardcache_node_t *));
            hosts[num_hosts - 1] = shardcache_node_create((char *)label, (char **)&addr, 1);
        } 
    }
    free(copy);
    return num_hosts;
}


int
main (int argc, char **argv)
{
    static struct option long_options[] = {
        { "clients", 2, 0, 'c' },
        { "threads", 2, 0, 't' },
        { "help", 0, 0, 'h' },
        { "hosts", 2, 0, 'H' },
        { "index", 0, 0, 'i' },
        { "keys", 2, 0, 'k' },
        { "prefix", 2, 0, 'p' },
        { "print_stats", 2, 0, 'P' },
        { "verbose", 0, 0, 'v' },
    };
    hosts_string = getenv("SHC_HOSTS");
    int option_index = 0;
    char c;
    while ((c = getopt_long(argc, argv, "c:hH:ik:p:Pt:v", long_options, &option_index))) {
        if (c == -1)
            break;
        switch(c) {
            case 'c':
                num_clients = strtol(optarg, NULL, 10);
                break;
            case 'h':
                usage(argv[0], 0, NULL);
                break;
            case 'H':
                hosts_string = optarg;
            case 'i':
                use_index = 1;
                break;
            case 'k':
                num_keys = strtol(optarg, NULL, 10);
                break;
            case 'p':
                prefix = optarg;
                break;
            case 'P':
                print_stats = 1;
                break;
            case 't':
                num_threads = strtol(optarg, NULL, 10);
                break;
            case 'v':
                verbose++;
                break;
            default:
                break;
        }
    }

    if (!hosts_string || !*hosts_string)
        usage(argv[0], -1, "No hosts string provided!");

    if (parse_hosts_string(hosts_string) <= 0)
        usage(argv[0], -1, "Can't parse the provided hosts string");

    shardcache_client_t *client = shardcache_client_create(hosts, num_hosts, secret);
    if (!client) {
        fprintf(stderr, "Can't create the shardcache client");
        exit(-1);
    }

    if (use_index) {
        printf("Fetching index ... ");
        keys_index = shardcache_client_index(client, shardcache_node_get_label(hosts[0]));
        printf("done!\nStarting clients ... ");
    } else {
        int n;
        keys_index = calloc(1, sizeof(shardcache_storage_index_t));
        for (n = 0; n < num_keys; n++) {
            int maxklen = strlen(prefix) + 32;
            keys_index->items = realloc(keys_index->items, keys_index->size + 1);
            shardcache_storage_index_item_t *item = &keys_index->items[keys_index->size++];
            item->key = malloc(maxklen);
            snprintf(item->key, maxklen, "%s%d", prefix, n);
            item->klen = strlen(item->key);
            item->vlen = 4;
            printf("Setting key %s\n", (char *)item->key);
            if (shardcache_client_set(client, item->key, item->klen, "TEST", 4, 0) != 0) {
                // TODO - Error message
                exit(-1);
            }
        }
    }
    shardcache_client_destroy(client);
    signal (SIGINT, stop);

    srand(time(NULL));

    counters = shardcache_init_counters();

    int i;
    pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
    iomux_t **muxes = malloc(sizeof(iomux_t *) * num_threads);
    for (i = 0; i < num_threads; i++) {
        muxes[i] = iomux_create(0, 0);
        if (pthread_create(&threads[i], NULL, worker, muxes[i]) != 0) {
            fprintf(stderr, "Can't spawn thread: %s\n", strerror(errno));
            exit(-1);            
        }
    }
    printf("Done\n");
    sleep(1);
    shardcache_counter_t *prev_counts = NULL;
    int num_prev_counters = 0;
    uint32_t num_responses_prev = 0;
    while (!__sync_fetch_and_add(&quit, 0)) {
        shardcache_counter_t *counts = NULL;
        int num_counters = shardcache_get_all_counters(counters, &counts);
        if (!num_counters)
            continue;
        uint32_t fastest_client = 0;
        uint32_t slowest_client = 0;
        uint32_t stuck_clients = 0;
        for (i = 0; i < num_counters; i++) {
            if (strstr(counts[i].name, "responses")) {
                uint32_t diff = (prev_counts && strcmp(counts[i].name, prev_counts[i].name) == 0)
                              ? counts[i].value - prev_counts[i].value
                              : 0;
                if (!slowest_client || slowest_client > diff)
                    slowest_client = diff;

                if (!fastest_client || fastest_client < diff)
                    fastest_client = diff;

                if (diff == 0)
                    stuck_clients++;
            }
/*            if (print_stats)
                printf("%s: %u\n", counts[i].name, counts[i].value);
*/
        }
        uint32_t num_responses_cur = __sync_add_and_fetch(&num_responses, 0);
        uint32_t responses_sum = num_responses_prev ? num_responses_cur - num_responses_prev : num_responses_cur;
        uint32_t avg_responses = responses_sum / (num_threads * num_clients);
        if (print_stats) {
            
            printf("\033[H\033[J"
                   "gets: %u\n"
                   "sets: %u\n"
                   "num_responses: %u\n"
                   "total_responses/s: %u\n"
                   "avg_responses/s: %u\n"
                   "slowest: %u\n"
                   "fastest: %u\n"
                   "stuck_clients: %u\n",
                   num_gets, num_sets, num_responses, responses_sum, avg_responses, slowest_client, fastest_client, stuck_clients);
        }

        // TODO - dump stats to a file
        if (prev_counts)
            free(prev_counts);
        prev_counts = counts;
        num_prev_counters = num_counters;
        num_responses_prev = __sync_add_and_fetch(&num_responses, 0);

        sleep(1);
    }
    if (prev_counts)
        free(prev_counts);

    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        fprintf(stderr, "Thread %d done\n", i);
    }

    shardcache_free_nodes(hosts, num_hosts);
    shardcache_free_index(keys_index);
    shardcache_release_counters(counters);

    exit (0);
}
