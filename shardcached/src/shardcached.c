#define _GNU_SOURCE
#include <getopt.h>
#include <string.h>

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <signal.h>
#include <errno.h>

#include "log.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <mongoose.h>

#include <pthread.h>
#include <regex.h>

#include <fbuf.h>
#include <shardcache.h>

#include "storage_mem.h"
#include "storage_fs.h"

#define SHARDCACHED_ADDRESS_DEFAULT "4321"
#define SHARDCACHED_LOGLEVEL_DEFAULT 0
#define SHARDCACHED_USERAGENT_SIZE_THRESHOLD 16
#define SHARDCACHED_MAX_SHARDS 1024

#define ATOMIC_READ(__p) __sync_fetch_and_add(&__p, 0)
#define ATOMIC_CMPXCHG(__p, __v1, __v2) __sync_bool_compare_and_swap(&__p, __v1, __v2)

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"

#define MAX_STORAGE_OPTIONS 256

static char *me = NULL;
static char *basepath = NULL;
static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    void *value;
    size_t size;
} shardcached_stored_item;

static void usage(char *progname, char *msg, ...)
{
    if (msg) {
        va_list arg;
        va_start(arg, msg);
        vprintf(msg, arg);
        printf("\n");
    }

    printf("Usage: %s [OPTION]...\n"
           "Possible options:\n"
           "    -f                    run in foreground\n"
           "    -d <level>            debug level\n"
           "    -l <ip_address:port>  ip address:port where to listen for incoming http connections\n"
           "    -b                    HTTP url basepath\n"
           "    -p <peers>            list of peers participating in the shardcache in the form : 'address:port,address2:port2'\n"
           "    -s                    shared secret used for message signing\n"
           "    -t <type>             storage type (available are : 'mem' and 'fs' (defaults to 'mem')\n"
           "    -o <options>          storage options\n"
           "       Storage Types:\n"
           "         * mem       memory based storage\n"
           "            Options:\n"
           "              - initial_table_size     the initial size of the internal hashtable\n"
           "              - max_table_size         maximum limit the internal hashtable can be grown up to\n"
           "\n"
           "         * fs        filesystem based storage\n"
           "            Options:\n"
           "              - storage_path           the parh where to store the keys/values on the filesystem\n"
           "              - tmp_path               the path to a temporary directory to use while new data is being uploaded\n"
           , progname);

    exit(-2);
}

static void shardcached_stop(int sig)
{
    pthread_mutex_lock(&exit_lock);
    pthread_cond_signal(&exit_cond);
    pthread_mutex_unlock(&exit_lock);
}

static void shardcached_do_nothing(int sig)
{
    DEBUG1("Signal %d received ... doing nothing\n", sig);
}

static int shardcached_request_handler(struct mg_connection *conn) {

    struct mg_request_info *request_info = mg_get_request_info(conn);
    shardcache_t *cache = request_info->user_data;
    char *key = (char *)request_info->uri;

    if (basepath) {
        if (strncmp(key, basepath, strlen(basepath)) != 0) {
            ERROR("Bad request uri : %s", request_info->uri);
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
            return 1;
        }
        key += strlen(basepath);
    }
    while (*key == '/')
        key++;
    if (*key == 0) {
        mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
        return 1;
    }

    if (strncasecmp(request_info->request_method, "GET", 3) == 0) {
        size_t vlen = 0;
        void *value = shardcache_get(cache, key, strlen(key), &vlen);
        if (value) {
            mg_printf(conn, "HTTP/1.0 200 OK\r\n"
                            "Content-Type: application/octet-stream\r\n"
                            "Content-length: %d\r\n"
                            "Server: shardcached\r\n"
                            "Connection: Close\r\n\r\n", (int)vlen);
            mg_write(conn, value, vlen);
            free(value);
        } else {
            mg_printf(conn, "HTTP/1.0 404 Not Found\r\n\r\nNot Found");
        }
    } else if (strncasecmp(request_info->request_method, "DELETE", 6) == 0) {
        int rc = shardcache_del(cache, key, strlen(key));
        mg_printf(conn, "HTTP/1.0 %s\r\n"
                        "Content-Length: 0\r\n\r\n",
                         rc == 0 ? "200 OK" : "500 ERR");
    } else if (strncasecmp(request_info->request_method, "PUT", 3) == 0) {
        int clen = 0;
        const char *clen_hdr = mg_get_header(conn, "Content-Length");
        if (clen_hdr) {
            clen = strtol(clen_hdr + 15, NULL, 10); 
        }
        
        if (!clen) {
            mg_printf(conn, "HTTP/1.0 400 Bad Request\r\n\r\nNo Content-Length");
            return 1;
        }

        char *in = malloc(clen);
        int rb = 0;
        do {
            int n = mg_read(conn, in+rb, clen-rb);
            if (n == 0) {
                // connection closed by peer
                break;
            } else if (n < 0) {
                // error
                break;
            } else {
                rb += n;
            }
        } while (rb != clen);
        

        shardcache_set(cache, key, strlen(key), in, rb);

        mg_printf(conn, "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n");
    }
    return 1;
}

void shardcached_end_request_handler(const struct mg_connection *conn, int reply_status_code) {
}

typedef void (*shardcache_storage_destroyer_t)(shardcache_storage_t *);

shardcache_storage_destroyer_t
shardcached_init_storage(char *storage_type, char *options_string, shardcache_storage_t **storage)
{
    const char *storage_options[MAX_STORAGE_OPTIONS];
    shardcache_storage_destroyer_t storage_destroy = NULL;

    int optidx = 0;
    char *p = options_string;
    char *str = p;
    while (*p != 0 && optidx < MAX_STORAGE_OPTIONS) {
        if (*p == '=' || *p == ',') {
            *p = 0;
            storage_options[optidx++] = str;
            str = p+1;
        }
        p++;
    }
    storage_options[optidx++] = str;
    storage_options[optidx] = NULL;
    // initialize the storage layer 
    if (strcmp(storage_type, "mem") == 0) {
        // TODO - validate options
        *storage = storage_mem_create(storage_options);
        storage_destroy = storage_mem_destroy;

    } else if (strcmp(storage_type, "fs") == 0) {
        // TODO - validate options
        *storage = storage_fs_create(storage_options);
        storage_destroy = storage_fs_destroy;
    } else {
        usage("Unknown storage type: %s\n", storage_type);
    }
    return storage_destroy;
}

int main(int argc, char **argv)
{

    int option_index = 0;
    int foreground = 0;
    int loglevel = SHARDCACHED_LOGLEVEL_DEFAULT;
    char *listen_address = SHARDCACHED_ADDRESS_DEFAULT;
    char *peers = NULL;
    char *secret = "default";
    char *storage_type = "mem";
    char options_string[2048];
    
    strcpy(options_string, "initial_table_size=1024,max_table_size=1000000");

    static struct option long_options[] = {
        {"base", 2, 0, 'b'},
        {"debug", 2, 0, 'd'},
        {"foreground", 0, 0, 'f'},
        {"listen", 2, 0, 'l'},
        {"peers", 2, 0, 'p'},
        {"secret", 2, 0, 's'},
        {"type", 2, 0, 't'},
        {"options", 2, 0, 'o'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "b:d:fhl:p:s:t:o:?", long_options, &option_index))) {
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'b':
                basepath = optarg;
                // skip leading '/'s
                while (*basepath == '/')
                    basepath++;
                break;
            case 'd':
                loglevel = optarg ? atoi(optarg) : 1;
                break;
            case 'f':
                foreground = 1;
                break;
            case 'l':
                listen_address = optarg;
                break;
            case 'p':
                peers = optarg;
                break;
            case 's':
                secret = optarg;
                break;
            case 't':
                storage_type = optarg;
                break;
            case 'o':
                snprintf(options_string, sizeof(options_string), "%s", optarg);
                break;
            case 'h':
            case '?':
                usage(argv[0], NULL);
                break;
            default:
                break;
        }
    }
    me = argv[argc-1];

    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        fprintf(stderr, "Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        exit(-1);
    }

    if (!me || *me == '-') {
        usage(argv[0], "The local address is mandatory");
    }

    int matched = regexec(&addr_regexp, me, 0, NULL, 0);
    if (matched != 0) {
        usage(argv[0], "Bad address format: '%s'", me);
    }

    char *shard_names[SHARDCACHED_MAX_SHARDS];

    char *tok = strtok(peers, ",");
    int cnt = 0;
    while(tok) {
        matched = regexec(&addr_regexp, tok, 0, NULL, 0);
        if (matched != 0) {
            usage(argv[0], "Bad address format for peer: '%s'", tok);
        }
        shard_names[cnt] = tok;
        cnt++;
        tok = strtok(NULL, ",");
    } 

    regfree(&addr_regexp);

    // go daemon if we have to
    if (!foreground) {
        int rc = daemon(0, 0);
        if (rc != 0) {
            fprintf(stderr, "Can't go daemon: %s\n", strerror(errno));
            exit(-1);
        }
    }

    signal(SIGINT, shardcached_stop);
    signal(SIGHUP, shardcached_stop);
    signal(SIGQUIT, shardcached_stop);
    signal(SIGPIPE, shardcached_do_nothing);

    log_init("shardcached", loglevel);

    shardcache_storage_t *storage = NULL;
    shardcache_storage_destroyer_t storage_destroy = shardcached_init_storage(storage_type, options_string, &storage);
    if (!storage) {
        ERROR("Can't initialize the storage subsystem");
        exit(-1);
    }

    shardcache_t *cache = shardcache_create(me, shard_names, cnt, storage, secret, 5);
    if (!cache) {
        ERROR("Can't initialize the shardcache engine");
        exit(-1);
    }

    // initialize the mongoose callbacks descriptor
    struct mg_callbacks shardcached_callbacks = {
        .begin_request = shardcached_request_handler,
        .end_request = shardcached_end_request_handler,
    };

    if (strncmp(listen_address, "*:", 2) == 0)
        listen_address += 2;

    const char *mongoose_options[] = { "listening_ports", listen_address, NULL };
    // let's start mongoose
    struct mg_context *ctx = mg_start(&shardcached_callbacks, cache, mongoose_options);
    if (ctx) {
        // and keep working until we are told to exit
        pthread_mutex_lock(&exit_lock);
        pthread_cond_wait(&exit_cond, &exit_lock);
        pthread_mutex_unlock(&exit_lock);
        mg_stop(ctx);  
    } else {
        ERROR("Can't start the http subsystem");
    }
    
    NOTICE("exiting");

    shardcache_destroy(cache);
    if (storage_destroy)
        storage_destroy(storage);
    
    exit(0);
}
