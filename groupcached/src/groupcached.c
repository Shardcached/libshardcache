#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <stdint.h>
#include <signal.h>
#include <errno.h>

#include "iomux.h"
#include "log.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <pthread.h>
#include <regex.h>

#include <fbuf.h>
#include <hashtable.h>
#include "groupcache.h"

#define _GNU_SOURCE
#include <getopt.h>

#define GROUPCACHED_DBFILE_DEFAULT "/usr/share/groupcache/groupcache.xml"
#define GROUPCACHED_PORT_DEFAULT 4321
#define GROUPCACHED_ADDRESS_DEFAULT "*"
#define GROUPCACHED_LOGLEVEL_DEFAULT 0
#define GROUPCACHED_USERAGENT_SIZE_THRESHOLD 16
#define GROUPCACHED_MAX_SHARDS 1024

#define ATOMIC_READ(__p) __sync_fetch_and_add(&__p, 0)
#define ATOMIC_CMPXCHG(__p, __v1, __v2) __sync_bool_compare_and_swap(&__p, __v1, __v2)

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"

static char *me = NULL;
static iomux_t *iomux = NULL;
static int use_http = 1;
static int single_thread = 0;
static hashtable_t *storage = NULL;
static groupcache_t *cache = NULL;

typedef struct {
    fbuf_t *input;
    fbuf_t *output;
    char *key;
    int fd;
    int is_http10;
    iomux_callbacks_t callbacks;
} groupcached_connection_context;

typedef struct {
    void *value;
    size_t size;
} groupcached_stored_item;

static char *unescape_uri_request(char *uri)
{
    fbuf_t buf = FBUF_STATIC_INITIALIZER;
    char *p = uri;
    while (*p != 0) {
        char *n = p;
        while (*n != '%' && *n != 0)
            n++;
        fbuf_add_binary(&buf, p, n-p);
        p = n;
        if (*n != 0) {
            // p and n now both point to %
            p+=3;
            n++;
            int c;
            if (sscanf(n, "%02x", &c) == 1)
                fbuf_add_binary(&buf, (char *)&c, 1);
            else
                WARN("Can't unescape uri byte");
        }
    }
    char *data = fbuf_data(&buf);
    return data;
}

static void groupcached_connection_handler(iomux_t *iomux, int fd, void *priv)
{
    iomux_callbacks_t *groupcached_callbacks = (iomux_callbacks_t *)priv;

    // create and initialize the context for the new connection
    groupcached_connection_context *context = calloc(1, sizeof(groupcached_connection_context));

    memcpy(&context->callbacks, groupcached_callbacks, sizeof(iomux_callbacks_t));

    context->input = fbuf_create(0);
    context->output = fbuf_create(0);
    context->callbacks.priv = context;

    // and wait for input data
    iomux_add(iomux, fd, &context->callbacks);
}

static int write_socket(int fd, char *buf, int len)
{
    int wb = 0;
    int ofx = 0;
    do {
        len -= wb;
        ofx += wb;
        wb =  write(fd, buf+ofx, len);
        if (wb == -1) {
            if (errno != EINTR || errno != EAGAIN) {
                NOTICE("write on fd %d failed: %s", fd, strerror(errno));
                return -1;
            }
            wb = 0;
        } else if (wb == 0) {
            break;
        }
    } while (wb != len);
    return wb;
}

static void send_response(groupcached_connection_context *ctx)
{
    const char *key = (const char *)ctx->key;
    DEBUG1("Worker %p is looking up useragent : %s", pthread_self(), key);

    // this might be unnecessary if libgroupcache is thread-safe
    // XXX - needs to be checked
    //groupcached_get_capabilities(useragent, ctx->output);
    size_t vlen = 0;
    void *value = groupcache_get(cache, ctx->key, strlen(ctx->key), &vlen);
    if (value)
        fbuf_add_binary(ctx->output, value, vlen);
    if (use_http) {
        char response_header[1024];
        sprintf(response_header, "%s 200 OK\r\n"
                "Content-Type: application/octet-stream\r\n"
                "Content-length: %d\r\n"
                "Server: groupcached\r\n"
                "Connection: Close\r\n\r\n",
                ctx->is_http10 ? "HTTP/1.0" : "HTTP/1.1", fbuf_used(ctx->output));

        int err = write_socket(ctx->fd, response_header, strlen(response_header));
        if (err != 0) {
            ERROR("(%p) Can't write the response header : %s", pthread_self(), strerror(errno));
        }
    }

    if (write_socket(ctx->fd, fbuf_data(ctx->output), fbuf_used(ctx->output)) != 0) {
        ERROR("(%p) Can't write the response data : %s", pthread_self(), strerror(errno));
    }
}

static char *extract_key(char *url)
{
    char *key = NULL;
    if (*url == '/')
        url++;
    char *reqline_start = url;
    char *reqline_end = reqline_start;

    while (*reqline_end != '\r' && *reqline_end != '\n')
        reqline_end++;
    reqline_end++;

    char reqline[reqline_end-reqline_start];
    snprintf(reqline, reqline_end-reqline_start, "%s", reqline_start);

    key = unescape_uri_request(reqline);
    return key;
}

void *worker(void *priv)
{
    char *key = NULL;

    groupcached_connection_context *ctx = (groupcached_connection_context *)priv;

    DEBUG1("Worker %p started on fd %d", pthread_self(), ctx->fd);

    /*
    // we don't need to receive anything anymore on this fd
    int err = shutdown(ctx->fd, SHUT_RD);
    if (err != 0)
        NOTICE("Can't shutdown the receive part of fd %d : %s", ctx->fd, strerror(errno));
    */
    int opts = fcntl(ctx->fd, F_GETFL);
    if (opts >= 0) {
        int err = fcntl(ctx->fd, F_SETFL, opts & (~O_NONBLOCK));
        if (err != 0)
            NOTICE("Can't set blocking mode on fd %d : %s", ctx->fd, strerror(errno));
    } else {
        ERROR("Can't get flags on fd %d : %s", ctx->fd, strerror(errno));
    }

    fbuf_trim(ctx->input);

    // parse the request 
    char *request_data = fbuf_data(ctx->input);
    struct sockaddr_in peer;
    socklen_t socklen = sizeof(struct sockaddr);
    getpeername(ctx->fd, (struct sockaddr *)&peer, &socklen);
    if (use_http) {
        char *method = strtok(request_data, " ");
        if (!method) {
            goto __end_worker;
        }
        char *url = strtok(NULL, " ");
        key = extract_key(url);
        if (key) {
            NOTICE("(%p) Lookup request from %s: %s", pthread_self(), inet_ntoa(peer.sin_addr), key);
            ctx->key = key;
        }
        char *httpv = strtok(NULL, "\r\n");
        if (httpv) {
            ctx->is_http10 = (strncmp(httpv, "HTTP/1.0", 8) == 0);
        }

        request_data += (httpv - request_data) + strlen(httpv) + 1;
        if (strncasecmp(method, "GET", 3) == 0) {
            send_response(ctx);
        } else if (strncasecmp(method, "PUT", 3) == 0) {
            int clen = 0;
            char *clen_hdr = strcasestr(request_data, "Content-Length:");
            if (clen_hdr) {
                while(*clen_hdr == ' ')
                    clen_hdr++;
                clen = strtol(clen_hdr + 15, NULL, 10); 
            }

            int rb = 0;
            fbuf_t v = FBUF_STATIC_INITIALIZER;
            char *end = strstr(request_data, "\r\n\r\n");
            if (end) {
                end += 4;
            } else {
                end = strstr(request_data, "\n\n");
                if (end) {
                    end += 2;
                }
            }

            int buffered = end ? fbuf_end(ctx->input) - end : 0;
            if (buffered)
                fbuf_add_binary(&v, end, buffered);

            rb = fbuf_used(&v);
            while (rb != clen) {
                int len = clen > 0 ? (clen - rb): 1024;
                int n = fbuf_read(&v, ctx->fd, len);
                if (n == -1) {
                    // TODO - Error Messages
                    break;
                }
                rb += n;
            } 
            DEBUG2("SETTING KEY %s to VALUE %s\n", key, fbuf_data(&v));
            groupcached_stored_item *item = malloc(sizeof(groupcached_stored_item));
            item->value = fbuf_data(&v);
            item->size = fbuf_used(&v);
            ht_set(storage, key, strlen(key), item);

            char response[2048];

            snprintf(response, sizeof(response),
                     "%s 200 OK\r\n"
                     "Content-Length: 0\r\n\r\n",
                    ctx->is_http10 ? "HTTP/1.0" : "HTTP/1.1");

            // XXX
            if (write_socket(ctx->fd, response, strlen(response)) != 0) {
                ERROR("Worker %p failed writing reponse: %s", pthread_self(), strerror(errno));
            }
        }

    } else if (!use_http) {
        key = strdup(fbuf_data(ctx->input));
    }

    if (!key) {
        NOTICE("(%p) Unsupported Request from %s: %s", pthread_self(), inet_ntoa(peer.sin_addr), request_data);
        char response[2048];

        snprintf(response, sizeof(response),
                 "%s 400 NOT SUPPORTED\r\n"
                 "Content-Type: text/plain\r\n"
                 "Content-Length: 17\r\n\r\n"
                 "400 NOT SUPPORTED",
                 ctx->is_http10 ? "HTTP/1.0" : "HTTP/1.1");

        if (write_socket(ctx->fd, response, strlen(response)) != 0) {
            ERROR("Worker %p failed writing reponse: %s", pthread_self(), strerror(errno));
        }
    }
__end_worker:
    DEBUG1("Worker %p finished on fd %d", pthread_self(), ctx->fd);
    shutdown(ctx->fd, SHUT_RDWR);
    close(ctx->fd);
    fbuf_free(ctx->input);
    fbuf_free(ctx->output);
    free(ctx->key); 
    free(ctx);
    return NULL;
}

static void groupcached_input_handler(iomux_t *iomux, int fd, void *data, int len, void *priv)
{
    groupcached_connection_context *ctx = (groupcached_connection_context *)priv;
    if (!ctx)
        return;
    DEBUG1("New data on fd %d", fd);
    fbuf_add_binary(ctx->input, data, len);

    if (fbuf_used(ctx->input) < 4)
        return;

    // check if we have a complete requset
    char *current_data = fbuf_end(ctx->input) - (use_http ? 4 : 1);
    char *request_terminator = use_http ? strstr(current_data, "\r\n\r\n") : strstr(current_data, "\n");
    if (!request_terminator && use_http) { // support some broken clients/requests
        request_terminator = strstr(current_data, "\n\n");
    }
    if (request_terminator) {
        // we have a complete request so we can now start 
        // background worker to handle it
        pthread_t worker_thread;
        ctx->fd = fd;
        // let the worker take care of the fd from now on
        iomux_remove(iomux, fd);
        if (single_thread) {
            worker(ctx);
        } else {
            pthread_create(&worker_thread, NULL, worker, ctx);
            pthread_detach(worker_thread);
        }
    }
}

static void groupcached_eof_handler(iomux_t *iomux, int fd, void *priv)
{
    DEBUG1("Connection to %d closed", fd);
    close(fd);
}

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
           "    -l <ip_address>       ip address where to listen for incoming connections\n"
           "    -p <port>             tcp port where to listen for incoming connections\n"
           "    -w <groupcache_file>       path to the groupcache xml file\n"
           "    -s                    no multithreading, handle all requests in the main thread\n"
           "    -n                    no http, expects a raw useragent string on the connected\n"
           "                          socket, terminated by a newline\n", progname);
    exit(-2);
}

static void groupcache_init()
{
}

static void groupcached_reload(int sig)
{
    NOTICE("reloading database");
    groupcache_init();
}

static void groupcached_stop(int sig)
{
    iomux_end_loop(iomux);
}

static void groupcached_do_nothing(int sig)
{
    DEBUG1("Signal %d received ... doing nothing\n", sig);
}

static void *cache_fetch_item(void *key, size_t len, size_t *vlen, void *priv)
{
    groupcached_stored_item *item = (groupcached_stored_item *)ht_get(storage, key, len);
    if (!item)
        return NULL;
    DEBUG2("GOT KEY %s VALUE %s\n\n", key, (char *)item->value);
    if (vlen)
        *vlen = item->size;
    return item->value;
}

static void free_stored_item(void *v)
{
    groupcached_stored_item *item = (groupcached_stored_item *)v;
    free(item->value);
    free(item);
}

static int string2sockaddr(const char *host, int port, struct sockaddr_in *sockaddr)
{
    u_int32_t ip = htonl(INADDR_LOOPBACK);
    errno = EINVAL;

    if (host) {
	char host2[512];
	char *p;
	char *pe;

	strncpy(host2, host, sizeof(host2)-1);
	p = strchr(host2, ':');

	if (p) {				// check for <host>:<port>
	    *p = '\0';				// point to port part
	    p++;
	    port = strtol(p, &pe, 10);		// convert string to number
	    if (*pe != '\0') {			// did not match complete string? try as string
		struct servent *e = getservbyname(p, "tcp");
		if (!e) {
		    errno = ENOENT;		// to avoid errno == 0 in error case
		    return -1;
		}
		port = ntohs(e->s_port);
	    }
	}

        if (strcmp(host2, "*") == 0) {
            ip = INADDR_ANY;
        } else {
            if (!inet_aton(host2, (struct in_addr *)&ip)) {
                struct hostent *e = gethostbyname(host2);
		if (!e || e->h_addrtype != AF_INET) {
		    errno = ENOENT;		// to avoid errno == 0 in error case
		    return -1;
		}
                ip = ((unsigned long *) (e->h_addr_list[0]))[0];
            }
        }
    }
    if (port == 0)
	return -1;
    else
	port = htons(port);

    bzero(sockaddr, sizeof(struct sockaddr_in));
#ifndef __linux
    sockaddr->sin_len = sizeof(struct sockaddr_in);
#endif
    sockaddr->sin_family = AF_INET;
    sockaddr->sin_addr.s_addr = ip;
    sockaddr->sin_port = port;

    return 0;
}

static int open_socket(const char *host, int port)
{
    int val = 1;
    struct sockaddr_in sockaddr;
    int sock;

    errno = EINVAL;
    if (host == NULL || strlen(host) == 0 || port == 0)
	return -1;

    sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == -1)
	return -1;

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &val,  sizeof(val));

    if (string2sockaddr(host, port, &sockaddr) == -1
	|| bind(sock, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) == -1) {
	shutdown(sock, SHUT_RDWR);
	close(sock);
	return -1;
    }

    listen(sock, -1);
    fcntl(sock, F_SETFD, FD_CLOEXEC);

    return sock;
}


int main(int argc, char **argv)
{

    int option_index = 0;
    int foreground = 0;
    int loglevel = GROUPCACHED_LOGLEVEL_DEFAULT;
    char *listen_address = GROUPCACHED_ADDRESS_DEFAULT;
    uint16_t listen_port = GROUPCACHED_PORT_DEFAULT;
    char *peers = NULL;

    static struct option long_options[] = {
        {"debug", 2, 0, 'd'},
        {"foreground", 0, 0, 'f'},
        {"listen", 2, 0, 'l'},
        {"peers", 2, 0, 'p'},
        {"nohttp", 0, 0, 'n'},
        {"singlethread", 0, 0, 's'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0}
    };

    char c;
    while ((c = getopt_long (argc, argv, "d:fhl:np:s?", long_options, &option_index))) {
        if (c == -1) {
            break;
        }
        switch (c) {
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
                single_thread = 1;
                break;
            case 'n':
                use_http = 0;
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

    char *shard_names[GROUPCACHED_MAX_SHARDS];

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

    if (!foreground)
        daemon(0, 0);

    log_init("groupcached", loglevel);

    cache = groupcache_create(me, shard_names, cnt, cache_fetch_item, free, NULL);
    groupcache_init();

    signal(SIGHUP, groupcached_reload);
    signal(SIGINT, groupcached_stop);
    signal(SIGQUIT, groupcached_stop);
    signal(SIGPIPE, groupcached_do_nothing);

    // initialize the callbacks descriptor
    iomux_callbacks_t groupcached_callbacks = {
        .mux_connection = groupcached_connection_handler,
        .mux_input = groupcached_input_handler,
        .mux_eof = groupcached_eof_handler,
        .mux_output = NULL,
        .mux_timeout = NULL,
        .priv = &groupcached_callbacks
    };

    storage = ht_create(1024);
    ht_set_free_item_callback(storage, free_stored_item);
    iomux = iomux_create();


    int listen_fd = open_socket(listen_address, listen_port);    
    if (listen_fd < 0) {
        ERROR("Can't bind address %s:%d - %s",
                listen_address, listen_port, strerror(errno));
        exit(-1);
    }
    NOTICE("Listening on %s:%d", listen_address, listen_port);

    iomux_add(iomux, listen_fd, &groupcached_callbacks);
    iomux_listen(iomux, listen_fd);

    // this takes over the runloop and handle incoming connections
    iomux_loop(iomux, 0);

    // if we are here, iomux has exited the loop
    NOTICE("exiting");
    iomux_destroy(iomux);
    //groupcache_destroy(ATOMIC_READ(groupcache));
    close(listen_fd);
    ht_destroy(storage);
    
    exit(0);
}
