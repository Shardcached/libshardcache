#include <unistd.h>
#include <pthread.h>

#include <fbuf.h>
#include <hashtable.h>
#include <queue.h>

#include "connection_cache.h"
#include "messaging.h"

struct __connection_cache_s {
    hashtable_t *table;
    int tcp_timeout;
};

connection_cache_t *
connection_cache_create(int tcp_timeout)
{
    connection_cache_t *cc = calloc(1, sizeof(connection_cache_t));
    cc->table = ht_create(128, 65535, (ht_free_item_callback_t)queue_destroy);
    cc->tcp_timeout = tcp_timeout;
    return cc;
}

void
connection_cache_destroy(connection_cache_t *cc)
{
    ht_destroy(cc->table);
    free(cc);
}

queue_t *
get_connection_queue(connection_cache_t *cc, char *addr)
{
    static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&lock);
    queue_t *connection_queue = ht_get(cc->table, addr, strlen(addr), NULL);
    if (!connection_queue) {
        // there is no queue, so we are the first one opening a connection to 'addr'
        connection_queue = queue_create();
        queue_set_free_value_callback(connection_queue, free);
        if (ht_set(cc->table, addr, strlen(addr), connection_queue, 0) != 0) {
            // ERRORS
            queue_destroy(connection_queue);
            pthread_mutex_unlock(&lock);
            return NULL;
        }
    }
    pthread_mutex_unlock(&lock);
    return connection_queue;
}

int connection_cache_get(connection_cache_t *cc, char *addr)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return -1;

    int *fd = queue_pop_left(connection_queue);
    while (fd) {
        char noop = SHARDCACHE_HDR_NOP;
        if (write(*fd, &noop, 1) == 1) {
            int rfd = *fd;
            free(fd);
            return rfd;
        } else {
            close(*fd);
            free(fd);
        }
        fd = queue_pop_left(connection_queue);
    }

    int new_fd = connect_to_peer(addr, __sync_fetch_and_add(&cc->tcp_timeout, 0));
    if (new_fd) {
        fd = malloc(sizeof(int));
        *fd = new_fd;
        queue_push_right(connection_queue, (void *)fd);
    }

    return new_fd; 
}


void connection_cache_add(connection_cache_t *cc, char *addr, int fd)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return;
    int *fdp = malloc(sizeof(int));
    *fdp = fd;
    queue_push_right(connection_queue, fdp);
}

int connection_cache_tcp_timeout(connection_cache_t *cc, int new_value)
{
    int old_value = 0;
    do {
        old_value = __sync_fetch_and_add(&cc->tcp_timeout, 0);
    } while (!__sync_bool_compare_and_swap(&cc->tcp_timeout, old_value, new_value));
    return old_value;
}
