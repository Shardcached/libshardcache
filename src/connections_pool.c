#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include <fbuf.h>
#include <hashtable.h>
#include <queue.h>

#include "connections_pool.h"
#include "messaging.h"

#include <errno.h>

#include "atomic.h"

struct __connections_pool_s {
    hashtable_t *table;
    int tcp_timeout;
    int max_spare;
    int check;
};

connections_pool_t *
connections_pool_create(int tcp_timeout, int max_spare)
{
    connections_pool_t *cc = calloc(1, sizeof(connections_pool_t));
    cc->table = ht_create(128, 65535, (ht_free_item_callback_t)queue_destroy);
    cc->tcp_timeout = tcp_timeout;
    cc->max_spare = max_spare;
    return cc;
}

void
connections_pool_destroy(connections_pool_t *cc)
{
    ht_destroy(cc->table);
    free(cc);
}

static void
free_connection(void *conn)
{
    int *fdp = (int *)conn;
    close(*fdp);
    free(fdp);
}

static queue_t *
get_connection_queue(connections_pool_t *cc, char *addr)
{
    static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_lock(&lock);
    queue_t *connection_queue = ht_get(cc->table, addr, strlen(addr), NULL);
    if (!connection_queue) {
        // there is no queue, so we are the first one opening a connection to 'addr'
        connection_queue = queue_create();
        queue_set_free_value_callback(connection_queue, free_connection);
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

int
connections_queue_empty(hashtable_t *table, void *value, size_t vlen, void *user)
{
    queue_t *connection_queue = (queue_t *)value;
    int *fd = queue_pop_left(connection_queue);
    while (fd) {
        close(*fd);
        free(fd);
        fd = queue_pop_left(connection_queue);
    }
    return 1;
}

int
connections_pool_get(connections_pool_t *cc, char *addr)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return -1;

    int *fd = queue_pop_left(connection_queue);
    while (fd) {
        int rfd = *fd;
        free(fd);
        int flags = fcntl(rfd, F_GETFL, 0);
        if (flags == -1) {
            close(rfd);
            fd = queue_pop_left(connection_queue);
            continue;
        }

        if (!ATOMIC_READ(cc->check))
            return rfd;

        flags &= ~O_NONBLOCK;
        fcntl(rfd, F_SETFL, flags);
        char noop = SHC_HDR_NOOP;

        // XXX - this is a blocking write
        if (write(rfd, &noop, 1) == 1)
            return rfd;
        else
            close(rfd);

        fd = queue_pop_left(connection_queue);
    }

    int new_fd = connect_to_peer(addr, ATOMIC_READ(cc->tcp_timeout));
    if (new_fd == -1 && (errno == EMFILE || errno == ENFILE)) {
        ht_foreach_value(cc->table, connections_queue_empty, NULL);
        // give us one more chance
        new_fd = connect_to_peer(addr, ATOMIC_READ(cc->tcp_timeout));
    }
    return new_fd; 
}


void
connections_pool_add(connections_pool_t *cc, char *addr, int fd)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue) {
        close(fd);
        return;
    }

    if (queue_count(connection_queue) < ATOMIC_READ(cc->max_spare)) {
        int *fdp = malloc(sizeof(int));
        *fdp = fd;
        if (queue_push_right(connection_queue, fdp) != 0) {
            free(fdp);
            close(fd);
        }
    } else {
        close(fd);
    }
}

int
connections_pool_tcp_timeout(connections_pool_t *cc, int new_value)
{
    int old_value = ATOMIC_READ(cc->tcp_timeout);

    if (new_value >= 0)
        ATOMIC_SET(cc->tcp_timeout, new_value);

    return old_value;
}

int
connections_pool_max_spare(connections_pool_t *cc, int new_value)
{
    int old_value = ATOMIC_READ(cc->max_spare);

    if (new_value >= 0)
        ATOMIC_SET(cc->max_spare, new_value);

    return old_value;
}

int
connections_pool_check(connections_pool_t *cc, int new_value)
{
    int old_value = ATOMIC_READ(cc->check);

    ATOMIC_SET(cc->check, new_value);

    return old_value;
}
