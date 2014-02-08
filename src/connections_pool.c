#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include <fbuf.h>
#include <hashtable.h>
#include <queue.h>

#include "connections_pool.h"
#include "messaging.h"

#include "atomic.h"

struct __connections_pool_s {
    hashtable_t *table;
    int tcp_timeout;
};

connections_pool_t *
connections_pool_create(int tcp_timeout)
{
    connections_pool_t *cc = calloc(1, sizeof(connections_pool_t));
    cc->table = ht_create(128, 65535, (ht_free_item_callback_t)queue_destroy);
    cc->tcp_timeout = tcp_timeout;
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

int connections_pool_get(connections_pool_t *cc, char *addr)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return -1;

    int *fd = queue_pop_left(connection_queue);
    while (fd) {
        char noop = SHC_HDR_NOOP;
        if (write(*fd, &noop, 1) == 1) {
            int rfd = *fd;
            free(fd);

            int flags = fcntl(rfd, F_GETFL, 0);
            if (flags == -1) {
                close(rfd);
                return -1;
            }

            flags &= ~O_NONBLOCK;
            fcntl(rfd, F_SETFL, flags);

            return rfd;
        } else {
            close(*fd);
            free(fd);
        }
        fd = queue_pop_left(connection_queue);
    }

    int new_fd = connect_to_peer(addr, ATOMIC_READ(cc->tcp_timeout));

    return new_fd; 
}


void connections_pool_add(connections_pool_t *cc, char *addr, int fd)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return;
    int *fdp = malloc(sizeof(int));
    *fdp = fd;
    queue_push_right(connection_queue, fdp);
}

int connections_pool_tcp_timeout(connections_pool_t *cc, int new_value)
{
    int old_value = ATOMIC_READ(cc->tcp_timeout);

    if (new_value >= 0)
        ATOMIC_SET(cc->tcp_timeout, new_value);

    return old_value;
}
