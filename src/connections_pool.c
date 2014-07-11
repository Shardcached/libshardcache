#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include <fbuf.h>
#include <hashtable.h>
#include <queue.h>

#include <atomic_defs.h>

#include "connections_pool.h"
#include "messaging.h"

#include <errno.h>


#include <time.h>

struct __connections_pool_s {
    hashtable_t *table;
    int tcp_timeout;
    int max_spare;
    int check;
    int expire_time;
};

struct __connection_pool_entry_s {
    int fd;
    struct timeval last_access;
};

static inline int
is_connection_time_valid(connections_pool_t *cc, struct timeval *conn_time)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    struct timeval result = { 0, 0 };
    int expire_time = ATOMIC_READ(cc->expire_time);
    struct timeval threshold = { expire_time/1000, expire_time*1000 };
    timersub(&now, conn_time, &result);

    if (timercmp(&result, &threshold, >))
        return 0;

    return 1;
}

connections_pool_t *
connections_pool_create(int tcp_timeout, int expire_time, int max_spare)
{
    connections_pool_t *cc = calloc(1, sizeof(connections_pool_t));
    cc->table = ht_create(128, 65535, (ht_free_item_callback_t)queue_destroy);
    cc->tcp_timeout = tcp_timeout;
    cc->max_spare = max_spare;
    cc->expire_time = expire_time;
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
    connection_pool_entry_t *entry = (connection_pool_entry_t *)conn;
    close(entry->fd);
    free(entry);
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
        queue_set_bpool_size(connection_queue, cc->max_spare);
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
    connection_pool_entry_t *entry = queue_pop_left(connection_queue);
    while (entry) {
        close(entry->fd);
        free(entry);
        entry = queue_pop_left(connection_queue);
    }
    return 1;
}

int
connections_pool_get(connections_pool_t *cc, char *addr)
{
    queue_t *connection_queue = get_connection_queue(cc, addr);
    if (!connection_queue)
        return -1;

    connection_pool_entry_t *entry = queue_pop_left(connection_queue);
    while (entry) {
        int fd = entry->fd;
        struct timeval last_access = entry->last_access;
        free(entry);
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1) {
            close(fd);
            entry = queue_pop_left(connection_queue);
            continue;
        }

        if (!ATOMIC_READ(cc->check))
            return fd;

        flags &= ~O_NONBLOCK;
        fcntl(fd, F_SETFL, flags);
        char noop = SHC_HDR_NOOP;

        // XXX - this is a blocking write
        if (is_connection_time_valid(cc, &last_access) || write(fd, &noop, 1) == 1)
            return fd;
        else
            close(fd);

        entry = queue_pop_left(connection_queue);
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
        connection_pool_entry_t *entry = malloc(sizeof(connection_pool_entry_t));
        entry->fd = fd;
        gettimeofday(&entry->last_access, NULL);
        if (queue_push_right(connection_queue, entry) != 0) {
            free(entry);
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
connections_pool_expire_time(connections_pool_t *cc, int new_value)
{
    int old_value = ATOMIC_READ(cc->expire_time);

    if (new_value >= 0)
        ATOMIC_SET(cc->expire_time, new_value);

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
