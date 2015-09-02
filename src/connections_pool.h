#ifndef CONNECTIONS_POOL_H
#define CONNECTIONS_POOL_H

typedef struct _connections_pool_s connections_pool_t;

typedef struct _connection_pool_entry_s connection_pool_entry_t;

connections_pool_t * connections_pool_create(int tcp_timeout, int expire_time, int max_spare);
void connections_pool_destroy(connections_pool_t *cc);
int connections_pool_get(connections_pool_t *cc, char *addr);
void connections_pool_add(connections_pool_t *cc, char *addr, int fd);
int connections_pool_tcp_timeout(connections_pool_t *cc, int new_value);
int connections_pool_check(connections_pool_t *cc, int new_value);
int connections_pool_expire_time(connections_pool_t *cc, int new_value);

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
