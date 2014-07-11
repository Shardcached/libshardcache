#ifndef __CONNECTIONS_POOL_H__
#define __CONNECTIONS_POOL_H__

typedef struct __connections_pool_s connections_pool_t;

typedef struct __connection_pool_entry_s connection_pool_entry_t;

connections_pool_t * connections_pool_create(int tcp_timeout, int expire_time, int max_spare);
void connections_pool_destroy(connections_pool_t *cc);
int connections_pool_get(connections_pool_t *cc, char *addr);
void connections_pool_add(connections_pool_t *cc, char *addr, int fd);
int connections_pool_tcp_timeout(connections_pool_t *cc, int new_value);
int connections_pool_check(connections_pool_t *cc, int new_value);
int connections_pool_expire_time(connections_pool_t *cc, int new_value);

#endif

