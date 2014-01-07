#ifndef __CONNECTIONS_POOL_H__
#define __CONNECTIONS_POOL_H__

typedef struct __connections_pool_s connections_pool_t;

connections_pool_t * connections_pool_create(int tcp_timeout);
void connections_pool_destroy(connections_pool_t *cc);
int connections_pool_get(connections_pool_t *cc, char *addr);
void connections_pool_add(connections_pool_t *cc, char *addr, int fd);
int connections_pool_tcp_timeout(connections_pool_t *cc, int new_value);

#endif

