#ifndef __CONNECTION_CACHE_H__
#define __CONNECTION_CACHE_H__

typedef struct __connection_cache_s connection_cache_t;

connection_cache_t * connection_cache_create(int tcp_timeout);
void connection_cache_destroy(connection_cache_t *cc);
int connection_cache_get(connection_cache_t *cc, char *addr);
void connection_cache_add(connection_cache_t *cc, char *addr, int fd);
int connection_cache_tcp_timeout(connection_cache_t *cc, int new_value);
#endif

