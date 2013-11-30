#include <shardcache.h>

shardcache_storage_t *storage_fs_create(const char **options);
void storage_fs_destroy(shardcache_storage_t *st);
