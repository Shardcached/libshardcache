#define _GNU_SOURCE
#include <shardcache.h>

static const int    FAKE_KEYS_NUMBERS = 10;
static const char * FAKE_KEYS_FORMAT  = "Example key n. %d";

int storage_version = SHARDCACHE_STORAGE_API_VERSION;

static int
st_fetch(void *key, size_t klen, void **value, size_t *vlen, void *priv)
{
    *value = malloc(klen);
    *vlen  = klen;
    memcpy(*value, key, klen);
    return 0;
}

static size_t
st_count(void *priv)
{
    return FAKE_KEYS_NUMBERS;
}

static size_t
st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv)
{
    for (int i = 0; i < isize; i++) {
        char key[50];

        snprintf(key, sizeof(key), FAKE_KEYS_FORMAT, i);

        index[i].key  = strndup(key, sizeof(key));
        index[i].klen = strlen(key);
        index[i].vlen = strlen(key);
    }

    return isize;
}

int
storage_init(shardcache_storage_t *storage, const char **options)
{
    storage->fetch  = st_fetch;
    storage->count  = st_count;
    storage->index  = st_index;
    storage->shared = 1;
    storage->global = 1;

    return 0;
}

void
storage_destroy(void *priv)
{
    // Empty.
}
