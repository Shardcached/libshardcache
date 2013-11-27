#include <stdlib.h>
#include <string.h>
#include <log.h>
#include <hashtable.h>
#include "storage_mem.h"

typedef struct {
    void *value;
    size_t size;
} stored_item_t;


static void free_item_cb(void *ptr) {
    stored_item_t *item = (stored_item_t *)ptr;
    if (item->value)
        free(item->value);
    free(item);
}

static void *st_init(char **args)
{
    int size = 1024;

    if (args) {
        while (*args) {
            char *key = *args++;
            char *value = NULL;
            if (*args) {
                value = *args++;
            } else {
                ERROR("Odd element in the options array");
                continue;
            }
            if (key && value) {
                if (strcmp(key, "initial_table_size") == 0) {
                    size = atoi(value);
                } else if (strcmp(key, "max_table_size") == 0) {
                } else {
                    ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    hashtable_t *storage = ht_create(size);
    ht_set_free_item_callback(storage, free_item_cb);
    return storage;
}

static void *st_fetch(void *key, size_t len, size_t *vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *item =  ht_get(storage, key, len);
    if (item && vlen)
        *vlen = item->size;
    return item;
}

static int st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *new_item = malloc(sizeof(stored_item_t));
    new_item->value = value;
    new_item->size = vlen;
    stored_item_t *previous_item = (stored_item_t *)ht_set(storage, key, len, new_item);
    if (previous_item) {
        free(previous_item->value);
        free(previous_item);
    }
    return 0;
}

static int st_remove(void *key, size_t len, void *priv) {
    hashtable_t *storage = (hashtable_t *)priv;
    ht_delete(storage, key, len);
    return 0;
}

static void st_destroy(void *priv) {
    hashtable_t *storage = (hashtable_t *)priv;
    ht_destroy(storage);
}

shardcache_storage_t storage_mem = {
    .init_storage    = st_init,
    .fetch_item      = st_fetch,
    .store_item      = st_store,
    .remove_item     = st_remove,
    .destroy_storage = st_destroy
};


