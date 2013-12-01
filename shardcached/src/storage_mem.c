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

static void *st_init(const char **args)
{
    int size = 1024;
    int maxsize = 1 << 20;
    if (args) {
        while (*args) {
            char *key = (char *)*args++;
            char *value = NULL;
            if (*args) {
                value = (char *)*args++;
            } else {
                ERROR("Odd element in the options array");
                continue;
            }
            if (key && value) {
                if (strcmp(key, "initial_table_size") == 0) {
                    size = atoi(value);
                } else if (strcmp(key, "max_table_size") == 0) {
                    maxsize = atoi(value);
                } else {
                    ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    hashtable_t *storage = ht_create(size, maxsize, free_item_cb);
    return storage;
}

static void *copy_item_cb(void *ptr, size_t len) {
    stored_item_t *item = (stored_item_t *)ptr;
    stored_item_t *copy = malloc(sizeof(stored_item_t));
    copy->value = malloc(item->size);
    memcpy(copy->value, item->value, item->size);
    copy->size = item->size;
    return copy;
}

static void *st_fetch(void *key, size_t len, size_t *vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *item =  ht_get_deep_copy(storage, key, len, NULL, copy_item_cb);
    void *v = NULL;
    if (item) {
        v = item->value;
        if (vlen) 
            *vlen = item->size;
    }
    return v;
}

static int st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    hashtable_t *storage = (hashtable_t *)priv;
    stored_item_t *new_item = malloc(sizeof(stored_item_t));
    new_item->value = malloc(vlen);
    memcpy(new_item->value, value, vlen);
    new_item->size = vlen;
    ht_set(storage, key, len, new_item, sizeof(stored_item_t), NULL, NULL);
    return 0;
}

static int st_remove(void *key, size_t len, void *priv) {
    hashtable_t *storage = (hashtable_t *)priv;
    ht_delete(storage, key, len, NULL, NULL);
    return 0;
}

static void st_destroy(void *priv) {
    hashtable_t *storage = (hashtable_t *)priv;
    ht_destroy(storage);
}

shardcache_storage_t *storage_mem_create(const char **options) {
    shardcache_storage_t *st = calloc(1, sizeof(shardcache_storage_t));
    st->init_storage    = st_init;
    st->fetch_item      = st_fetch;
    st->store_item      = st_store;
    st->remove_item     = st_remove;
    st->destroy_storage = st_destroy;
    st->options = options;
    return st;
}

void storage_mem_destroy(shardcache_storage_t *st) {
    free(st);
}
