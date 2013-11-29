#include <stdlib.h>
#include <string.h>
#include <log.h>
#include <hashtable.h>
#include <pthread.h>
#include "storage_mem.h"

typedef struct {
    void *value;
    size_t size;
} stored_item_t;

typedef struct {
    hashtable_t *table;
    pthread_mutex_t lock;
} mem_storage_t;

static void free_item_cb(void *ptr) {
    stored_item_t *item = (stored_item_t *)ptr;
    if (item->value)
        free(item->value);
    free(item);
}

static void *st_init(const char **args)
{
    int size = 1024;

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
                } else {
                    ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    mem_storage_t *storage = calloc(1, sizeof(mem_storage_t));
    pthread_mutex_init(&storage->lock, NULL);
    storage->table = ht_create(size);
    ht_set_free_item_callback(storage->table, free_item_cb);
    return storage;
}

static void *st_fetch(void *key, size_t len, size_t *vlen, void *priv)
{
    mem_storage_t *storage = (mem_storage_t *)priv;
    hashtable_t *table = storage->table;
    pthread_mutex_lock(&storage->lock);
    stored_item_t *item =  ht_get(table, key, len);
    void *v = NULL;
    if (item) {
        v = malloc(item->size);
        memcpy(v, item->value, item->size);
        if (vlen) 
            *vlen = item->size;
    }
    pthread_mutex_unlock(&storage->lock);
    return v;
}

static int st_store(void *key, size_t len, void *value, size_t vlen, void *priv)
{
    mem_storage_t *storage = (mem_storage_t *)priv;
    hashtable_t *table = storage->table;
    stored_item_t *new_item = malloc(sizeof(stored_item_t));
    new_item->value = malloc(vlen);
    memcpy(new_item->value, value, vlen);
    new_item->size = vlen;
    pthread_mutex_lock(&storage->lock);
    stored_item_t *previous_item = (stored_item_t *)ht_set(table, key, len, new_item);
    pthread_mutex_unlock(&storage->lock);
    if (previous_item) {
        free(previous_item->value);
        free(previous_item);
    }
    return 0;
}

static int st_remove(void *key, size_t len, void *priv) {
    mem_storage_t *storage = (mem_storage_t *)priv;
    hashtable_t *table = storage->table;
    pthread_mutex_lock(&storage->lock);
    ht_delete(table, key, len);
    pthread_mutex_unlock(&storage->lock);
    return 0;
}

static void st_destroy(void *priv) {
    mem_storage_t *storage = (mem_storage_t *)priv;
    hashtable_t *table = storage->table;
    ht_destroy(table);
    free(storage);
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
