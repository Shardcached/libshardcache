#ifndef __STORAGE_H__
#define __STORAGE_H__

#include <stdint.h>

typedef struct shc_storage_callbacks_s {
    void *(*init)(char **args);
    void *(*fetch)(void *key, size_t klen, size_t *vlen, void *priv);
    int (*store)(void *key, size_t kklen, void *value, size_t vlen, void *priv);
    int (*remove)(void *key, size_t klen, void *priv);
    void (*destroy)(void *priv);
} shc_storage_callbacks_t;

typedef struct shc_storage_s shc_storage_t;

shc_storage_t *shc_storage_create(shc_storage_callbacks_t *callbacks, char **args);

void *shc_storage_fetch(shc_storage_t *st, void *key, size_t klen, size_t *vlen);
int shc_storage_store(shc_storage_t *st, void *key, size_t klen, void *value, size_t vlen);
int shc_storage_remove(shc_storage_t *st, void *key, size_t klen);

void shc_storage_destroy(shc_storage_t *st);

#endif
