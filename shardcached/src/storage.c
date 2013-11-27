#include <stdlib.h>
#include "storage.h"

struct shc_storage_s {
    shc_storage_callbacks_t *ops;
    void *priv;
};

shc_storage_t *shc_storage_create(shc_storage_callbacks_t *callbacks, char **args) {
    shc_storage_t *st = malloc(sizeof(shc_storage_t));
    st->ops = callbacks;
    if (st->ops->init)
        st->priv = st->ops->init(args);
    return st;
}

void *shc_storage_fetch(shc_storage_t *st, void *key, size_t klen, size_t *vlen) {
    if (st->ops->fetch)
        return st->ops->fetch(key, klen, vlen, st->priv);
    return NULL;
}

int shc_storage_store(shc_storage_t *st, void *key, size_t klen, void *value, size_t vlen) {
    if (st->ops->store)
        return st->ops->store(key, klen, value, vlen, st->priv);
    return -1;
}

int shc_storage_remove(shc_storage_t *st, void *key, size_t klen) {
    if (st->ops->remove)
        return st->ops->remove(key, klen, st->priv);
    return -1;
}


void shc_storage_destroy(shc_storage_t *st) {
    if (st->ops->destroy)
        st->ops->destroy(st->priv);
    free(st);
}

