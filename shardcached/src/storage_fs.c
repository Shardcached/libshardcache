#include <stdlib.h>
#include <string.h>
#include <log.h>
#include <fbuf.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "storage_fs.h"

typedef struct {
    char *path;
    char *tmp;
} storage_fs_t;

static void *st_init(const char **args)
{
    storage_fs_t *storage = NULL;
    char *storage_path = NULL;
    char *tmp_path = NULL;
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
                if (strcmp(key, "storage_path") == 0) {
                    storage_path = strdup(value);
                } else if (strcmp(key, "tmp_path") == 0) {
                    tmp_path = strdup(value);
                }else {
                    ERROR("Unknown option name %s", key);
                }
            }
        }
    }
    if (storage_path) {
        int check = access(storage_path, R_OK|W_OK);
        if (check != 0) {
            /* TODO - Error Messages */
            return NULL;
        }

        storage = calloc(1, sizeof(storage_fs_t));
        storage->path = storage_path;
        if (tmp_path)
            storage->tmp = tmp_path;
        else
            storage->tmp = strdup("/tmp");
        check = access(storage->tmp, R_OK|W_OK);
        if (check != 0) {
            /* TODO - Error Messages */
            free(storage);
            return NULL;
        }
    } else {
        // TODO - Error Messages
    }
    return storage;
}

static void *st_fetch(void *key, size_t klen, size_t *vlen, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;

    size_t fullpath_len = strlen(storage->path)+klen+2;
    char *fullpath = malloc(fullpath_len);
    snprintf(fullpath, fullpath_len, "%s/%s", storage->path, key);
    int fd = open(fullpath, O_RDONLY|O_SHLOCK);
    if (fd >=0) {
        fbuf_t buf = FBUF_STATIC_INITIALIZER;
        int rb = fbuf_read(&buf, fd, 1024);
        while (rb != -1) {
            rb = fbuf_read(&buf, fd, 1024);
            if (rb == 0)
                break;
        }
        close(fd);
        if (fbuf_used(&buf)) {
            if (vlen)
                *vlen = fbuf_used(&buf);
            return fbuf_data(&buf);
        }
    }
    return NULL;
}

static int st_store(void *key, size_t klen, void *value, size_t vlen, void *priv)
{
    storage_fs_t *storage = (storage_fs_t *)priv;

    int ret = -1;
    size_t tmppath_len = strlen(storage->path)+klen+2;
    char *tmppath = malloc(tmppath_len);
    snprintf(tmppath, tmppath_len, "%s/%s", storage->tmp, key);
    int fd = open(tmppath, O_WRONLY|O_EXLOCK|O_TRUNC|O_CREAT, S_IRWXU|S_IRWXG|S_IRWXO);
    if (fd >=0) {
        int ofx = 0;
        while (ofx != vlen) {
            int wb = write(fd, value+ofx, vlen - ofx);
            if (wb > 0) { 
                ofx += wb;
            } else if (wb == 0 || (wb == -1 && errno != EINTR && errno != EAGAIN)) {
                // TODO - Error messages
                break;
            }
        }
        if (ofx == vlen) {
            size_t fullpath_len = strlen(storage->path)+klen+2;
            char *fullpath = malloc(fullpath_len);
            snprintf(fullpath, fullpath_len, "%s/%s", storage->path, key);
            ret = link(tmppath, fullpath);
            unlink(tmppath);
        }
        close(fd);
    }
    return ret;
}

static int st_remove(void *key, size_t klen, void *priv) {
    storage_fs_t *storage = (storage_fs_t *)priv;

    size_t fullpath_len = strlen(storage->path)+klen+1;
    char *fullpath = malloc(fullpath_len);
    snprintf(fullpath, fullpath_len, "%s/%s", storage->path, key);
    return unlink(fullpath); 
}

static void st_destroy(void *priv) {
    storage_fs_t *storage = (storage_fs_t *)priv;
    free(storage->path);
    free(storage->tmp);
    free(storage);
}

shardcache_storage_t *storage_fs_create(const char **options) {
    shardcache_storage_t *st = calloc(1, sizeof(shardcache_storage_t));
    st->init_storage    = st_init;
    st->fetch_item      = st_fetch;
    st->store_item      = st_store;
    st->remove_item     = st_remove;
    st->destroy_storage = st_destroy;
    st->options = options;
    return st;
}

void storage_fs_destroy(shardcache_storage_t *st) {
    free(st);
}
