#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>

#include "shardcache.h"
#include "shardcache_internal.h"
#include "arc_ops.h"
#include "messaging.h"

/**
 * * Here are the operations implemented
 *
 * */

typedef struct {
    cached_object_t *obj;
    void *data;
    size_t len;
} shardcache_fetch_from_peer_notify_arg;

static int
arc_ops_fetch_from_peer_notify_listener (void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    shardcache_fetch_from_peer_notify_arg *arg = (shardcache_fetch_from_peer_notify_arg *)user;
    cached_object_t *obj = arg->obj;
    int rc = listener->cb(obj->key, obj->klen, arg->data, arg->len, 0, NULL, listener->priv);
    if (rc != 0) {
        free(listener);
        return -1;
    }
    return 1;
}

static int
arc_ops_fetch_from_peer_notify_listener_complete(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cached_object_t *obj = (cached_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, obj->dlen, &obj->ts, listener->priv);
    free(listener);
    return -1;
}

static int
arc_ops_fetch_from_peer_notify_listener_error(void *item, uint32_t idx, void *user)
{
    shardcache_get_listener_t *listener = (shardcache_get_listener_t *)item;
    cached_object_t *obj = (cached_object_t *)user;
    listener->cb(obj->key, obj->klen, NULL, 0, 0, NULL, listener->priv);
    free(listener);
    return -1;
}

typedef struct
{
    cached_object_t *obj;
    shardcache_t *cache;
    char *peer_addr;
    int fd;
} shc_fetch_async_arg_t;

static int
arc_ops_fetch_from_peer_async_cb(char *peer,
                                 void *key,
                                 size_t klen,
                                 void *data,
                                 size_t len,
                                 int status, // 0 OK, -1 ERR, 1 DONE
                                 void *priv)
{
    shc_fetch_async_arg_t *arg = (shc_fetch_async_arg_t *)priv;
    cached_object_t *obj = arg->obj;
    shardcache_t *cache = arg->cache;
    char *peer_addr = arg->peer_addr;
    int fd = arg->fd;
    int total_len = 0;

    MUTEX_LOCK(&obj->lock);

    if (!obj->res) {
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
        if (fd >= 0)
            close(fd);
        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        MUTEX_UNLOCK(&obj->lock);
        return -1;
    }
    if (!obj->listeners) {
        if (fd >= 0)
            close(fd);
        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        MUTEX_UNLOCK(&obj->lock);
        free(arg);
        arc_release_resource(cache->arc, obj->res);
        return -1;
    }
    if (status == -1) {
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
        if (fd >= 0)
            close(fd);
        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        MUTEX_UNLOCK(&obj->lock);
        arc_drop_resource(cache->arc, obj->res);
        free(arg);
        return -1;
    } else if (status == 1) {

        if (fd >= 0)
            shardcache_release_connection_for_peer(cache, peer_addr, fd);
        free(arg);

        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        int drop = (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_DROP) || COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICT) || !obj->dlen);

        MUTEX_UNLOCK(&obj->lock);

        if (drop)
            arc_drop_resource(cache->arc, obj->res);
        else
            arc_release_resource(cache->arc, obj->res);

        return 0;
    } else if (len) {
        size_t olen = obj->dlen;
        obj->dlen += len;
        if (obj->dlen > sizeof(obj->dbuf)) {
            if (obj->data == obj->dbuf) {
                obj->data = malloc(obj->dlen);
                if (olen)
                    memcpy(obj->data, obj->dbuf, olen);
            } else {
                obj->data = realloc(obj->data, obj->dlen);
            }
        } else {
            obj->data = obj->dbuf;
        }
        memcpy(obj->data + olen, data, len);
        shardcache_fetch_from_peer_notify_arg arg = {
            .obj = obj,
            .data = data,
            .len = len
        };
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener, &arg);
        MUTEX_UNLOCK(&obj->lock);
        return 0;
    } else {
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_complete, obj);
        COBJ_SET_FLAG(obj, COBJ_FLAG_COMPLETE);
        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        total_len = obj->dlen;

        int evicted = COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICT) ||
                      COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICTED);

        if (total_len && !COBJ_CHECK_FLAGS(obj, COBJ_FLAG_DROP)) {
            arc_update_resource_size(cache->arc, obj->res,
                    sizeof(cached_object_t) + ((obj->data == obj->dbuf) ? 0 : total_len));

            if (cache->expire_time > 0 && !evicted && !cache->lazy_expiration)
                shardcache_schedule_expiration(cache, key, klen, cache->expire_time, 0);

        }
        if (!total_len)
            COBJ_SET_FLAG(obj, COBJ_FLAG_DROP);

        MUTEX_UNLOCK(&obj->lock);
        return 0;
    }
}


static int
arc_ops_fetch_from_peer(shardcache_t *cache, cached_object_t *obj, char *peer)
{
    int rc = -1;
    if (shardcache_log_level() >= LOG_DEBUG) {
        char keystr[1024];
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));
        SHC_DEBUG2("Fetching data for key %s from peer %s", keystr, peer); 
    }

    shardcache_node_t *node = shardcache_node_select(cache, peer);
    if (!peer) {
        SHC_ERROR("Can't find address for node %s\n", peer);
        return rc;
    }
    char *peer_addr = shardcache_node_get_address(node);

    // another peer is responsible for this item, let's get the value from there

    int fd = shardcache_get_connection_for_peer(cache, peer_addr);
    if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_ASYNC)) {
        shc_fetch_async_arg_t *arg = malloc(sizeof(shc_fetch_async_arg_t));
        arg->obj = obj;
        arg->cache = cache;
        arg->peer_addr = peer_addr;
        arg->fd = fd;
        async_read_wrk_t *wrk = NULL;
        arc_retain_resource(cache->arc, obj->res);
        rc = fetch_from_peer_async(peer_addr,
                                   (char *)cache->auth,
                                   SHC_HDR_CSIGNATURE_SIP,
                                   obj->key,
                                   obj->klen,
                                   0,
                                   0,
                                   arc_ops_fetch_from_peer_async_cb,
                                   arg,
                                   fd,
                                   &wrk);
        if (rc == 0) {
            // Keep the remote object in the cache only 10% of the time.
            // This is the same logic applied by groupcache to determine hot keys.
            // Better approaches are possible but maybe unnecessary.
            if (!cache->force_caching && random() % 10 != 0)
                COBJ_SET_FLAG(obj, COBJ_FLAG_DROP);
            else
                COBJ_UNSET_FLAG(obj, COBJ_FLAG_DROP);

            shardcache_queue_async_read_wrk(cache, wrk);
        } else {
            // if the storage is flagged as 'global' we don't want to notify the listeners yest
            // because an attempt of fetching form the local storage will be done in arc_ops_fetch()
            if (!cache->storage.global) {
                if (obj->listeners) {
                    list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
                    list_clear(obj->listeners);
                }

                COBJ_SET_FLAG(obj, COBJ_FLAG_EVICTED);
            }
            if (fd >= 0)
                close(fd);
            arc_release_resource(cache->arc, obj->res);

            free(arg);
        }
    } else { 
        fbuf_t value = FBUF_STATIC_INITIALIZER;
        rc = fetch_from_peer(peer_addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, obj->key, obj->klen, &value, fd);
        COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
        if (rc == 0) {
            shardcache_release_connection_for_peer(cache, peer_addr, fd);
            if (fbuf_used(&value)) {
                obj->data = fbuf_data(&value);
                obj->dlen = fbuf_used(&value);
                COBJ_SET_FLAG(obj, COBJ_FLAG_COMPLETE);
                if (!cache->force_caching && rand() % 10 != 0)
                    COBJ_SET_FLAG(obj, COBJ_FLAG_DROP);
                else
                    COBJ_UNSET_FLAG(obj, COBJ_FLAG_DROP);
            }
        } else {
            // if succeded the fbuf buffer has been moved to the obj structure
            // but otherwise we have to release it
            fbuf_destroy(&value);
            close(fd);
        }
    }

    return rc;
}

void
arc_ops_init(const void *key, size_t len, int async, arc_resource_t res, void *ptr, void *priv)
{
    // NOTE: the arc subsystem already allocates for us the memory where the
    // cached object needs to be stored. Such size was specified at creation time
    // as argument to arc_create()
    cached_object_t *obj = (cached_object_t *)ptr;

    obj->klen = len;
    if (obj->klen > sizeof(obj->kbuf))
        obj->key = malloc(obj->klen);
    else
        obj->key = obj->kbuf;
    memcpy(obj->key, key, obj->klen);
    obj->data = NULL;
    COBJ_UNSET_FLAG(obj, COBJ_FLAG_COMPLETE);
    obj->res = res;
    if (async) {
        COBJ_SET_FLAG(obj, COBJ_FLAG_ASYNC);
        obj->listeners = list_create();
        list_set_free_value_callback(obj->listeners, free);
    }
    MUTEX_INIT(&obj->lock);
}

static void *
arc_ops_fetch_copy_volatile_object_cb(void *ptr, size_t len, void *user)
{
    cached_object_t *obj = (cached_object_t *)user;
    volatile_object_t *item = (volatile_object_t *)ptr;
    if (item->dlen) {
        obj->data = (item->dlen > sizeof(obj->dbuf)) ? malloc(item->dlen) : obj->dbuf;
        memcpy(obj->data, item->data, item->dlen);
        obj->dlen = item->dlen;
    }
    return (void *)obj;
}

int
arc_ops_fetch(void *item, size_t *size, void * priv)
{
    cached_object_t *obj = (cached_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    MUTEX_LOCK(&obj->lock);

    if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_FETCHING)) {
        MUTEX_UNLOCK(&obj->lock);
        return 1;
    } else if (obj->data) {
        MUTEX_UNLOCK(&obj->lock);
        return 0;
    }

    COBJ_SET_FLAG(obj, COBJ_FLAG_FETCHING);

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_CACHE_MISSES].value);

    // this object is not evicted anymore (if it eventually was)
    COBJ_UNSET_FLAG(obj, COBJ_FLAG_EVICTED);
    COBJ_UNSET_FLAG(obj, COBJ_FLAG_EVICT);
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    // if we are not the owner try asking to the peer responsible for this data
    if (!shardcache_test_ownership(cache, obj->key, obj->klen, node_name, &node_len))
    {
        int done = 1;
        int ret = arc_ops_fetch_from_peer(cache, obj, node_name);
        if (ret == -1) {
            int check = shardcache_test_migration_ownership(cache,
                                                            obj->key,
                                                            obj->klen,
                                                            node_name,
                                                            &node_len);
            if (check == 0) {
                ret = arc_ops_fetch_from_peer(cache, obj, node_name);
            }

            if (check == 1 || (ret == -1 && cache->storage.global)) {
                // if it's a global storage or we are responsible in the
                // migration context, we don't want to return earlier
                SHC_WARNING("Can't fetch data from peer, falling back to the global storage");
                done = 0;
                COBJ_UNSET_FLAG(obj, COBJ_FLAG_EVICTED);
            }
        }
        if (done) {
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_FETCH_REMOTE].value);
            if (ret == 0) {
                ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHED_ITEMS].value, arc_count(cache->arc));
                gettimeofday(&obj->ts, NULL);
                *size = sizeof(cached_object_t) + ((obj->data == obj->dbuf) ? 0 : obj->dlen);
                int drop = COBJ_CHECK_FLAGS(obj, COBJ_FLAG_DROP|COBJ_FLAG_COMPLETE);
                MUTEX_UNLOCK(&obj->lock);
                ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHED_ITEMS].value, arc_count(cache->arc));
                return drop ? 1 : 0;
            }
            MUTEX_UNLOCK(&obj->lock);
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_ERRORS].value);
            return -1;
        }
    }

    char keystr[1024];
    if (shardcache_log_level() >= LOG_DEBUG)
        KEY2STR(obj->key, obj->klen, keystr, sizeof(keystr));

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_FETCH_LOCAL].value);

    // we are responsible for this item ... 
    // let's first check if it's among the volatile keys otherwise
    // fetch it from the storage
    ht_get_deep_copy(cache->volatile_storage,
                     obj->key,
                     obj->klen,
                     NULL,
                     arc_ops_fetch_copy_volatile_object_cb,
                     obj);
    if (obj->data && obj->dlen) {
        SHC_DEBUG3("Found volatile value %s (%lu) for key %s",
               shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE, 0),
               (unsigned long)obj->dlen, keystr);
    } else if (cache->use_persistent_storage && cache->storage.fetch) {
        int rc = cache->storage.fetch(obj->key, obj->klen, &obj->data, &obj->dlen, cache->storage.priv);
        if (rc == -1) {
            if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_ASYNC) && obj->listeners)
                list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
            SHC_ERROR("Fetch storage callback returned an error (%d)", rc);
            ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_ERRORS].value);
            COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);
            COBJ_SET_FLAG(obj, COBJ_FLAG_DROP);
            MUTEX_UNLOCK(&obj->lock);
            return -1;
        }
        if (obj->data && obj->dlen) {
            SHC_DEBUG3("Fetch storage callback returned value %s (%lu) for key %s",
                   shardcache_hex_escape(obj->data, obj->dlen, DEBUG_DUMP_MAXSIZE, 0),
                   (unsigned long)obj->dlen, keystr);
        } else {
            SHC_DEBUG3("Fetch storage callback returned an empty value for key %s", keystr);
        }
    }

    gettimeofday(&obj->ts, NULL);

    COBJ_SET_FLAG(obj, COBJ_FLAG_COMPLETE);
    COBJ_UNSET_FLAG(obj, COBJ_FLAG_FETCHING);

    if (!obj->data) {
        if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_ASYNC) && obj->listeners)
            list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_complete, obj);

        MUTEX_UNLOCK(&obj->lock);
        SHC_DEBUG("Item not found for key %s", keystr);
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_NOT_FOUND].value);
        return 1;
    }

    if (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_ASYNC)) {
        shardcache_fetch_from_peer_notify_arg arg = {
            .obj = obj,
            .data = obj->data,
            .len = obj->dlen
        };
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener, &arg);
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_complete, obj);
    }

    *size = sizeof(cached_object_t) + ((obj->data == obj->dbuf) ? 0 : obj->dlen);

    int evicted = (COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICT) ||
                   COBJ_CHECK_FLAGS(obj, COBJ_FLAG_EVICTED));

    if (cache->expire_time > 0 && !evicted && !cache->lazy_expiration) {
        shardcache_schedule_expiration(cache, obj->key, obj->klen, cache->expire_time, 0);
    }

    MUTEX_UNLOCK(&obj->lock);

    ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHED_ITEMS].value, arc_count(cache->arc));

    return evicted;
}

void
arc_ops_evict(void *item, void *priv)
{
    cached_object_t *obj = (cached_object_t *)item;
    shardcache_t *cache = (shardcache_t *)priv;

    MUTEX_LOCK(&obj->lock); // XXX - this shouldn't be really necessary
                            // TODO : try removing it and see what happens
                            //        during stress tests

    if (!cache->lazy_expiration)
        shardcache_unschedule_expiration(cache, obj->key, obj->klen, 0);

    if (obj->listeners) {
        // safety belts, just to ensure not leaking listeners by notifying them an error
        // before relasing the object completely
        // NOTE: there shouldn't ever be listeners here if the object is being released,
        //       but in case of race conditions or bugs which would end up registering
        //       listeners when they shouldn't, at least we notify back an error instead
        //       of making them wait forever
        list_foreach_value(obj->listeners, arc_ops_fetch_from_peer_notify_listener_error, obj);
        list_destroy(obj->listeners);
    }
    MUTEX_UNLOCK(&obj->lock);

    if (obj->data)
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_EVICTS].value);

    // no lock is necessary here ... if we are here
    // nobody is referencing us anymore
    if (obj->data && obj->data != obj->dbuf)
        free(obj->data);

    if (obj->key && obj->key != obj->kbuf)
        free(obj->key);

    MUTEX_DESTROY(&obj->lock);
    // NOTE : we don't need to free the memory used to store the actual cached_object_t
    // structure because it's managed by the arc subsystem, which provided us a pointer
    // to the prealloc'd memory as argument to the arc_ops_init() callback
}

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
