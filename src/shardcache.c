#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <signal.h>
#include <fcntl.h>
#include <limits.h>
#include <siphash.h>
#include <limits.h>
#include <regex.h>

#include "shardcache.h"
#include "shardcache_internal.h"
#include "arc_ops.h"
#include "connections.h"
#include "messaging.h"
#include "shardcache_replica.h"

const char *LIBSHARDCACHE_VERSION = "0.9.9";

extern int shardcache_log_initialized;

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"
static int
shardcache_check_address_string(char *str)
{
    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        fprintf(stderr, "Can't compile regexp %s: %s\n", ADDR_REGEXP, errbuf);
        return -1;
    }

    int matched = regexec(&addr_regexp, str, 0, NULL, 0);
    regfree(&addr_regexp);

    if (matched != 0) {
        return -1;
    }

    return 0;
}


shardcache_node_t *
shardcache_node_create_from_string(char *str)
{
    char *copy = strdup(str);
    char *string = copy;
    char *label = strsep(&string, ":");
    char *addrstring = string;
    char *addr = NULL;
    char **addrlist = NULL;
    int num_addresses = 0;

    while ((addr = strsep(&addrstring, ";")) != NULL) {
        if (!addr || shardcache_check_address_string(addr) != 0) {
            SHC_ERROR("Bad address format for peer: '%s'", addr);
            int i;
            for (i = 0; i < num_addresses; i++)
                free(addrlist[i]);
            free(addrlist);
            free(copy);
            return NULL;
        }
        addrlist = realloc(addrlist, num_addresses + 1);
        addrlist[num_addresses] = strdup(addr);
        num_addresses++;
    }

    shardcache_node_t *node = malloc(sizeof(shardcache_node_t));
    node->label = strdup(label);
    node->address = addrlist;
    node->string = strdup(str);
    node->num_replicas = num_addresses;

    free(copy);
    return node;
}

shardcache_node_t *
shardcache_node_create(char *label, char **addresses, int num_addresses)
{
    int i;
    shardcache_node_t *node = calloc(1, sizeof(shardcache_node_t));
    node->label = strdup(label);
    node->num_replicas = num_addresses;
    node->address = calloc(num_addresses, sizeof(char *));
    int slen = strlen(node->label) + 3;
    char *node_string = malloc(slen);
    snprintf(node_string, slen, "%s:", node->label);
    for (i = 0; i < num_addresses; i++) {
        if (i > 0)
            strcat(node_string, ";");
        slen += strlen(addresses[i]) + 1;
        node_string = realloc(node_string, slen);
        strcat(node_string, addresses[i]);
        node->address[i] = strdup(addresses[i]);
    }
    node->string = node_string;
    return node;
}

shardcache_node_t *
shardcache_node_copy(shardcache_node_t *node)
{
    int i;

    shardcache_node_t *copy = malloc(sizeof(shardcache_node_t));
    copy->label = strdup(node->label);
    copy->address = malloc(sizeof(char *) * node->num_replicas);
    for (i = 0; i < node->num_replicas; i++)
        copy->address[i] = strdup(node->address[i]);
    copy->num_replicas = node->num_replicas;
    copy->string = strdup(node->string);
    return copy;
}

void
shardcache_node_destroy(shardcache_node_t *node)
{
    int i;
    free(node->label);
    for (i = 0; i < node->num_replicas; i++)
        free(node->address[i]);
    free(node->string);
    free(node);
}

char *
shardcache_node_get_string(shardcache_node_t *node)
{
    return node->string;
}

char *
shardcache_node_get_label(shardcache_node_t *node)
{
    return node->label;
}

char *
shardcache_node_get_address(shardcache_node_t *node)
{
    return node->address[rand() % node->num_replicas];
}

shardcache_node_t *
shardcache_node_select(shardcache_t *cache, char *label)
{
    shardcache_node_t *node = NULL;
    int i;
    for (i = 0; i < cache->num_shards; i++ ){
        if (strcmp(cache->shards[i]->label, label) == 0) {
            node = cache->shards[i];
            break;
        }
    }
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration && !node) {
        for (i = 0; i < cache->num_migration_shards; i++) {
            if (strcmp(cache->migration_shards[i]->label, label) == 0) {
                node = cache->migration_shards[i];
                break;
            }
        }
    }
    SPIN_UNLOCK(&cache->migration_lock);
    return node;
}

int shardcache_node_num_addresses(shardcache_node_t *node)
{
    return node->num_replicas;

}

int shardcache_node_get_all_addresses(shardcache_node_t *node, char **addresses, int num_addresses)
{
    int i;
    for (i = 0; i < num_addresses && i < node->num_replicas; i++)
        addresses[i] = node->address[i];
    return node->num_replicas;
}

char *shardcache_node_get_address_at_index(shardcache_node_t *node, int index)
{
    if (index < node->num_replicas)
        return node->address[index];
    return NULL;
}


static int
shardcache_test_ownership_internal(shardcache_t *cache,
                                   void *key,
                                   size_t klen,
                                   char *owner,
                                   size_t *len,
                                   int  migration)
{
    const char *node_name;
    size_t name_len = 0;

    if (len && *len == 0)
        return -1;

    if (cache->num_shards == 1)
        return 1;

    SPIN_LOCK(&cache->migration_lock);

    chash_t *continuum = NULL;
    if (cache->migration && cache->migration_done) { 
        shardcache_migration_end(cache);
    } 

    if (migration) {
        if (cache->migration) {
            continuum = cache->migration;
        } else {
            SPIN_UNLOCK(&cache->migration_lock);
            return -1;
        }
    } else {
        continuum = cache->chash;
    }

    chash_lookup(continuum, key, klen, &node_name, &name_len);
    if (owner) {
        if (len && name_len + 1 > *len)
            name_len = *len - 1;
        memcpy(owner, node_name, name_len);
        owner[name_len] = 0;
    }
    if (len)
        *len = name_len;

    SPIN_UNLOCK(&cache->migration_lock);
    return (strcmp(owner, cache->me) == 0);
}

int
shardcache_test_migration_ownership(shardcache_t *cache,
                                    void *key,
                                    size_t klen,
                                    char *owner,
                                    size_t *len)
{
    int ret = shardcache_test_ownership_internal(cache, key, klen, owner, len, 1);
    return ret;
}

int
shardcache_test_ownership(shardcache_t *cache,
                          void *key,
                          size_t klen,
                          char *owner,
                          size_t *len)
{
    return shardcache_test_ownership_internal(cache, key, klen, owner, len, 0);
}

int
shardcache_get_connection_for_peer(shardcache_t *cache, char *peer)
{
    if (!cache->use_persistent_connections)
        return connect_to_peer(peer, cache->tcp_timeout);

    // this will reuse an available filedescriptor already connected to peer
    // or create a new connection if there isn't any available
    return connections_pool_get(cache->connections_pool, peer);
}

void
shardcache_release_connection_for_peer(shardcache_t *cache, char *peer, int fd)
{
    if (fd < 0)
        return;

    if (!cache->use_persistent_connections) {
        close(fd);
        return;
    }
    // put back the fildescriptor into the connection cache
    connections_pool_add(cache->connections_pool, peer, fd);
}

static void
shardcache_do_nothing(int sig)
{
    // do_nothing
}

typedef struct {
    void *key;
    size_t klen;
} shardcache_key_t;

typedef shardcache_key_t shardcache_evictor_job_t;

static void
destroy_evictor_job(shardcache_evictor_job_t *job)
{
    free(job->key);
    free(job);
}

static
shardcache_evictor_job_t *create_evictor_job(void *key, size_t klen)
{
    shardcache_evictor_job_t *job = malloc(sizeof(shardcache_evictor_job_t)); 
    job->key = malloc(klen);
    memcpy(job->key, key, klen);
    job->klen = klen; 
    return job;
}

static void *
evictor(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    linked_list_t *jobs = cache->evictor_jobs;

    while (!ATOMIC_READ(cache->evictor_quit))
    {
        shardcache_evictor_job_t *job = (shardcache_evictor_job_t *)shift_value(jobs);
        while (job) {
            char keystr[1024];
            KEY2STR(job->key, job->klen, keystr, sizeof(keystr));
            SHC_DEBUG2("Eviction job for key '%s' started", keystr);

            int i;
            for (i = 0; i < cache->num_shards; i++) {
                char *peer = cache->shards[i]->label;
                if (strcmp(peer, cache->me) != 0) {
                    SHC_DEBUG3("Sending Eviction command to %s", peer);
                    int rindex = rand()%cache->shards[i]->num_replicas;
                    int fd = shardcache_get_connection_for_peer(cache, cache->shards[i]->address[rindex]);
                    int rc = evict_from_peer(cache->shards[i]->address[rindex], (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, job->key, job->klen, fd);
                    if (rc != 0)
                        SHC_WARNING("evict_from_peer return %d for peer %s", rc, peer);
                    shardcache_release_connection_for_peer(cache, cache->shards[i]->address[rindex], fd);
                }
            }
            destroy_evictor_job(job);

            SHC_DEBUG2("Eviction job for key '%s' completed", keystr);

            job = (shardcache_evictor_job_t *)shift_value(jobs);
        }
        struct timeval now;
        int rc = 0;
        rc = gettimeofday(&now, NULL);
        if (rc == 0) {
            struct timespec abstime = { now.tv_sec + 1, now.tv_usec * 1000 };
            MUTEX_LOCK(&cache->evictor_lock);
            pthread_cond_timedwait(&cache->evictor_cond, &cache->evictor_lock, &abstime);
            MUTEX_UNLOCK(&cache->evictor_lock);
        } else {
            // TODO - Error messsages
        }
    }
    return NULL;
}

static void
destroy_volatile(volatile_object_t *obj)
{
    if (obj->data)
        free(obj->data);
    free(obj);
}

typedef struct {
    linked_list_t *list;
    time_t now;
    uint32_t next;
} expire_volatile_arg_t;

typedef shardcache_key_t expire_volatile_item_t;

static int
expire_volatile(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    expire_volatile_arg_t *arg = (expire_volatile_arg_t *)user;
    volatile_object_t *v = (volatile_object_t *)value;
    if (v->expire && v->expire < arg->now) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("Key %s expired", keystr);
        expire_volatile_item_t *item = malloc(sizeof(expire_volatile_item_t));
        item->key = malloc(klen);
        memcpy(item->key, key, klen);
        item->klen = klen;
        push_value(arg->list, item);
    } else if (v->expire && (!arg->next || v->expire < arg->next)) {
        arg->next = v->expire;
    }
    return 1;
}

void *
shardcache_expire_volatile_keys(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    while (!ATOMIC_READ(cache->expirer_quit))
    {
        time_t now = time(NULL);

        uint32_t next_expire = ATOMIC_READ(cache->next_expire);
        if (next_expire && now >= next_expire && ht_count(cache->volatile_storage)) {
            expire_volatile_arg_t arg = {
                .list = create_list(),
                .now = time(NULL),
                .next = 0
            };

            ht_foreach_pair(cache->volatile_storage, expire_volatile, &arg);

            ATOMIC_SET_IF(cache->next_expire, >, arg.next, uint32_t);

            expire_volatile_item_t *item = shift_value(arg.list);
            while (item) {
                void *prev_ptr = NULL;
                ht_delete(cache->volatile_storage, item->key, item->klen, &prev_ptr, NULL);
                if (prev_ptr) {
                    volatile_object_t *prev = (volatile_object_t *)prev_ptr;
                    ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                                    prev->dlen);
                    destroy_volatile(prev);
                }
                arc_remove(cache->arc, (const void *)item->key, item->klen);
                free(item->key);
                free(item);
                item = shift_value(arg.list);
            }

            destroy_list(arg.list);

        }
        sleep(1);
    }
    return NULL;
}

void *
shardcache_run_async(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;
    while (!ATOMIC_READ(cache->async_quit)) {
        struct timeval timeout = { 0, 20000 };
        iomux_run(cache->async_mux, &timeout);
    }
    return NULL;
}

shardcache_t *
shardcache_create(char *me,
                  shardcache_node_t **nodes,
                  int nnodes,
                  shardcache_storage_t *st,
                  char *secret,
                  int num_workers,
                  size_t cache_size)
{
    int i, n;
    size_t shard_lens[nnodes];
    char *shard_names[nnodes];

    shardcache_t *cache = calloc(1, sizeof(shardcache_t));

    cache->evict_on_delete = 1;
    cache->use_persistent_connections = 1;
    cache->tcp_timeout = SHARDCACHE_TCP_TIMEOUT_DEFAULT;

    SPIN_INIT(&cache->migration_lock);

    if (st) {
        memcpy(&cache->storage, st, sizeof(cache->storage));
        cache->use_persistent_storage = 1;
    } else {
        SHC_NOTICE("No storage callbacks provided,"
                   "using only the internal volatile storage");
        cache->use_persistent_storage = 0;
    }


    cache->me = strdup(me);

    cache->ops.create  = arc_ops_create;
    cache->ops.fetch   = arc_ops_fetch;
    cache->ops.evict   = arc_ops_evict;
    cache->ops.destroy = arc_ops_destroy;

    cache->ops.priv = cache;
    cache->shards = malloc(sizeof(shardcache_node_t *) * nnodes);
    for (i = 0; i < nnodes; i++) {
        shard_names[i] = nodes[i]->label;
        shard_lens[i] = strlen(shard_names[i]);
        cache->shards[i] = shardcache_node_create(nodes[i]->label,
                                                  nodes[i]->address,
                                                  nodes[i]->num_replicas);
        if (strcmp(nodes[i]->label, me) == 0) {

shardcache_replica_t *shardcache_replica_create(shardcache_t *shc,
                                                shardcache_node_t *node,
                                                char *me,
                                                char *wrkdir);
            for (n = 0; n < nodes[i]->num_replicas; n++) {
                int fd = open_socket(nodes[i]->address[n], 0);
                if (fd >= 0) {
                    cache->addr = strdup(nodes[i]->address[n]);
                }
               close(fd); 
            }
            if (nodes[i]->num_replicas > 1)
                cache->replica = shardcache_replica_create(cache, cache->shards[i], cache->addr, NULL);
        }
    }

    if (!cache->addr) {
        fprintf(stderr, "Can't find my address (%s) among the configured nodes\n", cache->me);
        shardcache_destroy(cache);
        return NULL;
    }

    cache->num_shards = nnodes;

    cache->chash = chash_create((const char **)shard_names, shard_lens, cache->num_shards, 200);

    cache->arc = arc_create(&cache->ops, cache_size);
    cache->arc_size = cache_size;

    // check if there is already signal handler registered on SIGPIPE
    struct sigaction sa;
    if (sigaction(SIGPIPE, NULL, &sa) != 0) {
        fprintf(stderr, "Can't check signal handlers: %s\n", strerror(errno)); 
        shardcache_destroy(cache);
        return NULL;
    }

    // if not we need to register one to handle writes/reads to disconnected sockets
    if (sa.sa_handler == NULL)
        signal(SIGPIPE, shardcache_do_nothing);

    if (secret && *secret) {
        cache->auth = calloc(1, 16);
        strncpy((char *)cache->auth, secret, 16);
    } 

    if (secret && *secret) {
        SHC_DEBUG("AUTH KEY (secret: %s) : %s\n", secret,
                  shardcache_hex_escape(cache->auth, SHARDCACHE_MSG_SIG_LEN, DEBUG_DUMP_MAXSIZE));
    }

    const char *counters_names[SHARDCACHE_NUM_COUNTERS] =
        { "gets", "sets", "dels", "heads", "evicts", "cache_misses",
          "fetch_remote", "fetch_local", "not_found",
          "volatile_table_size", "cache_size" };

    cache->counters = shardcache_init_counters();

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        cache->cnt[i].name = counters_names[i];
        shardcache_counter_add(cache->counters, cache->cnt[i].name, &cache->cnt[i].value); 
    }

    if (ATOMIC_READ(cache->evict_on_delete)) {
        MUTEX_INIT(&cache->evictor_lock);
        CONDITION_INIT(&cache->evictor_cond);
        cache->evictor_jobs = create_list();
        set_free_value_callback(cache->evictor_jobs,
                                (free_value_callback_t)destroy_evictor_job);
        pthread_create(&cache->evictor_th, NULL, evictor, cache);
    }

    srand(time(NULL));
    cache->volatile_storage = ht_create(1<<16, 1<<20, (ht_free_item_callback_t)destroy_volatile);

    cache->connections_pool = connections_pool_create(cache->tcp_timeout, num_workers + 1);

    cache->async_mux = iomux_create();
    iomux_set_threadsafe(cache->async_mux, 1);

    if (pthread_create(&cache->async_io_th, NULL, shardcache_run_async, cache) != 0) {
        fprintf(stderr, "Can't create the async i/o thread: %s\n", strerror(errno));
        shardcache_destroy(cache);
        return NULL;
    }

    cache->serv = start_serving(cache, cache->auth, cache->addr, num_workers, cache->counters); 
    if (!cache->serv) {
        fprintf(stderr, "Can't start the communication engine\n");
        shardcache_destroy(cache);
        return NULL;
    }

    pthread_create(&cache->expirer_th, NULL, shardcache_expire_volatile_keys, cache);
    cache->expirer_started = 1;

    if (!shardcache_log_initialized)
        shardcache_log_init("libshardcache", LOG_WARNING);

    return cache;
}

void
shardcache_destroy(shardcache_t *cache)
{
    int i;

    if (cache->serv)
        stop_serving(cache->serv);

    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        shardcache_migration_abort(cache);    
    }
    SPIN_UNLOCK(&cache->migration_lock);
    SPIN_DESTROY(&cache->migration_lock);

    if (ATOMIC_READ(cache->evict_on_delete)) {
        SHC_DEBUG2("Stopping evictor thread");
        ATOMIC_INCREMENT(cache->evictor_quit);
        pthread_join(cache->evictor_th, NULL);
        MUTEX_DESTROY(&cache->evictor_lock);
        CONDITION_DESTROY(&cache->evictor_cond);
        destroy_list(cache->evictor_jobs);
        SHC_DEBUG2("Evictor thread stopped");
    }

    if (cache->expirer_started) {
        SHC_DEBUG2("Stopping expirer thread");
        ATOMIC_INCREMENT(cache->expirer_quit);
        pthread_join(cache->expirer_th, NULL);
        SHC_DEBUG2("Expirer thread stopped");
    }

    SHC_DEBUG2("Stopping the async i/o thread");
    ATOMIC_INCREMENT(cache->async_quit);
    pthread_join(cache->async_io_th, NULL);
    iomux_destroy(cache->async_mux);
    SHC_DEBUG2("Async i/o thread stopped");

    if (cache->replica)
        shardcache_replica_destroy(cache->replica);

    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i ++) {
        shardcache_counter_remove(cache->counters, cache->cnt[i].name);
    }
    shardcache_release_counters(cache->counters);

    ht_destroy(cache->volatile_storage);

    if (cache->auth)
        free((void *)cache->auth);

    if (cache->arc)
        arc_destroy(cache->arc);

    if (cache->chash)
        chash_free(cache->chash);

    free(cache->me);
    if (cache->addr)
        free(cache->addr);
    shardcache_free_nodes(cache->shards, cache->num_shards);

    connections_pool_destroy(cache->connections_pool);

    free(cache);
    SHC_DEBUG("Shardcache node stopped");
}

size_t
shardcache_get_offset(shardcache_t *cache,
                      void *key,
                      size_t klen,
                      void *data,
                      size_t *dlen,
                      size_t offset,
                      struct timeval *timestamp)
{
    size_t vlen = 0;
    size_t copied = 0;
    if (!key)
        return 0;

    if (offset == 0)
        ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_GETS].value);

    void *obj_ptr = NULL;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 0);
    if (!res)
        return 0;

    if (obj_ptr) {
        cache_object_t *obj = (cache_object_t *)obj_ptr;
        MUTEX_LOCK(&obj->lock);
        if (obj->data) {
            if (dlen && data) {
                if (offset < obj->dlen) {
                    int size = obj->dlen - offset;
                    copied = size < *dlen ? size : *dlen;
                    memcpy(data, obj->data + offset, copied);
                    *dlen = copied;
                }
            }
            if (timestamp)
                memcpy(timestamp, &obj->ts, sizeof(struct timeval));
        }
        vlen = obj->dlen;
        MUTEX_UNLOCK(&obj->lock);
    }
    arc_release_resource(cache->arc, res);
    ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
              (uint32_t)arc_size(cache->arc));
    return (offset < vlen + copied) ? (vlen - offset - copied) : 0;
}


typedef struct {
    int stat;
    size_t dlen;
    arc_t *arc;
    arc_resource_t *res;
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_async_helper_arg_t;

static int
shardcache_get_async_helper(void *key,
                            size_t klen,
                            void *data,
                            size_t dlen,
                            size_t total_size,
                            struct timeval *timestamp,
                            void *priv)
{
    shardcache_get_async_helper_arg_t *arg = (shardcache_get_async_helper_arg_t *)priv;

    int rc = arg->cb(key, klen, data, dlen, total_size, timestamp, arg->priv);
    if (rc != 0 || (!dlen && !total_size)) { // error
        //arg->stat = -1;
        arc_release_resource(arg->arc, arg->res);
        free(arg);
        return -1;
    }

    arg->dlen += dlen;

    if (total_size) {
        arc_release_resource(arg->arc, arg->res);
        free(arg);
    }

    return 0;
}

int
shardcache_get_async(shardcache_t *cache,
                     void *key,
                     size_t klen,
                     shardcache_get_async_callback_t cb,
                     void *priv)
{
    if (!key)
        return -1;

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_GETS].value);

    void *obj_ptr;
    arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 1);

    if (!res)
        return -1;

    if (!obj_ptr) {
        arc_release_resource(cache->arc, res);
        return -1;
    }

    cache_object_t *obj = (cache_object_t *)obj_ptr;
    MUTEX_LOCK(&obj->lock);
    if (obj->evicted) {
        // if marked for eviction we don't want to return this object
        MUTEX_UNLOCK(&obj->lock);
        arc_release_resource(cache->arc, res);
        return -1;
    } else if (obj->complete) {
        cb(key, klen, obj->data, obj->dlen, obj->dlen, &obj->ts, priv);
        MUTEX_UNLOCK(&obj->lock);
        arc_release_resource(cache->arc, res);
    } else {
        if (obj->dlen) // let's send what we have so far
            cb(key, klen, obj->data, obj->dlen, 0, NULL, priv);

        shardcache_get_async_helper_arg_t *arg = calloc(1, sizeof(shardcache_get_async_helper_arg_t));
        arg->cb = cb;
        arg->priv = priv;
        arg->arc = cache->arc;
        arg->res = res;

        shardcache_get_listener_t *listener = malloc(sizeof(shardcache_get_listener_t));
        listener->cb = shardcache_get_async_helper;
        listener->priv = arg;
        push_value(obj->listeners, listener);
        MUTEX_UNLOCK(&obj->lock);
    }

    ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
              (uint32_t)arc_size(cache->arc));

    return 0;
}

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    fbuf_t data;
    int stat;
    struct timeval ts;
    int complete;
} shardcache_get_helper_arg_t;

static int
shardcache_get_helper(void *key,
                      size_t klen,
                      void *data,
                      size_t dlen,
                      size_t total_size,
                      struct timeval *timestamp,
                      void *priv)
{
    shardcache_get_helper_arg_t *arg = (shardcache_get_helper_arg_t *)priv;
    MUTEX_LOCK(&arg->lock);
    if (dlen) {
        fbuf_add_binary(&arg->data, data, dlen);
    } else if (!total_size) {
        // error notified (dlen == 0 && total_size == 0)
	arg->complete = 1;
        arg->stat = -1;
        pthread_cond_signal(&arg->cond);
        MUTEX_UNLOCK(&arg->lock);
        return -1;
    }

    if (total_size) {
        arg->complete = 1;
        if (timestamp)
            memcpy(&arg->ts, timestamp, sizeof(struct timeval));
        if (total_size != fbuf_used(&arg->data)) {
            arg->stat = -1;
        }
        pthread_cond_signal(&arg->cond);
    }
    MUTEX_UNLOCK(&arg->lock);
    return 0;
}

void *
shardcache_get(shardcache_t *cache,
               void *key,
               size_t klen,
               size_t *vlen,
               struct timeval *timestamp)
{
    if (!key)
        return NULL;

    shardcache_get_helper_arg_t arg = {

        .lock = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .data = FBUF_STATIC_INITIALIZER,
        .stat = 0,
        .ts = { 0, 0 },
        .complete = 0
    };

    char keystr[1024];
    if (shardcache_log_level() >= LOG_DEBUG)
        KEY2STR(key, klen, keystr, sizeof(keystr));

    SHC_DEBUG2("Getting value for key: %s", keystr);

    int rc = shardcache_get_async(cache, key, klen, shardcache_get_helper, &arg);

    if (rc == 0) {
        CONDITION_WAIT_WHILE(&arg.cond, &arg.lock, !arg.complete && arg.stat == 0);

         if (arg.stat != 0) {
             fbuf_destroy(&arg.data);
             SHC_ERROR("Error trying to get key: %s", keystr);
             return NULL;
         }

         char *value = fbuf_data(&arg.data);
         if (vlen)
             *vlen = fbuf_used(&arg.data);

         if (timestamp)
             memcpy(timestamp, &arg.ts, sizeof(struct timeval));

         if (!value)
             SHC_DEBUG("No value for key: %s", keystr);

         return value;
    }
    fbuf_destroy(&arg.data);
    return NULL;
}

size_t
shardcache_head(shardcache_t *cache,
                void *key,
                size_t len,
                void *head,
                size_t hlen,
                struct timeval *timestamp)
{
    if (!key)
        return 0;

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_HEADS].value);

    size_t rlen = hlen;
    size_t remainder =  shardcache_get_offset(cache, key, len, head, &rlen, 0, timestamp);
    return remainder + rlen;
}

int
shardcache_exists(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        if (!ht_exists(cache->volatile_storage, key, klen)) {
            if (cache->use_persistent_storage && cache->storage.exist) {
                if (!cache->storage.exist(key, klen, cache->storage.priv))
                    return 0;
            } else {
                return 0;
            }
        }
        return 1;
    } else {
        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name); 
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        int rc = exists_on_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, addr, fd);
        return rc;
    }

    return -1;
}

int
shardcache_touch(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        void *obj_ptr = NULL;
        arc_resource_t res = arc_lookup(cache->arc, (const void *)key, klen, &obj_ptr, 0);
        if (res) {
            cache_object_t *obj = (cache_object_t *)obj_ptr;
            MUTEX_LOCK(&obj->lock);
            gettimeofday(&obj->ts, NULL);
            MUTEX_UNLOCK(&obj->lock);
            arc_release_resource(cache->arc, res);
            return obj ? 0 : -1;
        }
    } else {
        shardcache_node_t *peer = shardcache_node_select(cache, node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        int rc = touch_on_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, addr, fd);
        return rc;
    }

    return -1;
}


static void
shardcache_commence_eviction(shardcache_t *cache, void *key, size_t klen)
{
    shardcache_evictor_job_t *job = create_evictor_job(key, klen);

    char keystr[1024];
    KEY2STR(key, klen, keystr, sizeof(keystr));
    SHC_DEBUG("Adding evictor job for key %s", keystr);

    push_value(cache->evictor_jobs, job);
    MUTEX_LOCK(&cache->evictor_lock);
    pthread_cond_signal(&cache->evictor_cond);
    MUTEX_UNLOCK(&cache->evictor_lock);
}


int
shardcache_set_internal(shardcache_t *cache,
                         void *key,
                         size_t klen,
                         void *value,
                         size_t vlen,
                         time_t expire,
                         int inx,
                         int replica)
{
    // if we are not the owner try propagating the command to the responsible peer
    
    if (!key || !value)
        return -1;

    char keystr[1024];
    KEY2STR(key, klen, keystr, sizeof(keystr));
    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_SETS].value);

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        int rc = -1;
        SHC_DEBUG2("Storing value %s (%d) for key %s",
                   shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE),
                   (int)vlen, keystr);

        volatile_object_t *prev = NULL;
        if (!cache->use_persistent_storage || expire)
        {
            // ensure removing this key from the persistent storage (if present)
            // since it's now going to be a volatile item
            if (cache->use_persistent_storage && cache->storage.remove)
                cache->storage.remove(key, klen, cache->storage.priv);

            if (inx && ht_exists(cache->volatile_storage, key, klen)) {
                SHC_DEBUG("A volatile value already exists for key %s", keystr);
                return 1;
            }

            volatile_object_t *obj = malloc(sizeof(volatile_object_t));
            obj->data = malloc(vlen);
            memcpy(obj->data, value, vlen);
            obj->dlen = vlen;
            obj->expire = expire;

            SHC_DEBUG2("Setting volatile item %s to expire %d (now: %d)", 
                keystr, obj->expire, (int)time(NULL));

            void *prev_ptr = NULL;
            if (inx) {
                rc = ht_set(cache->volatile_storage, key, klen,
                             obj, sizeof(volatile_object_t));
            } else {
                rc = ht_get_and_set(cache->volatile_storage, key, klen,
                                     obj, sizeof(volatile_object_t),
                                     &prev_ptr, NULL);
            }

            if (prev_ptr) {
                prev = (volatile_object_t *)prev_ptr;
                if (vlen > prev->dlen) {
                    ATOMIC_INCREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                                    vlen - prev->dlen);
                } else {
                    ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                                    prev->dlen - vlen);
                }
                destroy_volatile(prev); 
                arc_remove(cache->arc, (const void *)key, klen);
                if (!replica)
                    shardcache_commence_eviction(cache, key, klen);
            } else {
                ATOMIC_INCREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value, vlen);
            }

            if (obj->expire && !ATOMIC_CAS(cache->next_expire, 0, obj->expire))
                ATOMIC_SET_IF(cache->next_expire, >, obj->expire, uint32_t)
        }
        else if (cache->use_persistent_storage && cache->storage.store)
        {
            void *prev_ptr = NULL;
            if (inx) {
                if (ht_exists(cache->volatile_storage, key, klen) == 1) {
                    SHC_DEBUG("A volatile value already exists for key %s", keystr);
                    return 1;
                }
            } else {
                // remove this key from the volatile storage (if present)
                // it's going to be eventually persistent now (depending on the storage type)
                ht_delete(cache->volatile_storage, key, klen, prev_ptr, NULL);
            }

            if (prev_ptr) {
                prev = (volatile_object_t *)prev_ptr;
                ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value, prev->dlen);
                destroy_volatile(prev);
            }

            if (inx && cache->storage.exist &&
                    cache->storage.exist(key, klen, cache->storage.priv) == 1)
            {
                SHC_DEBUG("A value already exists for key %s", keystr);
                return 1;
            }

            rc = cache->storage.store(key, klen, value, vlen, cache->storage.priv);

            arc_remove(cache->arc, (const void *)key, klen);
            if (!replica)
                shardcache_commence_eviction(cache, key, klen);
        }
        return rc;
    }
    else if (node_len)
    {
        SHC_DEBUG("Forwarding set command %s => %s (%d) to %s",
                keystr, shardcache_hex_escape(value, vlen, DEBUG_DUMP_MAXSIZE),
                (int)vlen, node_name);

        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);

        int fd = shardcache_get_connection_for_peer(cache, addr);

        int rc = -1;
        if (inx)
            rc = add_to_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, expire, fd);
        else
            rc = send_to_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, expire, fd);

        shardcache_release_connection_for_peer(cache, addr, fd);

        if (rc == 0) {
            arc_remove(cache->arc, (const void *)key, klen);
            if (!replica)
                shardcache_commence_eviction(cache, key, klen);
        }
        return rc;
    }

    return -1;
}

int
shardcache_set_volatile(shardcache_t *cache,
                        void *key,
                        size_t klen,
                        void *value,
                        size_t vlen,
                        time_t expire)
{
    if (!key || !klen)
        return -1;

    time_t real_expire = expire ? time(NULL) + expire : 0;
    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_SET, key, klen, value, vlen, real_expire);

    return shardcache_set_internal(cache, key, klen, value, vlen, real_expire, 0, 0);
}

int
shardcache_add_volatile(shardcache_t *cache,
                        void *key,
                        size_t klen,
                        void *value,
                        size_t vlen,
                        time_t expire)
{
    if (!key || !klen)
        return -1;

    time_t real_expire = expire ? time(NULL) + expire : 0;
    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_ADD, key, klen, value, vlen, real_expire);

    return shardcache_set_internal(cache, key, klen, value, vlen, real_expire, 1, 0);
}

int
shardcache_set(shardcache_t *cache,
               void *key,
               size_t klen,
               void *value,
               size_t vlen)
{
    if (!key || !klen)
        return -1;

    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_SET, key, klen, value, vlen, 0);

    return shardcache_set_internal(cache, key, klen, value, vlen, 0, 0, 0);
}

int
shardcache_add(shardcache_t *cache,
               void *key,
               size_t klen,
               void *value,
               size_t vlen)
{
    if (!key || !klen)
        return -1;

    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_ADD, key, klen, value, vlen, 0);

    return shardcache_set_internal(cache, key, klen, value, vlen, 0, 1, 0);
}

int
shardcache_del_internal(shardcache_t *cache, void *key, size_t klen, int replica)
{
    if (!key)
        return -1;

    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_DELS].value);

    // if we are not the owner try propagating the command to the responsible peer
    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);

    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1)
        is_mine = shardcache_test_ownership(cache, key, klen, node_name, &node_len);

    if (is_mine == 1)
    {
        void *prev_ptr;
        int rc = ht_delete(cache->volatile_storage, key, klen, &prev_ptr, NULL);

        if (rc != 0) {
            if (cache->use_persistent_storage && cache->storage.remove)
                cache->storage.remove(key, klen, cache->storage.priv);
        } else if (prev_ptr) {
            volatile_object_t *prev_item = (volatile_object_t *)prev_ptr;
            ATOMIC_DECREASE(cache->cnt[SHARDCACHE_COUNTER_TABLE_SIZE].value,
                            prev_item->dlen);
            destroy_volatile(prev_item);
        }

        if (ATOMIC_READ(cache->evict_on_delete))
        {
            arc_remove(cache->arc, (const void *)key, klen);

            if (!replica)
                shardcache_commence_eviction(cache, key, klen);
        }

        ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
                   (uint32_t)arc_size(cache->arc));

        return 0;
    } else if (!replica) {
        shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
        if (!peer) {
            SHC_ERROR("Can't find address for node %s\n", peer);
            return -1;
        }
        char *addr = shardcache_node_get_address(peer);
        int fd = shardcache_get_connection_for_peer(cache, addr);
        int rc = delete_from_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, fd);
        shardcache_release_connection_for_peer(cache, addr, fd);
        return rc;
    }

    return -1;

}

int
shardcache_del(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !key)
        return -1;

    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_DELETE, key, klen, NULL, 0, 0);

    return shardcache_del_internal(cache, key, klen, 0);
}

int
shardcache_evict(shardcache_t *cache, void *key, size_t klen)
{
    if (!key || !klen)
        return -1;

    if (cache->replica)
        return shardcache_replica_dispatch(cache->replica, SHARDCACHE_REPLICA_OP_EVICT, key, klen, NULL, 0, 0);

    arc_remove(cache->arc, (const void *)key, klen);
    ATOMIC_INCREMENT(cache->cnt[SHARDCACHE_COUNTER_EVICTS].value);

    ATOMIC_SET(cache->cnt[SHARDCACHE_COUNTER_CACHE_SIZE].value,
               (uint32_t)arc_size(cache->arc));

    return 0;
}

shardcache_node_t **
shardcache_get_nodes(shardcache_t *cache, int *num_nodes)
{
    int i;
    int num = 0;
    SPIN_LOCK(&cache->migration_lock);
    num = cache->num_shards;
    if (num_nodes)
        *num_nodes = num;
    shardcache_node_t **list = malloc(sizeof(shardcache_node_t *) * num);
    for (i = 0; i < num; i++) {
        shardcache_node_t *orig = cache->shards[i];
        list[i] = shardcache_node_create(orig->label, orig->address, orig->num_replicas);
    }
    SPIN_UNLOCK(&cache->migration_lock);
    return list;
}

void
shardcache_free_nodes(shardcache_node_t **nodes, int num_nodes)
{
    int i;
    for (i = 0; i < num_nodes; i++)
        shardcache_node_destroy(nodes[i]);
    free(nodes);
}

int
shardcache_get_counters(shardcache_t *cache, shardcache_counter_t **counters)
{
    return shardcache_get_all_counters(cache->counters, counters); 
}

void
shardcache_clear_counters(shardcache_t *cache)
{
    int i;
    for (i = 0; i < SHARDCACHE_NUM_COUNTERS; i++)
        ATOMIC_SET(cache->cnt[i].value, 0);
}

shardcache_storage_index_t *
shardcache_get_index(shardcache_t *cache)
{
    shardcache_storage_index_t *index = NULL;

    if (cache->use_persistent_storage) {
        size_t isize = 65535;
        if (cache->storage.count)
            isize = cache->storage.count(cache->storage.priv);

        ssize_t count = 0;
        shardcache_storage_index_item_t *items = NULL;
        if (cache->storage.index) {
            items = calloc(sizeof(shardcache_storage_index_item_t), isize);
            count = cache->storage.index(items, isize, cache->storage.priv);
        }
        index = calloc(1, sizeof(shardcache_storage_index_t));
        index->items = items;
        index->size = count; 
    }
    return index;
}

void
shardcache_free_index(shardcache_storage_index_t *index)
{
    if (index->items) {
        int i;
        for (i = 0; i < index->size; i++) {
            if (index->items[i].key)
                free(index->items[i].key);
        }
        free(index->items);
    }
    free(index);
}

static int
expire_migrated(hashtable_t *table, void *key, size_t klen, void *value, size_t vlen, void *user)
{
    shardcache_t *cache = (shardcache_t *)user;
    volatile_object_t *v = (volatile_object_t *)value;

    char node_name[1024];
    size_t node_len = sizeof(node_name);
    memset(node_name, 0, node_len);
    int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
    if (is_mine == -1) {
        fprintf(stderr, "expire_migrated running while no migration continuum present ... aborting\n");
        return 0;
    } else if (!is_mine) {
        char keystr[1024];
        KEY2STR(key, klen, keystr, sizeof(keystr));
        SHC_DEBUG("Forcing Key %s to expire because not owned anymore", keystr);

        v->expire = 0;
    }
    return 1;
}


void *
migrate(void *priv)
{
    shardcache_t *cache = (shardcache_t *)priv;

    shardcache_storage_index_t *index = shardcache_get_index(cache);
    int aborted = 0;
    linked_list_t *to_delete = create_list();

    uint32_t migrated_items = 0;
    uint32_t scanned_items = 0;
    uint32_t errors = 0;
    uint32_t total_items = 0;

    if (index) {
        total_items = index->size;

        shardcache_counter_add(cache->counters, "migrated_items", &migrated_items);
        shardcache_counter_add(cache->counters, "scanned_items", &scanned_items);
        shardcache_counter_add(cache->counters, "total_items", &total_items);
        shardcache_counter_add(cache->counters, "migration_errors", &errors);

        SHC_INFO("Migrator starting (%d items to precess)", total_items);

        int i;
        for (i = 0; i < index->size; i++) {
            size_t klen = index->items[i].klen;
            void *key = index->items[i].key;

            char node_name[1024];
            size_t node_len = sizeof(node_name);
            memset(node_name, 0, node_len);

            char keystr[1024];
            KEY2STR(key, klen, keystr, sizeof(keystr));

            SHC_DEBUG("Migrator processign key %s", keystr);

            int is_mine = shardcache_test_migration_ownership(cache, key, klen, node_name, &node_len);
            
            if (is_mine == -1) {
                fprintf(stderr, "Migrator running while no migration continuum present ... aborting\n");
                ATOMIC_INCREMENT(errors);
                aborted = 1;
                break;
            } else if (!is_mine) {
                // if we are not the owner try asking our peer responsible for this data
                void *value = NULL;
                size_t vlen = 0;
                if (cache->storage.fetch) {
                    value = cache->storage.fetch(key, klen, &vlen, cache->storage.priv);
                }
                if (value) {
                    shardcache_node_t *peer = shardcache_node_select(cache, (char *)node_name);
                    if (peer) {
                        char *addr = shardcache_node_get_address(peer);
                        SHC_DEBUG("Migrator copying %s to peer %s (%s)", keystr, node_name, addr);
                        int fd = shardcache_get_connection_for_peer(cache, addr);
                        int rc = send_to_peer(addr, (char *)cache->auth, SHC_HDR_SIGNATURE_SIP, key, klen, value, vlen, 0, fd);
                        shardcache_release_connection_for_peer(cache, addr, fd);
                        if (rc == 0) {
                            ATOMIC_INCREMENT(migrated_items);
                            push_value(to_delete, &index->items[i]);
                        } else {
                            fprintf(stderr, "Errors copying %s to peer %s (%s)\n", keystr, node_name, addr);
                            ATOMIC_INCREMENT(errors);
                        }
                    } else {
                        fprintf(stderr, "Can't find address for peer %s (me : %s)\n", node_name, cache->me);
                        ATOMIC_INCREMENT(errors);
                    }
                }
            }
            ATOMIC_INCREMENT(scanned_items);
        }

        shardcache_counter_remove(cache->counters, "migrated_items");
        shardcache_counter_remove(cache->counters, "scanned_items");
        shardcache_counter_remove(cache->counters, "total_items");
        shardcache_counter_remove(cache->counters, "migration_errors");
    }

    if (!aborted) {
            SHC_INFO("Migration completed, now removing not-owned  items");
        shardcache_storage_index_item_t *item = shift_value(to_delete);
        while (item) {
            if (cache->storage.remove)
                cache->storage.remove(item->key, item->klen, cache->storage.priv);

            char ikeystr[1024];
            KEY2STR(item->key, item->klen, ikeystr, sizeof(ikeystr));
            SHC_DEBUG2("removed item %s", ikeystr);

            item = shift_value(to_delete);
        }

        // and now let's expire all the volatile keys that don't belong to us anymore
        ht_foreach_pair(cache->volatile_storage, expire_migrated, cache);
        //ATOMIC_SET(cache->next_expire, 0);
    }

    destroy_list(to_delete);

    SPIN_LOCK(&cache->migration_lock);
    cache->migration_done = 1;
    SPIN_UNLOCK(&cache->migration_lock);
    if (index) {
        SHC_INFO("Migrator ended: processed %d items, migrated %d, errors %d",
                total_items, migrated_items, errors);
    }

    if (index)
        shardcache_free_index(index);

    return NULL;
}

int
shardcache_migration_begin(shardcache_t *cache,
                           shardcache_node_t **nodes,
                           int num_nodes,
                           int forward)
{
    int i, n;

    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        // already in a migration, ignore this command
        SPIN_UNLOCK(&cache->migration_lock);
        return -1;
    }

    SHC_NOTICE("Starting migration");

    size_t shard_lens[num_nodes];
    char *shard_names[num_nodes];

    fbuf_t mgb_message = FBUF_STATIC_INITIALIZER;

    for (i = 0; i < num_nodes; i++) {
        shard_names[i] = nodes[i]->label;
        shard_lens[i] = strlen(shard_names[i]);
        if (i > 0) 
            fbuf_add(&mgb_message, ",");
        int rindex = rand()%nodes[i]->num_replicas;
        fbuf_printf(&mgb_message, "%s:%s", nodes[i]->label, nodes[i]->address[rindex]);
    }


    int ignore = 0;

    if (num_nodes == cache->num_shards) {
        // let's assume the lists are the same, if not
        // ignore will be set again to 0
        ignore = 1;
        for (i = 0 ; i < num_nodes; i++) {
            int found = 0;
            for (n = 0; n < num_nodes; n++) {
                if (strcmp(nodes[i]->label, cache->shards[n]->label) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                // the lists differ, we don't want to ignore the request
                ignore = 0;
                break;
            }
        }
    }

    if (!ignore) {
        cache->migration_done = 0;
        cache->migration_shards = malloc(sizeof(shardcache_node_t *) * num_nodes);
        cache->num_migration_shards = num_nodes;
        for (i = 0; i < cache->num_migration_shards; i++) {
            shardcache_node_t *node = nodes[i];
            cache->migration_shards[i] = shardcache_node_create(node->label, node->address, node->num_replicas);
        }

        cache->migration = chash_create((const char **)shard_names,
                                        shard_lens,
                                        num_nodes,
                                        200);

        pthread_create(&cache->migrate_th, NULL, migrate, cache);
    }

    SPIN_UNLOCK(&cache->migration_lock);

    if (forward) {
        for (i = 0; i < cache->num_shards; i++) {
            if (strcmp(cache->shards[i]->label, cache->me) != 0) {
                int rindex = rand()%cache->shards[i]->num_replicas;
                int fd = shardcache_get_connection_for_peer(cache, cache->shards[i]->address[rindex]);
                int rc = migrate_peer(cache->shards[i]->address[rindex],
                                      (char *)cache->auth,
                                      SHC_HDR_SIGNATURE_SIP,
                                      fbuf_data(&mgb_message),
                                      fbuf_used(&mgb_message), fd);
                shardcache_release_connection_for_peer(cache, cache->shards[i]->address[rindex], fd);
                if (rc != 0) {
                    fprintf(stderr, "Node %s (%s) didn't aknowledge the migration\n",
                            cache->shards[i]->label, cache->shards[i]->address[rindex]);
                }
            }
        }
    }
    fbuf_destroy(&mgb_message);

    return 0;
}

int
shardcache_migration_abort(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->migration);
        free(cache->migration_shards);
        SHC_NOTICE("Migration aborted");
        ret = 0;
    }
    cache->migration = NULL;
    cache->migration_shards = NULL;
    cache->num_migration_shards = 0;

    SPIN_UNLOCK(&cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int
shardcache_migration_end(shardcache_t *cache)
{
    int ret = -1;
    SPIN_LOCK(&cache->migration_lock);
    if (cache->migration) {
        chash_free(cache->chash);
        shardcache_free_nodes(cache->shards, cache->num_shards);
        cache->chash = cache->migration;
        cache->shards = cache->migration_shards;
        cache->num_shards = cache->num_migration_shards;
        cache->migration = NULL;
        cache->migration_shards = NULL;
        cache->num_migration_shards = 0;
        SHC_NOTICE("Migration ended");
        ret = 0;
    }
    cache->migration_done = 0;
    SPIN_UNLOCK(&cache->migration_lock);
    pthread_join(cache->migrate_th, NULL);
    return ret;
}

int
shardcache_use_persistent_connections(shardcache_t *cache, int new_value)
{
    int old_value = ATOMIC_READ(cache->use_persistent_connections);

    if (new_value >= 0)
        ATOMIC_SET(cache->use_persistent_connections, new_value);

    return old_value;
}

int
shardcache_evict_on_delete(shardcache_t *cache, int new_value)
{
    int old_value = ATOMIC_READ(cache->evict_on_delete);

    if (new_value >= 0)
        ATOMIC_SET(cache->evict_on_delete, new_value);

    return old_value;
}

int
shardcache_tcp_timeout(shardcache_t *cache, int new_value)
{
    return connections_pool_tcp_timeout(cache->connections_pool, new_value);
}

