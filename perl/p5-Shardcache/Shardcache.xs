#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <shardcache.h>

#include "const-c.inc"

#include <pthread.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

static PerlInterpreter *orig_perl = NULL;

static inline int __acquire_lock() {
    int rc;
    int retries = 0;
    rc = pthread_mutex_trylock(&lock);
    while(rc != 0) {
        if (retries++ == 100) {
            return -1;
        }
        struct timespec tv = { 0, 10000000 }; // 10 ms
        struct timespec remainder = { 0, 0 };
        int ret;
        do {
            ret = nanosleep(&tv, &remainder);
            if (ret == -1) {
                memcpy(&tv, &remainder, sizeof(struct timespec));
                memset(&remainder, 0, sizeof(struct timespec));
            }
        } while (ret != 0);
        rc = pthread_mutex_trylock(&lock);
    }
    return 0;
}

static size_t __st_index(shardcache_storage_index_item_t *index, size_t isize, void *priv) {

    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_fetch can't acquire lock\n");
        return 0;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;

    ENTER;
    SAVETMPS;

    SV *storage = (SV *)priv;
    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    PUTBACK;

    int count = call_method("index", G_SCALAR);

    if (count != 1) {
        croak("Unexpected errors calling the 'fetch' method on the storage object");
    }

    SPAGAIN;

    SV *indexsv = POPs;
    if (!SvROK(indexsv) || SvTYPE(SvRV(indexsv)) != SVt_PVHV)
      croak("Expected an hash reference as return value from storage->index()");

    HV *index_hash = (HV *)SvRV(indexsv);
    I32 rc = hv_iterinit(index_hash);
    char *key; 
    I32 klen;
    SV *val = hv_iternextsv(index_hash, &key, &klen);
    int icnt = 0;
    while (val) {
        if (icnt >= isize)
            break;
        STRLEN vlen;
        shardcache_storage_index_item_t *index = &index[icnt];
        index->key = malloc(klen);
        memcpy(index->key, key, klen);
        index->klen = klen;
        SvPVbyte(val, vlen);
        index->vlen = vlen;
        icnt++;
        val = hv_iternextsv(index_hash, &key, &klen);
    }

    PUTBACK;
    FREETMPS;
    LEAVE;

    pthread_mutex_unlock(&lock);
    
    return icnt;
}


static size_t __st_count(void *priv) {
    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_fetch can't acquire lock\n");
        return 0;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;

    ENTER;
    SAVETMPS;

    SV *storage = (SV *)priv;
    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    PUTBACK;

    int count = call_method("count", G_SCALAR);

    if (count != 1) {
        croak("Unexpected errors calling the 'fetch' method on the storage object");
    }

    SPAGAIN;

    IV ret = POPi;

    PUTBACK;
    FREETMPS;
    LEAVE;

    pthread_mutex_unlock(&lock);
    
    return ret;
}

static void *__st_fetch(void *key, size_t len, size_t *vlen, void *priv) {
    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_fetch can't acquire lock\n");
        return NULL;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;

    ENTER;
    SAVETMPS;

    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    PUTBACK;

    int count = call_method("fetch", G_SCALAR);

    if (count != 1) {
        croak("Unexpected errors calling the 'fetch' method on the storage object");
    }

    SPAGAIN;

    SV *valref = POPs;
    char * out = NULL;

    if (SvOK(valref)) {
        if (!SvROK(valref))
          croak("Expected a scalar reference as return value from a fetch command");

        SV *val = (SV *)SvRV(valref);
        if (SvOK(val)) {
            if (SvTYPE(val) != SVt_PV) {
                croak("Not a scalar value");
            }
            STRLEN l;
            char *v = SvPVbyte(val, l);
            // make a copy to return, the caller will take care of releasing it
            out = malloc(l);
            memcpy(out, v, l);
            
            if (vlen)
                *vlen = l; 
        }
    }

    PUTBACK;
    FREETMPS;
    LEAVE;

    pthread_mutex_unlock(&lock);
    
    return (void *)out;
}

static int __st_store(void *key, size_t len, void *value, size_t vlen, void *priv) {
    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_store can't acquire lock\n");
        return -1;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;
    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);
    SV *v = newSVpv(value, vlen);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    XPUSHs(sv_2mortal(v));
    PUTBACK;

    call_method("store", G_DISCARD);
    pthread_mutex_unlock(&lock);
    return 0;
}

static int __st_remove(void *key, size_t len, void *priv) {
    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_remove can't acquire lock\n");
        return -1;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;
    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    PUTBACK;

    call_method("remove", G_DISCARD);
    pthread_mutex_unlock(&lock);
    return 0;
}

static int __st_exist(void *key, size_t len, void *priv) {
    if (__acquire_lock() != 0) {
        fprintf(stderr, "__st_exist can't acquire lock\n");
        return -1;
    }
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;
    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
        croak("missing storage or not of class 'Shardcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    PUTBACK;

    int count = call_method("exist", G_SCALAR);

    if (count != 1) {
        croak("Unexpected errors calling the registered runloop callback");
    }

    SPAGAIN;

    IV ret = POPi;

    PUTBACK;
    FREETMPS;
    LEAVE;



    pthread_mutex_unlock(&lock);

    return ret;
}

MODULE = Shardcache		PACKAGE = Shardcache		

INCLUDE: const-xs.inc

shardcache_t *
shardcache_create(me, nodes, storage, secret, num_workers, evict_on_delete = 1)
        char *  me
        SV   *  nodes
        SV   *  storage
        char *  secret
        int     num_workers
        int     evict_on_delete
    PREINIT:
        int i;
	int	num_nodes = 0;
        shardcache_node_t *shards = NULL;
    CODE:
        orig_perl = my_perl;
        if (!sv_isobject(storage) || !sv_derived_from(storage, "Shardcache::Storage")) {
            croak("missing storage or not of class 'Shardcache::Storage'");
        }
        
        if (SvOK(nodes)) {
            if (!SvROK(nodes) || SvTYPE(SvRV(nodes)) != SVt_PVAV)
              croak("Expected an array reference as 'nodes' parameter");

            AV *nodes_array = (AV *)SvRV(nodes);

            num_nodes = av_len(nodes_array) + 1;
            if (num_nodes > 0) {
                Newx(shards, sizeof(shardcache_node_t) * num_nodes + 1, shardcache_node_t);
                for (i = 0; i < num_nodes; i++) {
                    STRLEN len = 0, alen = 0;
                    char *node = NULL, *addr = NULL;

                    SV **svp = av_fetch(nodes_array, i, 0);
                    if (svp) {
                        if (SvROK(*svp) && SvTYPE(SvRV(*svp)) != SVt_PVAV) {
                            AV *pa = (AV *)SvRV(*svp);
                            SV **nodep = av_fetch(pa, i, 0);
                            node = SvPVbyte(*nodep, len);
                            if (av_len(pa)) {
                                SV **addrp = av_fetch(pa, i, 1);
                                addr = SvPVbyte(*addrp, alen);
                            } else {
                                addr = node;
                                alen = len;
                            }
                        } else {
                            node = SvPVbyte(*svp, len);
                            addr = node;
                            alen = len;
                        }
                    }
                    if (node && addr) {
                        int l = MIN(len, sizeof(shards[i].label)-1);
                        memcpy(shards[i].label, node, l);
                        shards[i].label[l] = 0;
                        int l2 = MIN(alen, sizeof(shards[i].address)-1);
                        memcpy(shards[i].address, addr, l2);
                        shards[i].address[l2] = 0;
                    } else {
                        croak("Can't parse the 'nodes' array");
                    }
                }
            } else {
                croak("No items found in the nodes array");
            }
        } else {
            croak("No nodes configured");
        }

        shardcache_storage_t storage_struct = {
            .fetch   = __st_fetch,
            .store   = __st_store,
            .remove  = __st_remove,
            .exist   = __st_exist,
            .index   = __st_index,
            .count   = __st_count,
            .priv    = SvREFCNT_inc(storage)
        };

        RETVAL = shardcache_create(me, shards, num_nodes, &storage_struct,
                                   secret, num_workers, evict_on_delete);
        if (shards)
            Safefree(shards);

        if (RETVAL == NULL)
            croak("Unknown error");

    OUTPUT:
        RETVAL

int
shardcache_del(cache, key, klen)
	shardcache_t *	cache
	char *	key
	size_t	klen
    CODE:
        int should_lock = 0;
        if (pthread_mutex_trylock(&lock) == EDEADLK)
            should_lock = 1;
        pthread_mutex_unlock(&lock);
        int ret = shardcache_del(cache, key, klen);
        if (should_lock)
            pthread_mutex_lock(&lock);
        RETVAL = ret;
    OUTPUT:
        RETVAL

void
shardcache_destroy(cache)
	shardcache_t *	cache

SV *
shardcache_get(cache, key, klen)
	shardcache_t *	cache
	char *	key
	size_t	klen
    PREINIT:
        size_t vlen = 0;
        void *v = NULL;
    CODE:
        RETVAL = &PL_sv_undef;
        int should_lock = 0;
        if (pthread_mutex_trylock(&lock) == EDEADLK)
            should_lock = 1;
        pthread_mutex_unlock(&lock);
        v = shardcache_get(cache, key, klen, &vlen);
        if (v) {
            char *out = malloc(vlen);
            memcpy(out, v, vlen);
            RETVAL = newSVpv(out, vlen);
            free(v);
        }
        if (should_lock)
            pthread_mutex_lock(&lock);
    OUTPUT:
        RETVAL

AV *
shardcache_get_nodes(cache, num_nodes)
	shardcache_t *	cache
	int *	num_nodes
    PREINIT:
        int i;
        AV *nodes;
    CODE:
        nodes = newAV();
        const shardcache_node_t *list = shardcache_get_nodes(cache, num_nodes);
        for (i = 0; i < *num_nodes; i++) {
            SV *node = newSVpv(list[i].label, strlen(list[i].label));
            av_push(nodes, sv_2mortal(node));
        }
        RETVAL = nodes;
    OUTPUT:
        RETVAL

int
shardcache_set(cache, key, klen, value, vlen)
	shardcache_t *	cache
	char *	key
	size_t	klen
	char *	value
	size_t	vlen
    CODE:
        int should_lock = 0;
        if (pthread_mutex_trylock(&lock) == EDEADLK)
            should_lock = 1;
        pthread_mutex_unlock(&lock);
        int ret = shardcache_set(cache, key, klen, value, vlen);
        if (should_lock)
            pthread_mutex_lock(&lock);
        RETVAL = ret;
    OUTPUT:
        RETVAL

SV *
shardcache_test_ownership(cache, key, len)
	shardcache_t *	cache
	char *	key
	size_t	len
    PREINIT:
        char node[1024];
        size_t plen = sizeof(node);
    CODE:
        shardcache_test_ownership(cache, key, len, (char *)&node, &plen);
        if (plen) {
            RETVAL = newSVpv(node, plen);
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL

int 
shardcache_evict(cache, key, klen)
        shardcache_t *cache
        char *key
        size_t klen

void
shardcache_run(coderef, timeout=1000, priv=&PL_sv_undef)
        SV *coderef
        IV timeout
        SV *priv
    CODE:
        struct timespec tv = { 1, 0 };
        if (!SvTRUE(coderef) || ! SvROK(coderef) || SvTYPE(SvRV(coderef)) != SVt_PVCV) {
            croak("missing coderef or not a CODE reference");
        }

        int secs = timeout/1000;
        int nsecs = (timeout%1000)*1000000;
        tv.tv_sec = secs;
        tv.tv_nsec = nsecs;

        /*
        PerlInterpreter *slave_perl = perl_clone(my_perl, CLONEf_KEEP_PTR_TABLE|CLONEf_COPY_STACKS);
        PERL_SET_CONTEXT(slave_perl);
        */
        for(;;) {
            struct timespec remainder = { 0, 0 };
            struct timespec tosleep = { tv.tv_sec, tv.tv_nsec };

            pthread_mutex_lock(&lock);

            dTHX;
            dSP;

            ENTER;
            SAVETMPS;

            PUSHMARK(SP);
            XPUSHs(priv);
            PUTBACK;

            int count = call_sv(coderef, G_SCALAR|G_EVAL);

            if (count != 1) {
                croak("Unexpected errors calling the registered runloop callback");
            }

            SPAGAIN;

            IV ret = POPi;

            PUTBACK;
            FREETMPS;
            LEAVE;

            pthread_mutex_unlock(&lock);

            if (ret == -1)  {
                // TODO - WARN
                break;
            }

            int rc;
            do {
                rc = nanosleep(&tosleep, &remainder);
                if (rc == -1) {
                    fprintf(stderr, "nanosleep exited: %s\n", strerror(errno));
                    memcpy(&tosleep, &remainder, sizeof(struct timespec));
                    memset(&remainder, 0, sizeof(struct timespec));
                }
            } while (rc != 0);
        }
        /*
        perl_free(slave_perl);
        PERL_SET_CONTEXT(orig_perl);        
        */

