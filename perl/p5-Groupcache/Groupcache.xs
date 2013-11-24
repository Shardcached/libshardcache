#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <groupcache.h>

#include "const-c.inc"

#include <pthread.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static PerlInterpreter *orig_perl = NULL;

static void *__st_fetch(void *key, size_t len, size_t *vlen, void *priv) {
    pthread_mutex_lock(&lock);
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;

    ENTER;
    SAVETMPS;

    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Groupcache::Storage")) {
        croak("missing storage or not of class 'Groupcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    PUTBACK;

    int count = call_method("fetch", G_SCALAR);

    SPAGAIN;

    if (count != 1) {
        croak("Unexpected errors calling the 'fetch' method on the storage object");
    }

    SV *val = POPs;
    char * out = NULL;

    if (SvOK(val)) {
        STRLEN l;
        char *str = SvPVbyte(val, l);
        
        if (l) {
            Newx(out, l, char);
            memcpy(out, str, l);

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

static void __st_store(void *key, size_t len, void *value, size_t vlen, void *priv) {
    pthread_mutex_lock(&lock);
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;
    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);
    SV *v = newSVpv(value, vlen);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Groupcache::Storage")) {
        croak("missing storage or not of class 'Groupcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    XPUSHs(sv_2mortal(v));
    PUTBACK;

    call_method("store", G_DISCARD);
    pthread_mutex_unlock(&lock);
}

static void __st_remove(void *key, size_t len, void *priv) {
    pthread_mutex_lock(&lock);
    PERL_SET_CONTEXT(orig_perl);
    dTHX;
    dSP;
    SV *storage = (SV *)priv;
    SV *k = newSVpv(key, len);

    if (!sv_isobject(storage) || !sv_derived_from(storage, "Groupcache::Storage")) {
        croak("missing storage or not of class 'Groupcache::Storage'");
    }

    PUSHMARK(SP);
    XPUSHs(storage);
    XPUSHs(sv_2mortal(k));
    PUTBACK;

    call_method("remove", G_DISCARD);
    pthread_mutex_unlock(&lock);
}

static void __st_free(void *val) {
    Safefree(val);
}

MODULE = Groupcache		PACKAGE = Groupcache		

INCLUDE: const-xs.inc

groupcache_t *
groupcache_create(me, peers, storage)
        char *  me
        SV   *  peers
        SV   *  storage
    PREINIT:
        int i;
	int	num_peers = 0;
        char ** shards = NULL;
    CODE:
        orig_perl = my_perl;
        if (!sv_isobject(storage) || !sv_derived_from(storage, "Groupcache::Storage")) {
            croak("missing storage or not of class 'Groupcache::Storage'");
        }
        
        if (peers) {
            if (!SvROK(peers) || SvTYPE(SvRV(peers)) != SVt_PVAV)
              croak("Expected an array reference as 'peers' parameter");

            AV *peers_array = (AV *)SvRV(peers);

            num_peers = av_len(peers_array) + 1;
            if (num_peers > 0) {
                Newx(shards, sizeof(char *) * num_peers + 1, char *);
                for (i = 0; i < num_peers; i++) {
                    STRLEN len;
                    char *peer;
                    SV **svp = av_fetch(peers_array, i, 0);
                    if (svp == NULL) {
                        // WRONG
                        len = 0;
                        peer = NULL;
                    } else {
                        peer = SvPVbyte(*svp, len);
                    }
                    shards[i] = peer;
                }
            }
        }

        groupcache_storage_t storage_struct = {
            .fetch  = __st_fetch,
            .store  = __st_store,
            .remove = __st_remove,
            .free   = __st_free,
            .priv   = SvREFCNT_inc(storage)
        };

        RETVAL = groupcache_create(me, shards, num_peers, &storage_struct);

        if (RETVAL == NULL)
            croak("Unknown error");

    OUTPUT:
        RETVAL

int
groupcache_del(cache, key, klen)
	groupcache_t *	cache
	char *	key
	size_t	klen

void
groupcache_destroy(cache)
	groupcache_t *	cache

SV *
groupcache_get(cache, key, klen)
	groupcache_t *	cache
	char *	key
	size_t	klen
    PREINIT:
        size_t vlen = 0;
        const char *v = NULL;
    CODE:
        v = groupcache_get(cache, key, klen, &vlen);
        RETVAL = v ? newSVpv(v, vlen) : &PL_sv_undef;
    OUTPUT:
        RETVAL

AV *
groupcache_get_peers(cache, num_peers)
	groupcache_t *	cache
	int *	num_peers
    PREINIT:
        int i;
        AV *peers;
    CODE:
        peers = newAV();
        char **list = groupcache_get_peers(cache, num_peers);
        for (i = 0; i < *num_peers; i++) {
            SV *peer = newSVpv(list[i], strlen(list[i]));
            av_push(peers, sv_2mortal(peer));
        }
        RETVAL = peers;
    OUTPUT:
        RETVAL

int
groupcache_set(cache, key, klen, value, vlen)
	groupcache_t *	cache
	char *	key
	size_t	klen
	char *	value
	size_t	vlen

SV *
groupcache_test_ownership(cache, key, len)
	groupcache_t *	cache
	char *	key
	size_t	len
    PREINIT:
        const char *peer = NULL;
    CODE:
        groupcache_test_ownership(cache, key, len, &peer);
        if (peer) {
            RETVAL = newSVpv(peer, strlen(peer));
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL
