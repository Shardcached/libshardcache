#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <shardcache_client.h>

#include "const-c.inc"

typedef struct {
    SV *code;
    SV *priv;
} get_async_helper_arg_t;

static int _get_async_helper (char *node,
                              void *key,
                              size_t klen,
                              void *data,
                              size_t dlen,
                              int idx,
                              void *priv)
{
        get_async_helper_arg_t *arg = (get_async_helper_arg_t *)priv;
        SV *nodeSv = newSVpv(node, strlen(node));
        SV *keySv = newSVpv(key, klen);
        SV *dataSv = &PL_sv_undef;

        if (dlen)
            dataSv = newSVpv(data, dlen);

        dSP;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        XPUSHs(sv_2mortal(nodeSv));
        XPUSHs(sv_2mortal(keySv));
        XPUSHs(sv_2mortal(dataSv));
        mXPUSHi(status);
        XPUSHs(arg->priv);
        PUTBACK;

        int count = call_sv(arg->code, G_SCALAR|G_EVAL);

        if (count != 1) {
            croak("Unexpected errors calling the registered runloop callback");
        }

        SPAGAIN;

        IV ret = POPi;

        PUTBACK;
        FREETMPS;
        LEAVE;

        return (ret && status != -1) ? 0 : -1;
}

MODULE = Shardcache::Client::Fast		PACKAGE = Shardcache::Client::Fast		

INCLUDE: const-xs.inc

shardcache_client_t *
shardcache_client_create(nodes, log_level=0)
	SV *	nodes
    int log_level
    CODE:
        int i;
        int	num_nodes = 0;
        shardcache_node_t **shards = NULL;

        if (SvOK(nodes)) {
            if (!SvROK(nodes) || SvTYPE(SvRV(nodes)) != SVt_PVAV)
              croak("Expected an array reference as 'nodes' parameter");

            AV *nodes_array = (AV *)SvRV(nodes);

            num_nodes = av_len(nodes_array) + 1;
            if (num_nodes > 0) {
                shards = malloc(sizeof(shardcache_node_t *) * num_nodes);
                for (i = 0; i < num_nodes; i++) {
                    STRLEN len = 0, alen = 0;
                    char *node = NULL, *addr = NULL;

                    SV **svp = av_fetch(nodes_array, i, 0);
                    if (svp) {
                        if (SvROK(*svp) && SvTYPE(SvRV(*svp)) == SVt_PVAV) {
                            AV *pa = (AV *)SvRV(*svp);
                            SV **nodep = av_fetch(pa, 0, 0);
                            node = SvPVbyte(*nodep, len);
                            if (av_len(pa)) {
                                SV **addrp = av_fetch(pa, 1, 0);
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
                        shards[i] = shardcache_node_create(node, &addr, 1);
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

        if (shards) {
            RETVAL = shardcache_client_create(shards, num_nodes);
            shardcache_free_nodes(shards, num_nodes);
            shardcache_log_init("shardcache_client", LOG_INFO + log_level);
        } else {
            RETVAL = NULL;
        }

    OUTPUT:
        RETVAL

int
shardcache_client_tcp_timeout(c, new_value)
      shardcache_client_t *   c
      int new_value

int
shardcache_client_check_connection_timeout(c, new_value)
      shardcache_client_t *   c
      int new_value

int
shardcache_client_multi_command_max_wait(c, new_value)
      shardcache_client_t *   c
      int new_value

int
shardcache_client_use_random_node(c, new_value)
      shardcache_client_t *   c
      int new_value

int
shardcache_client_pipeline_max(c, new_value)
      shardcache_client_t *   c
      int new_value

int
shardcache_client_del(c, key)
	shardcache_client_t *	c
	SV *	key
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        RETVAL = shardcache_client_del(c, k, klen);
    OUTPUT:
        RETVAL



void
shardcache_client_destroy(c)
	shardcache_client_t *	c

int
shardcache_client_evict(c, key)
	shardcache_client_t *	c
	SV *	key
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        RETVAL = shardcache_client_evict(c, k, klen);
    OUTPUT:
        RETVAL

SV *
shardcache_client_get(c, key)
	shardcache_client_t *	c
	SV *	key
    CODE:
	void *	data = NULL;
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        size_t size = 0;
        int rc = shardcache_client_get_sync(c, k, klen, &data, &size, NULL);
        if (rc == 0 && data && size > 0) {
            RETVAL = newSVpv(data, size);
            free(data);
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL

int
shardcache_client_getf(c, key)
    shardcache_client_t *c
    SV *key
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        RETVAL = shardcache_client_getf(c, k, klen);
    OUTPUT:
        RETVAL


SV *
shardcache_client_offset(c, key, offset, length)
	shardcache_client_t *	c
	SV *	key
        int     offset
        int     length
    CODE:
	char    data[length];
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        size_t size = shardcache_client_offset(c, k, klen, offset, &data, length);
        if (size > 0) {
            RETVAL = newSVpv(data, size);
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL

int
shardcache_client_get_async(c, key, coderef, priv=&PL_sv_undef)
	shardcache_client_t *	c
	SV *	key
	SV *	coderef
	SV *	priv

    CODE:

        if (!SvTRUE(coderef) || ! SvROK(coderef) || SvTYPE(SvRV(coderef)) != SVt_PVCV) {
            croak("missing coderef or not a CODE reference");
        }
	void *	data = NULL;
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        get_async_helper_arg_t arg = {
            .code = coderef,
            .priv = priv
        };
        RETVAL = shardcache_client_get(c, k, klen, _get_async_helper, &arg);
    OUTPUT:
        RETVAL

int
shardcache_client_exists(c, key)
	shardcache_client_t *	c
	SV *	key
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        RETVAL = shardcache_client_exists(c, k, klen);
    OUTPUT:
        RETVAL

int
shardcache_client_touch(c, key)
	shardcache_client_t *	c
	SV *	key
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
        RETVAL = shardcache_client_touch(c, k, klen);
    OUTPUT:
        RETVAL



int
shardcache_client_set(c, key, data, expire)
	shardcache_client_t *	c
	SV *	key
	SV *	data
	uint32_t	expire
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
	STRLEN	dlen = 0;
        char *d = SvPVbyte(data, dlen);
        RETVAL = shardcache_client_set(c, k, klen, d, dlen, expire);
    OUTPUT:
        RETVAL

int
shardcache_client_add(c, key, data, expire)
	shardcache_client_t *	c
	SV *	key
	SV *	data
	uint32_t	expire
    CODE:
	STRLEN	klen = 0;
        char *k = SvPVbyte(key, klen);
	STRLEN	dlen = 0;
        char *d = SvPVbyte(data, dlen);
        RETVAL = shardcache_client_add(c, k, klen, d, dlen, expire);
    OUTPUT:
        RETVAL

int
shardcache_client_check(c, peer)
	shardcache_client_t *	c
	char *	peer

SV *
shardcache_client_stats(c, peer)
	shardcache_client_t *	c
	char *	peer
    CODE:
        char *data = NULL;
        size_t size = 0;
        if (shardcache_client_stats(c, peer, &data, &size) == 0) {
            RETVAL = newSVpv(data, size);
            free(data);
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL

SV *
shardcache_client_index(c, peer)
        shardcache_client_t *   c
        char * peer
    CODE:
        shardcache_storage_index_t *index = shardcache_client_index(c, peer);
        if (index) {
            HV *ret = newHV();
            int i;
            for (i = 0; i < index->size; i++) {
                SV *len = newSViv(index->items[i].vlen);
                hv_store(ret, index->items[i].key, index->items[i].klen, len, 0);
            }
            shardcache_free_index(index);
            RETVAL = newRV_noinc((SV *)ret);
        } else {
            RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL

char *
shardcache_client_errstr(c)
	shardcache_client_t *c

int
shardcache_client_errno(c)
	shardcache_client_t *c

SV *
shardcache_client_get_multi(c, keys, results = &PL_sv_undef)
        shardcache_client_t *c
        SV *keys
        SV *results
    CODE:
        RETVAL = &PL_sv_undef;
        if (SvOK(keys)) {
            if (!SvROK(keys) || SvTYPE(SvRV(keys)) != SVt_PVAV)
              croak("shardcache_client_get_multi(): Expected an array reference as 'keys' parameter");

            AV *items_av = (AV *)SvRV(keys);

            int num_items = av_len(items_av) + 1;
            if (num_items > 0) {
                shc_multi_item_t *items_array[num_items+1];
                int i;
                for (i = 0; i < num_items; i++) {
                   SV **svp = av_fetch(items_av, i, 0);
                    if (!svp)
                        croak("shardcache_client_get_multi(): null element in the 'keys' array");

                    STRLEN klen = 0;
                    char *key = SvPVbyte(*svp, klen);
                    items_array[i] = shc_multi_item_create(key, klen, NULL, 0);
                }
                items_array[num_items] = NULL; // null-terminate it

                int rc = shardcache_client_get_multi(c, items_array);
                if (rc >= 0) { // 0 means OK, 1 means that some keys failed and some succeeded
                    AV *out_array;
                    if (!SvROK(results) || SvTYPE(SvRV(results)) != SVt_PVAV) {
                        out_array = newAV();
                        RETVAL = newRV_noinc((SV *)out_array);
                    } else {
                        out_array = (AV *)SvRV(results);
                        av_clear(out_array);
                        RETVAL = newRV_inc((SV *)out_array);
                    }

                    for (i = 0; i < num_items; i++) {
                        SV *item_sv = &PL_sv_undef;

                        if (items_array[i]->data)
                            item_sv = newSVpv(items_array[i]->data, items_array[i]->dlen);

                        if (!av_store(out_array, i, item_sv) && item_sv != &PL_sv_undef)
                            SvREFCNT_dec(item_sv);

                        shc_multi_item_destroy(items_array[i]);
                    }
                } else {
                    for (i = 0; i < num_items; i++)
                        shc_multi_item_destroy(items_array[i]);
                }
            }
        }

    OUTPUT:
        RETVAL

int
shardcache_client_get_multif(c, keys)
        shardcache_client_t *c
        SV *keys
    CODE:
        RETVAL = -1;
        if (SvOK(keys)) {
            if (!SvROK(keys) || SvTYPE(SvRV(keys)) != SVt_PVAV)
              croak("shardcache_client_get_multi(): Expected an array reference as 'keys' parameter");

            AV *items_av = (AV *)SvRV(keys);

            int num_items = av_len(items_av) + 1;
            if (num_items > 0) {
                shc_multi_item_t *items_array[num_items+1];
                int i;
                for (i = 0; i < num_items; i++) {
                   SV **svp = av_fetch(items_av, i, 0);
                    if (!svp)
                        croak("shardcache_client_get_multi(): null element in the 'keys' array");

                    STRLEN klen = 0;
                    char *key = SvPVbyte(*svp, klen);
                    items_array[i] = shc_multi_item_create(key, klen, NULL, 0);
                }
                items_array[num_items] = NULL; // null-terminate it

                RETVAL = shardcache_client_get_multif(c, items_array);
                for (i = 0; i < num_items; i++)
                    shc_multi_item_destroy(items_array[i]);
            }
        }
    OUTPUT:
        RETVAL


SV *
shardcache_client_set_multi(c, keys)
        shardcache_client_t *c
        SV *keys
    CODE:
        RETVAL = &PL_sv_undef;
        if (SvOK(keys)) {
            if (!SvROK(keys) || SvTYPE(SvRV(keys)) != SVt_PVHV)
              croak("shardcache_client_get_multi(): Expected an hash reference as 'keys' parameter");

            HV *items_hv = (HV *)SvRV(keys);

            int num_items = 0;

            // count the number of keys in the hashref
            if (GIMME_V == G_SCALAR) {
                IV i;
                dTARGET;

                if (! SvTIED_mg((const SV *)items_hv, PERL_MAGIC_tied) ) {
                    num_items = HvUSEDKEYS(items_hv);
                }
                else {
                    while (hv_iternext(items_hv)) num_items++;
                }
            }
            if (num_items > 0) {
                shc_multi_item_t *items_array[num_items+1];
                HE *entry;
                int i = 0;
                while ((entry = hv_iternext(items_hv))) {
                    if (i > num_items)
                        croak("shardcache_client_set_multi() found more elements than expected in the 'keys' hashref");

                    SV* const key_sv = hv_iterkeysv(entry);
                    SV *value_sv = hv_iterval(items_hv, entry);

                    STRLEN klen = 0;
                    char *key = SvPVbyte(key_sv, klen);

                    STRLEN vlen = 0;
                    char *value = SvPVbyte(value_sv, vlen);
                    items_array[i] = shc_multi_item_create(key, klen, value, vlen);
                    i++;
                }

                items_array[num_items] = NULL; // null-terminate it

                int rc = shardcache_client_set_multi(c, items_array);
                if (rc == 0) {
                    HV *out_hash = newHV();
                    for (i = 0; i < num_items; i++) {
                        SV *item_sv = newSViv((items_array[i]->status == 0));
                        SV **ref = hv_store(out_hash, (const char *)items_array[i]->key,
                                            items_array[i]->klen, item_sv, 0);
                        if (!ref)
                            SvREFCNT_dec(item_sv);

                        shc_multi_item_destroy(items_array[i]);
                    }
                    RETVAL = newRV_noinc((SV *)out_hash);
                } else {
                    for (i = 0; i < num_items; i++)
                        shc_multi_item_destroy(items_array[i]);
                }
            }
        }

    OUTPUT:
        RETVAL

SV *
shardcache_client_current_node(c)
        shardcache_client_t *c
    CODE:
        RETVAL = &PL_sv_undef;
        shardcache_node_t *node = shardcache_client_current_node(c);
        if (node) {
            char *string = shardcache_node_get_string(node);
            RETVAL = newSVpv(string, strlen(string));
        }
    OUTPUT:
        RETVAL

