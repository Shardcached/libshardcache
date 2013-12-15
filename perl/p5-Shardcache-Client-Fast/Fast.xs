#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <shardcache_client.h>

#include "const-c.inc"

MODULE = Shardcache::Client::Fast		PACKAGE = Shardcache::Client::Fast		

INCLUDE: const-xs.inc

shardcache_client_t *
shardcache_client_create(nodes, auth=NULL)
	SV *	nodes
	char *	auth
    CODE:
        int i;
	int	num_nodes = 0;
        shardcache_node_t *shards = NULL;

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
        RETVAL = shardcache_client_create(shards, num_nodes, auth);

    OUTPUT:
        RETVAL


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
        RETVAL = shardcache_client_del(c, k, klen);
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
        size_t size = shardcache_client_get(c, k, klen, &data);
        if (data && size > 0) {
            RETVAL = newSVpv(data, size);
            free(data);
        } else {
            RETVAL = &PL_sv_undef;
        }
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
        RETVAL = (shardcache_client_set(c, k, klen, d, dlen, expire) == 0);
    OUTPUT:
        RETVAL
