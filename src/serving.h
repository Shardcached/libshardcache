#ifndef __SERVING_H__
#define __SERVING_H__

#include "shardcache.h"
#include "counters.h"

typedef struct __shardcache_serving_s shardcache_serving_t;

void *accept_requests(void *priv);
shardcache_serving_t *start_serving(shardcache_t *cache, int num_workers);

void stop_serving(shardcache_serving_t *s);

#endif

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
