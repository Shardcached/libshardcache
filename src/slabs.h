/*
 * Copyright (c) 2003, Danga Interactive, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 * 
 *     * Neither the name of the Danga Interactive nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* slabs memory allocation */
#ifndef SLABS_H
#define SLABS_H

typedef struct __slabs_s slabs_t;
/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor.
    3rd argument specifies if the slab allocator should allocate all memory
    up front (if true), or allocate memory in chunks as it is needed (if false)
*/
slabs_t *slabs_create(const size_t limit, const double factor, const int prealloc, size_t max_size);

void slabs_destroy(slabs_t *sl);

/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(slabs_t *sl, const size_t size);

/** Allocate object of given length. 0 on error */ /*@null@*/
void *slabs_alloc(slabs_t *sl, const size_t size, unsigned int id);

/** Free previously allocated object */
void slabs_free(slabs_t *sl, void *ptr, size_t size, unsigned int id);

/** Adjust the stats for memory requested */
void slabs_adjust_mem_requested(slabs_t *sl, unsigned int id, size_t old, size_t ntotal);

/** Return a datum for stats in binary protocol */
//int get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c);

/** Fill buffer with stats */ /*@null@*/
//void slabs_stats(ADD_STAT add_stats, void *c);

int start_slab_maintenance_thread(slabs_t *sl);
void stop_slab_maintenance_thread(slabs_t *sl);

int start_slab_rebalance_thread(slabs_t *sl);
void stop_slab_rebalance_thread(slabs_t *sl);

enum reassign_result_type {
    REASSIGN_OK=0, REASSIGN_RUNNING, REASSIGN_BADCLASS, REASSIGN_NOSPARE,
    REASSIGN_SRC_DST_SAME
};

enum reassign_result_type slabs_reassign(slabs_t *sl, int src, int dst);

void slabs_rebalancer_pause(slabs_t *sl);
void slabs_rebalancer_resume(slabs_t *sl);

#endif
