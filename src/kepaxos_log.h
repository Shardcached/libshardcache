#ifndef __KEPAXOS_LOG_H__
#define __KEPAXOS_LOG_H__

#include <sys/types.h>
#include <stdint.h>

typedef struct __kepaxos_log_s kepaxos_log_t;

kepaxos_log_t *kepaxos_log_create(char *dbfile);
void kepaxos_log_destroy(kepaxos_log_t *log);


uint64_t kepaxos_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t *ballot);
void kepaxos_set_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t ballot, uint64_t seq);
uint64_t kepaxos_max_ballot(kepaxos_log_t *log);

typedef struct {
    void *key;
    size_t klen;
    uint64_t ballot;
    uint64_t seq;
} kepaxos_log_item_t;

int kepaxos_diff_from_ballot(kepaxos_log_t *log, uint64_t ballot, kepaxos_log_item_t **items, int *num_items);
void kepaxos_release_diff_items(kepaxos_log_item_t *items, int num_items);

#endif

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
