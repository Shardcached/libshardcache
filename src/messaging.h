#ifndef SHARDCACHE_MESSAGING_H
#define SHARDCACHE_MESSAGING_H

#include <sys/types.h>
#include <iomux.h>
#include <fbuf.h>
#include <rbuf.h>
#include "shardcache.h"
#include "protocol.h"
#include "async_reader.h"

// TODO - Document all exposed functions

int global_tcp_timeout(int tcp_timeout);

// synchronously read a message (blocking)
int read_message(int fd,
                 fbuf_t **out,
                 int expected_records,
                 shardcache_hdr_t *hdr,
                 int ignore_timeout);

// asynchronously read a message (callback-based)
int read_message_async(int fd,
                   async_read_callback_t cb,
                   void *priv,
                   async_read_wrk_t **worker);


// synchronously write a message (blocking)
int write_message(int fd,
                  unsigned char hdr,
                  shardcache_record_t *records,
                  int num_records);

// build a valid shardcache message containing the provided records
int build_message(unsigned char hdr,
                  shardcache_record_t *records,
                  int num_records,
                  fbuf_t *out);


// convert an array of items to a (chunkized) record ready to be sent on the wire
// NOTE: the produced record will be chunkized if necessary and will include
// the chunk-size headers
void array_to_record(int num_items, fbuf_t **items, fbuf_t *out);


typedef enum {
    WRITE_STATUS_MODE_SIMPLE = 0,
    WRITE_STATUS_MODE_BOOLEAN = 1,
    WRITE_STATUS_MODE_EXISTS = 2
} rc_to_status_mode_t;
// convert a return code to a protocol-encoded status byte
char rc_to_status(int rc, rc_to_status_mode_t mode);

// convert a (de-chunkized) record to an array of vaules
// NOTE: the record MUST be complete and without the chunk-size headers
uint32_t record_to_array(fbuf_t *record, char ***items, size_t **lens);

// delete a key from a peer
int delete_from_peer(char *peer,
                     void *key,
                     size_t klen,
                     int fd,
                     int expect_response);

// evict a key from a peer
int
evict_from_peer(char *peer,
                void *key,
                size_t klen,
                int fd,
                int expect_response);


// send a new value for a given key to a peer
int send_to_peer(char *peer,
                 void *key,
                 size_t klen,
                 void *value,
                 size_t vlen,
                 uint32_t ttl,
                 uint32_t cttl,
                 int fd,
                 int expect_response);

// cas operation for a given key on a peer
int cas_on_peer(char *peer,
                void *key,
                size_t klen,
                void *old_value,
                size_t old_vlen,
                void *new_value,
                size_t new_vlen,
                uint32_t ttl,
                uint32_t cttl,
                int fd,
                int expect_response);

// add a new value (set if not exists) for a given key to a peer 
int
add_to_peer(char *peer,
            void *key,
            size_t klen,
            void *value,
            size_t vlen,
            uint32_t expire,
            int fd,
            int expect_response);

// fetch the value for a given key from a peer
int fetch_from_peer(char *peer,
                    void *key,
                    size_t len,
                    fbuf_t *out,
                    int fd);

// fetch part of the value for a given key from a peer
int offset_from_peer(char *peer,
                     void *key,
                     size_t len,
                     uint32_t offset,
                     uint32_t dlen,
                     fbuf_t *out,
                     int fd);

// check if a key exists on a peer
int exists_on_peer(char *peer,
                   void *key,
                   size_t len,
                   int fd,
                   int expect_response);

// touch a key on a peer (loads into cache if responsible and timestamp is updated)
int
touch_on_peer(char *peer,
              void *key,
              size_t klen,
              int fd);

// retrieve all the stats counters from a peer
int stats_from_peer(char *peer,
                    char **out,
                    size_t *len,
                    int fd);

// check if a peer is alive (using the CHK command)
int check_peer(char *peer,
               int fd);

// start migration
int migrate_peer(char *peer,
                 void *msgdata,
                 size_t len,
                 int fd);

// abort migration
int abort_migrate_peer(char *peer, int fd);


// connect to a given peer and return the opened filedescriptor
int connect_to_peer(char *address_string, unsigned int timeout);

// retrieve the index of keys stored in a given peer
// NOTE: caller must use shardcache_free_index() to release memory used
//       by the returned shardcache_storage_index_t pointer
shardcache_storage_index_t *index_from_peer(char *peer,
                                            int fd);

typedef int (*fetch_from_peer_async_cb)(char *peer,
                                        void *key,
                                        size_t klen,
                                        void *data,
                                        size_t len,
                                        int idx,
                                        void *priv);

int fetch_from_peer_async(char *peer,
                          void *key,
                          size_t klen,
                          size_t offset,
                          size_t len,
                          fetch_from_peer_async_cb cb,
                          void *priv,
                          int fd,
                          async_read_wrk_t **async_read_wrk_t);


#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
