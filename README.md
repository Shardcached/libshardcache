libshardcache
======

C implementation of a sharded key/value storage + caching 
initially inspired by [groupcache](http://github.com/golang/groupcache "groupcache") (golang/groupcache).

Note that this is a fresh implementation of the logic/strategy
implemented in [groupcache](http://github.com/golang/groupcache "groupcache").
It's not a porting but an implementation from scratch.
It uses its own custom protocol for internal communication that differs from the one
used by the [groupcache](http://github.com/golang/groupcache "groupcache") Go implementation
(which is instead based on protobuf + httpd).

Like [groupcache](http://github.com/golang/groupcache "groupcache"),
this library (together with the [shardcached](http://github.com/xant/shardcached "shardcached") daemon implementation)
is intended as a replacement for memcached with some additions:

 * libshardcache is a client library as well as a server library.
   It can be used either to run a shardcache node or to query already running nodes (or both).
   When using the library to run a new node, the library will connect to its own peers and handle
   the internal communication.

 * It has a cache filling mechanism and minimizes access to the underlying storage

 * It ensures fetching the items from the peers or from the storage only once
   even when multiple concurrent requests are looking for the same uncached item.

Unlike [groupcache](http://github.com/golang/groupcache "groupcache") :

 * Supports SET operations. If the node which receives the SET operation
   is responsible for the specified KEY, the new value will be provided to
   the underlying storage (and to next GET requests).
   If the receiving node is not the responsible for the key, the request
   will be forwarded (through the internal communication channel)
   to the responsible peer which will eventualy store the new value and make it
   available to all the [groupcache](http://github.com/golang/groupcache "groupcache") nodes.
 
 * Supports DEL operations. If the node which receives the DEL operation
   is responsible for the specified KEY, the key will be removed from the
   underlying storage and from the cache.
   If evict-on-delete is turned on (it is by default) an evict command will
   be sent to all other peers to force eviction from their cache.
   If the receiving node is not responsible for the key, it will still
   be removed from the local cache (if present) and the request will be
   forwarded (through the internal communication channel) to the
   responsible peer which will eventually remove the key from its storage
   and from the cache.

 * Supports EVICT operations. Evicts differ from deletes in the sense that the 
   key is only unloaded from the cache but not removed from the storage.
   Forcing evictions might be very useful to force new values to be visible
   as soon as possible after being set.

 * Supports migrations via the MGB (migration-begin), MGA (migration-abort)
   and MGE (migration-end) commands. The nodes automatically redistribute
   the keys taking the newly added ones into account.
   
   While the migration is in progress (and keys are being redistributed) 
   the behaviour is the following :

   - If a get operation arrives, first the old continuum is checked,
     if not found the new continuum is checked.
     
   - If a set/delete operation arrives the new continuum is used
     to determine the owner of the key.

   Once the migration is completed the continua are swapped and the new
   continuum becomes the main one.

  * Supports volatile keys, which have an expiration time and will be automatically removed when expired.
    Note that such keys are always kept in memory, regardless of the storage type, and are never 
    passed to the storage backend.
    During a migration the volatile keys eventually not owned by a node will NOT be forwarded to
    the new owner but will be instead expired "prematurely" (since they won't be anyway available anymore
    when switching to the new continuum)

## Lookup process

Exactly like in [groupcache](http://github.com/golang/groupcache "groupcache") implementation, a shardcache lookup of **get("foo")** looks like:

 1. Is the value of "foo" in local memory because in the ARC cache, if so use it.

 2. Is the value of "foo" in local storage/memory because I'm the owner, if so use it.

 3. Ask the value of "foo" to the owner of the key via RPC.
    If other callers come in, via the same process or via RPC requests
    from peers, they block waiting for the load to finish and get the
    same answer. 


