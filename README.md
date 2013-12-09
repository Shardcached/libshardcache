shardcache
======

C implementation of a sharded key/value storage + caching 
initially inspired to [groupcache](http://github.com/golang/groupcache "groupcache") (golang/groupcache)

Note that this is a fresh implementation of the logic/strategy
implemented in [groupcache](http://github.com/golang/groupcache "groupcache"). It's not a porting but an implementation
from scratch. It uses its own custom protocol for internal communication
that differs from the one used by the [groupcache](http://github.com/golang/groupcache "groupcache") Go implementation
(which is instead based on protobuf + httpd)

Like [groupcache](http://github.com/golang/groupcache "groupcache"),
this library, together with the [shardcached](http://github.com/xant/shardcached "shardcached") daemon implementation)
is intended as a replacement for memcached with some additions:

 * does not require running a separate set of servers, thus massively
   reducing deployment/configuration pain. As for [groupcache](http://github.com/golang/groupcache "groupcache"),
   libshardcache is a client library as well as a server.
   It connects to its own peers.

 * cache filling mechanism based on ARC (Adaptive Replacement Cache)

 * ensure fetching the items from peers or from the storage only once
   even when multiple concurrent request are looking for the same 
   uncached item.

Differently from the [groupcache](http://github.com/golang/groupcache "groupcache") :

 * supports SET operations, If the node which receives the SET operation
   is responsible for the specified KEY, the new value will be provided to
   the underlying storage and will be provided to next GET requests.
   If the receiving node is not the responsible one for the key, the request
   will be forwarded (through the internal [groupcache](http://github.com/golang/groupcache "groupcache") communication channel)
   to the responsible peer which will eventualy store the new value and make it
   available to all the [groupcache](http://github.com/golang/groupcache "groupcache") nodes
 
 * supports DEL operations If the node which receives the DEL operation
   is responsible for the specified KEY, the key will be removed from the
   underlying storage and from the cache.
   If the receiving node is not the responsible one for the key, it will still
   be removed from the local cache (if present) and the request will be
   forwarded (through the internal [groupcache](http://github.com/golang/groupcache "groupcache") communication channel) to the
   responsible peer which will eventualy remove the key from its storage
   and from the cache

 * supports migrations via the MGB (migration-begin), MGA (migration-abort)
   and MGE (migration-end) commands. The nodes automatically redistribute
   the keys taking the newly added ones into account.
   
   While the migration is in progress (and keys are being redistributed) 
   the behaviour is the following :

   - If a get operation arrives, first the old continuum is checked,
     if not found the new continuum is checked.
     
   - If a set/delete operation arrives the new continuum is used
     to determine where to set/delete the item

   Once the migration is completed the continua are swapped and the new
   continuum will become the actual one

## Lookup process

Exactly like in [groupcache](http://github.com/golang/groupcache "groupcache") implementation, a shardcache lookup of **get("foo")** looks like:

(On machine #5 of a set of N machines running the same code)

 1. Is the value of "foo" in local memory because it's super hot?  If so, use it.

 2. Is the value of "foo" in local memory because peer #5 (the current
    peer) is the owner of it?  If so, use it.

 3. Amongst all the peers in my set of N, am I the owner of the key
    "foo"?  (e.g. does it consistent hash to 5?)  If so, load it.  If
    other callers come in, via the same process or via RPC requests
    from peers, they block waiting for the load to finish and get the
    same answer.  If not, RPC to the peer that's the owner and get
    the answer.  If the RPC fails, just load it locally (still with
    local dup suppression).

