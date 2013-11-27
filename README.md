shardcache
======

C implementation of a sharded key/value storage + caching 
initially inspired to groupcache (golang/groupcache)

Note that this is a fresh implementation of the logic/strategy
implemented in groupcache. It's not a porting but an implementation
from scratch. It uses its own custom protocol for internal communication
that differs from the one used by the groupcache Go implementation
(which is instead based on protobuf + httpd)

Like groupcache (https://github.com/golang/groupcache.git),
this library, together with the shardcached daemon implementation)
is intended as a replacement for memcached with some additions:

 * does not require running a separate set of servers, thus massively
   reducing deployment/configuration pain. As for groupcache,
   libshardcache is a client library as well as a server.
   It connects to its own peers.

 * cache filling mechanism based on ARC (Adaptive Replacement Cache)

 * supports automatic mirroring of super-hot items to multiple
   processes.  This prevents memcached hot spotting where a machine's
   CPU and/or NIC are overloaded by very popular keys/values.

Differently from the groupcache :

 * supports SET operations, If the node which receives the SET operation
   is responsible for the specified KEY, the new value will be provided to
   the underlying storage and will be provided to next GET requests.
   If the receiving node is not the responsible one for the key, the request
   will be forwarded (through the internal groupcache communication channel)
   to the responsible peer which will eventualy store the new value and make it
   available to all the groupcache nodes
 
 * supports DEL operations If the node which receives the DEL operation
   is responsible for the specified KEY, the key will be removed from the
   underlying storage and from the cache.
   If the receiving node is not the responsible one for the key, it will still
   be removed from the local cache (if present) and the request will be
   forwarded (through the internal groupcache communication channel) to the
   responsible peer which will eventualy remove the key from its storage
   and from the cache

## Lookup process

Exactly like in groupcache implementation, a shardcache lookup of **get("foo")** looks like:

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

