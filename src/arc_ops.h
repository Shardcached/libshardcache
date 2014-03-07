/* This is the object we're managing. It has a key
 * and some data. This data will be loaded when ARC instruct
 * us to do so. */
typedef struct {
    arc_t *arc; // A pointer to the ARC cache owning this object

    void *key;   // The key
    size_t klen; // The length of the key

    void *data;  // The data (if any, NULL otherwise)
    size_t dlen; // The length of the data (if any, 0 otherwise)

    struct timeval ts; // the timestamp of when the object has been loaded
                       // into the cache

    int async;         // true if the object should be retreived
                       // in asynchronous mode

    linked_list_t *listeners; // list of listeners which will be notified
                              // while the object data is being retreived 

    int complete; // true if the object has been completely retrieved
                  // (either from the storage or from a peer)

    int evicted; // true if the object has been evicted and no new
                 // new listeners should be registered anymore
                 // (since they wouldn't be called anymore)

    int evict;   // true if the object should be evicted as soon as possible

    int drop;    // true if the object will not be cached cached but dropped 
                 // just after it has been returned to the requester

    pthread_mutex_t lock; // All operations on this structure should be
                          // synchronized using this lock
                          // NOTE :we want a mutex here because the object
                          // might be locked for long time if involved in a
                          // fetch or store operation
} cache_object_t;

typedef struct {
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_listener_t;

void *arc_ops_create(const void *key, size_t len, int async, void *priv);
int arc_ops_fetch(void *item, size_t *size, void * priv);
void arc_ops_evict(void *item, void *priv);
void arc_ops_destroy(void *item, void *priv);

