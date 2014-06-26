/* The cached object.
 * The data will be loaded when ARC instruct us to do so.
 */
typedef struct {
    void *key;   // The key (weak reference to the actual key stored in the arc resource)
    size_t klen; // The length of the key

    char buf[256]; // internal storage for data which doesn't exceeds 256 bytes.
                   // If the complete data is bigger than 256 bytes, the required
                   // memory will be allocated and the data pointer will be set
                   // to point to the newly allocated memory.

    void *data;  // The data (if any, NULL otherwise)
                 // Note that if the data is less than 256 bytes this pointer
                 // will point back to the internal buffer (buf pointer)

    size_t dlen; // The length of the data (if any, 0 otherwise)

    struct timeval ts; // the timestamp of when the object has been loaded
                       // into the cache

    linked_list_t *listeners; // list of listeners which will be notified
                              // while the object data is being retreived

    uint16_t flags;
    #define COBJ_FLAG_ASYNC    (1)
    #define COBJ_FLAG_COMPLETE (1<<1)
    #define COBJ_FLAG_EVICTED  (1<<2)
    #define COBJ_FLAG_EVICT    (1<<3)
    #define COBJ_FLAG_DROP     (1<<4)
    #define COBJ_FLAG_FETCHING (1<<5)

    pthread_mutex_t lock; // All operations on this structure should be
                          // synchronized using this lock

    arc_resource_t *res;
} cached_object_t;

#define COBJ_CHECK_FLAGS(__o, __f) ((((__o)->flags) & (__f)) == (__f))
#define COBJ_SET_FLAG(__o, __f) ((__o)->flags |= (__f))
#define COBJ_UNSET_FLAG(__o, __f) ((__o)->flags &= ~(__f))

typedef struct {
    shardcache_get_async_callback_t cb;
    void *priv;
} shardcache_get_listener_t;

void *arc_ops_create(const void *key, size_t len, int async, arc_resource_t *res, void *priv);
int arc_ops_fetch(void *item, size_t *size, void * priv);
void arc_ops_evict(void *item, void *priv);
void arc_ops_destroy(void *item, void *priv);

