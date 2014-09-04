#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <dlfcn.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#include <shardcache.h>

#define DEFAULT_NUM_THREADS    4
#define MODULE_PATH_LEN     1024
#define OPTION_STRING_LEN   1024
#define MAX_STORAGE_OPTIONS  256

/* - */

typedef int  (*module_init)    (shardcache_storage_t *st, const char **options);
typedef void (*module_destroy) (void *);

typedef struct {
    shardcache_storage_t       * storage;
    shardcache_storage_index_t * index;
    int                        * counter;
} worker_thread_args_t;

typedef struct {
    char storage_module[MODULE_PATH_LEN];
    char storage_options_string[OPTION_STRING_LEN];
    int  number_of_threads;
    char * storage_options[MAX_STORAGE_OPTIONS];
} options_t;

static int quit = 0;

static int index_get_from_storage(shardcache_storage_t * storage, shardcache_storage_index_t * index)
{
    if (storage->index == NULL) {
        SHC_ERROR("this storage module doesn't implement the INDEX command, unable to get the keys index");
        return -1;
    }

    if (storage->count == NULL) {
        SHC_ERROR("this storage module doesn't implement the COUNT command, unable to get the keys index");
        return -1;
    }

    index->size  = storage->count(storage->priv);
    index->items = calloc(index->size, sizeof(shardcache_storage_index_item_t));

    storage->index(index->items, index->size, storage->priv);

    return 0;
}

/* - */

static void * worker_thread(void * in_args)
{
    void   * value;
    size_t   value_len;

    worker_thread_args_t * args = (worker_thread_args_t *)in_args;

    if (args->storage->thread_start)
        args->storage->thread_start(args->storage->priv);

    while (!__sync_fetch_and_add(&quit, 0)) {
        for (int i = 0; i < args->index->size; i++) {
            args->storage->fetch(args->index->items[i].key,
                                        args->index->items[i].klen,
                                        &value,
                                        &value_len,
                                        args->storage->priv);

            __sync_fetch_and_add(args->counter, 1);

            if (__sync_fetch_and_add(&quit, 0))
                break;
        }
    }

    if (args->storage->thread_exit)
        args->storage->thread_exit(args->storage->priv);

    return NULL;
}

static void stop(int signal)
{
    printf("\nQuitting...\n");
    (void)__sync_fetch_and_add(&quit, 1);
}

/* - */

static void usage(char * prog, int rc) {
    printf("usage: %s [OPTIONS]...\n"
           "    -s <storagemodule>    the path of the storage module plugin\n"
           "    -o <options>          comma-separated list of storage options\n"
           "    -n <num_threads>      specify the number of threads to use for the test (defaults to: %d)\n"
           "    -h                    prints this help\n",
           prog,
           DEFAULT_NUM_THREADS);
    exit(rc);
}

static void set_default_options(options_t * options) {
    memset(options, 0, sizeof(options_t));
}

static void parse_cmdline(int argc, char ** argv, options_t * options) {
    static struct option long_options[] = {
        { "storagemodule", 2, 0, 's' },
        { "options",       2, 0, 'o' },
        { "num-threads",   2, 0, 'n' },
        { "help",          0, 0, 'h' },
        { NULL,            0, 0,  0  }
    };

    int  option_index = 0;
    char c;

    options->number_of_threads = DEFAULT_NUM_THREADS;

    while ((c = getopt_long(argc, argv, "s:o:n:h", long_options, &option_index))) {
        if (c == -1)
            break;

        switch (c) {
            case 's':
                strncpy(options->storage_module, optarg, MODULE_PATH_LEN);
                break;

            case 'o':
                strncpy(options->storage_options_string, optarg, OPTION_STRING_LEN);
                break;

            case 'n':
                options->number_of_threads = strtol(optarg, NULL, 10);
                break;

            case 'h':
                usage(argv[0], 0);
                break;
        }
    }
}

static int parse_options(char        * options_string,
                         char ** module_options,
                         size_t        max_storage_options)
{
    int    optidx = 0;
    char * p      = options_string;
    char * str    = p;

    while (*p != 0 && optidx < max_storage_options) {
        if (*p == '=' || *p == ',') {
            *p = 0;
            module_options[optidx++] = str;
            str = p+1;
        }
        p++;
    }

    module_options[optidx++] = str;
    module_options[optidx] = NULL;

    return optidx;
}

/* - */

int main(int argc, char ** argv) {
    options_t        options;

    shardcache_log_init("st_benchmark", LOG_WARNING);

    set_default_options(&options);
    parse_cmdline(argc, argv, &options);

    /* - */

    if (strlen(options.storage_module) == 0) {
        SHC_ERROR("the storage module path must be specified");
        usage(argv[0], -1);
    }

    parse_options(options.storage_options_string,
                  options.storage_options,
                  MAX_STORAGE_OPTIONS);

    /* - */

    shardcache_storage_t *storage = shardcache_storage_load(options.storage_module, options.storage_options);
    if (!storage)
        exit(-1);

    signal(SIGINT, stop);

    shardcache_storage_index_t index = {0};
    index_get_from_storage(storage, &index);

    pthread_t            threads        [options.number_of_threads];
    worker_thread_args_t thread_args    [options.number_of_threads];
    int32_t              counters       [options.number_of_threads];

    int32_t              counters_local [options.number_of_threads];
    int32_t              counters_prev  [options.number_of_threads];

    for (int i = 0; i < options.number_of_threads; i++) {
        worker_thread_args_t * args = &thread_args[i];
        args->storage = storage,
        args->index   = &index,
        args->counter = &counters[i];

        counters[i] = 0;
        counters_prev[i] = 0;
    }

    for (int i = 0; i < options.number_of_threads; i++) {
        if (pthread_create(&threads[i], NULL, worker_thread, &thread_args[i]) != 0) {
            SHC_ERROR("Cannot spawn new thread: %s\n", strerror(errno));
            return -1;
        }
    }

    while (!__sync_fetch_and_add(&quit, 0)) {
        sleep(1);

        printf("\033[H\033[J"); // clear the screen
        for (int i = 0; i < options.number_of_threads; i++) {
            counters_local[i] = __sync_fetch_and_add(&counters[i], 0);

            printf("Thread %d: %d - %d per sec.\n",
                i,
                counters_local[i],
                counters_local[i] - counters_prev[i]
            );

            counters_prev[i] = counters_local[i];
        }

        printf("\n");
    }

    for (int i = 0; i < options.number_of_threads; i++) {
        pthread_join(threads[i], NULL);
        printf("Thread %d done\n", i);
    }

    shardcache_storage_dispose(storage);

    return 0;
}

