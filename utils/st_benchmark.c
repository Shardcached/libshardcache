#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <dlfcn.h>

#include <shardcache.h>

static const int MAX_STORAGE_OPTIONS = 256;

/* - */

typedef struct {
    char storage_module[1024];
    char storage_options_string[1024];
    const char * storage_options[MAX_STORAGE_OPTIONS];
} options_t;

typedef int  (*module_init)    (shardcache_storage_t *st, const char **options);
typedef void (*module_destroy) (void *);

typedef struct {
    shardcache_storage_t module;
    module_init          init;
    module_destroy       destroy;
} storage_module_t;

/* - */

static int storage_module_load(storage_module_t * storage, char * module_path)
{
    char * error = NULL;

    void * handle = dlopen(module_path, RTLD_NOW);
    if (!handle) {
        SHC_ERROR("cannot load the storage module: %s (%s)", module_path, dlerror());
        exit(1);
    }

    int * version = dlsym(handle, "storage_version");
    if (!version || ((error = dlerror()) != NULL)) {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_version' in the loaded module");

        return 1;
    }

    if (*version != SHARDCACHE_STORAGE_API_VERSION) {
        SHC_ERROR("The storage plugin version doesn't match (%d != %d)",
                    version, SHARDCACHE_STORAGE_API_VERSION);
        return 1;
    }

    storage->init = dlsym(handle, "storage_init");
    if (!storage->init || ((error = dlerror()) != NULL))  {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_init' in the loaded module");
        dlclose(handle);

        return 1;
    }

    storage->destroy = dlsym(handle, "storage_destroy");
    if (!storage->destroy || ((error = dlerror()) != NULL))  {
        if (error)
            SHC_ERROR("%s", error);
        else
            SHC_ERROR("Can't find the symbol 'storage_destroy' in the loaded module");
        dlclose(handle);

        return 1;
    }

    return 0;
}

static int storage_module_instantiate(storage_module_t * storage, const char ** options)
{
    int ret = storage->init(&storage->module, options);
    if (ret != 0) {
        SHC_ERROR("storage module initialization reported failure %d", ret);
        return 1;
    }

    return 0;
}

/* - */

static void print_help(char * prog) {
    printf("usage: %s [OPTIONS]...\n"
           "    -s <storagemodule>    the path of the storage module plugin\n"
           "    -o <options>          comma-separated list of storage options\n",
           prog);
}

static void set_default_options(options_t * options) {
    memset(options, 0, sizeof(options_t));
}

static void parse_cmdline(int argc, char ** argv, options_t * options) {
    static struct option long_options[] = {
        { "storagemodule", 2, 0, 's' },
        { "options",       2, 0, 'o' },
        { "help",          0, 0, 'h' },
        { 0,               0, 0,  0  }
    };

    int  option_index = 0;
    char c;

    while ((c = getopt_long(argc, argv, "s:o:h", long_options, &option_index))) {
        if (c == -1)
            break;

        switch (c) {
            case 's':
                strncpy(options->storage_module, optarg, sizeof(options->storage_module));
                break;

            case 'o':
                strncpy(options->storage_options_string, optarg, sizeof(options->storage_options_string));
                break;

            case 'h':
                print_help(argv[0]);
                exit(0);
                break;
        }
    }
}

static int parse_options(char        * options_string,
                         const char ** module_options,
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
    storage_module_t storage;

    shardcache_log_init("st_benchmark", LOG_WARNING);

    set_default_options(&options);
    parse_cmdline(argc, argv, &options);

    /* - */

    if (strlen(options.storage_module) == 0) {
        SHC_ERROR("the storage module path must be specified");
        exit(1);
    }

    parse_options(options.storage_options_string,
                  options.storage_options,
                  MAX_STORAGE_OPTIONS);
    /* - */

    if (storage_module_load(&storage, options.storage_module) != 0)
        return 1;

    if (storage_module_instantiate(&storage, options.storage_options) != 0)
        return 1;

    storage.destroy(storage.module.priv);

    return 0;
}

