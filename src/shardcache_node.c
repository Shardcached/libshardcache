#include <string.h>
#include <syslog.h>
#include <regex.h>

#include "shardcache_node.h"
#include "shardcache_log.h"

#include "shardcache_internal.h"

#define ADDR_REGEXP "^[a-z0-9_\\.\\-]+(:[0-9]+)?$"

struct __shardcache_node_s {
    char *label;
    char **address;
    int num_replicas;
    char *string;
}; 

static int
shardcache_check_address_string(char *str)
{
    regex_t addr_regexp;
    int rc = regcomp(&addr_regexp, ADDR_REGEXP, REG_EXTENDED|REG_ICASE);
    if (rc != 0) {
        char errbuf[1024];
        regerror(rc, &addr_regexp, errbuf, sizeof(errbuf));
        SHC_ERROR("Can't compile regexp %s: %s", ADDR_REGEXP, errbuf);
        return -1;
    }

    int matched = regexec(&addr_regexp, str, 0, NULL, 0);
    regfree(&addr_regexp);

    if (matched != 0) {
        return -1;
    }

    return 0;
}

shardcache_node_t *
shardcache_node_create_from_string(char *str)
{
    char *copy = strdup(str);
    char *string = copy;
    char *label = strsep(&string, ":");
    char *addrstring = string;
    char *addr = NULL;
    char **addrlist = NULL;
    int num_addresses = 0;

    while ((addr = strsep(&addrstring, ";")) != NULL) {
        if (!addr || shardcache_check_address_string(addr) != 0) {
            SHC_ERROR("Bad address format for peer: '%s'", addr);
            int i;
            for (i = 0; i < num_addresses; i++)
                free(addrlist[i]);
            free(addrlist);
            free(copy);
            return NULL;
        }
        addrlist = realloc(addrlist, sizeof(char *) * (num_addresses + 1));
        addrlist[num_addresses] = strdup(addr);
        num_addresses++;
    }

    shardcache_node_t *node = malloc(sizeof(shardcache_node_t));
    node->label = strdup(label);
    node->address = addrlist;
    node->string = strdup(str);
    node->num_replicas = num_addresses;

    free(copy);
    return node;
}

shardcache_node_t *
shardcache_node_create(char *label, char **addresses, int num_addresses)
{
    int i;
    shardcache_node_t *node = calloc(1, sizeof(shardcache_node_t));
    node->label = strdup(label);
    node->num_replicas = num_addresses;
    node->address = calloc(num_addresses, sizeof(char *));
    int slen = strlen(node->label) + 3;
    char *node_string = malloc(slen);
    snprintf(node_string, slen, "%s:", node->label);
    for (i = 0; i < num_addresses; i++) {
        if (i > 0)
            strcat(node_string, ";");
        slen += strlen(addresses[i]) + 1;
        node_string = realloc(node_string, slen);
        strcat(node_string, addresses[i]);
        node->address[i] = strdup(addresses[i]);
    }
    node->string = node_string;
    return node;
}

shardcache_node_t *
shardcache_node_copy(shardcache_node_t *node)
{
    int i;

    shardcache_node_t *copy = malloc(sizeof(shardcache_node_t));
    copy->label = strdup(node->label);
    copy->address = malloc(sizeof(char *) * node->num_replicas);
    for (i = 0; i < node->num_replicas; i++)
        copy->address[i] = strdup(node->address[i]);
    copy->num_replicas = node->num_replicas;
    copy->string = strdup(node->string);
    return copy;
}

void
shardcache_node_destroy(shardcache_node_t *node)
{
    int i;
    free(node->label);
    for (i = 0; i < node->num_replicas; i++)
        free(node->address[i]);
    free(node->address);
    free(node->string);
    free(node);
}

char *
shardcache_node_get_string(shardcache_node_t *node)
{
    return node->string;
}

char *
shardcache_node_get_label(shardcache_node_t *node)
{
    return node->label;
}

char *
shardcache_node_get_address(shardcache_node_t *node)
{
    return node->address[random() % node->num_replicas];
}

shardcache_node_t *
shardcache_node_select(shardcache_t *cache, char *label)
{
    shardcache_node_t *node = NULL;
    int i;
    for (i = 0; i < cache->num_shards; i++ ){
        if (strcmp(cache->shards[i]->label, label) == 0) {
            node = cache->shards[i];
            break;
        }
    }
    SPIN_LOCK(cache->migration_lock);
    if (cache->migration && !node) {
        for (i = 0; i < cache->num_migration_shards; i++) {
            if (strcmp(cache->migration_shards[i]->label, label) == 0) {
                node = cache->migration_shards[i];
                break;
            }
        }
    }
    SPIN_UNLOCK(cache->migration_lock);
    return node;
}

int
shardcache_node_num_addresses(shardcache_node_t *node)
{
    return node->num_replicas;

}

int
shardcache_node_get_all_addresses(shardcache_node_t *node, char **addresses, int num_addresses)
{
    int i;
    for (i = 0; i < num_addresses && i < node->num_replicas; i++)
        addresses[i] = node->address[i];
    return node->num_replicas;
}

int
shardcache_node_get_all_labels(shardcache_node_t *node, char **labels, int num_labels)
{
    int i;
    for (i = 0; i < num_labels && i < node->num_replicas; i++)
        labels[i] = node->label;
    return node->num_replicas;
}

char *
shardcache_node_get_address_at_index(shardcache_node_t *node, int index)
{
    if (index < node->num_replicas)
        return node->address[index];
    return NULL;
}

// vim: tabstop=4 shiftwidth=4 expandtab:
// /* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
