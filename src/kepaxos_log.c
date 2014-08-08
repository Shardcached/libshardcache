#include "kepaxos_log.h"
#include "shardcache.h"

#ifndef HAVE_UINT64_T
#define HAVE_UINT64_T
#endif
#include <siphash.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <stdio.h>
#include <errno.h>

struct __kepaxos_log_s {
    char *dbpath;
    uint64_t max_ballot;
    FILE *bfile;
};

kepaxos_log_t *
kepaxos_log_create(char *dbpath)
{

    struct stat st;
    size_t ballots_path_len = strlen(dbpath) + 10;
    char ballots_path[ballots_path_len];
    snprintf(ballots_path, ballots_path_len, "%s/ballots", dbpath);

    if (stat(dbpath, &st) != 0) {
        if (mkdir(dbpath, 0700) != 0) {
            SHC_ERROR("Can't create the dbpath %s: %s", dbpath, strerror(errno));
            return NULL;
        }
        if (stat(dbpath, &st) != 0) {
            SHC_ERROR("Can't stat the dbpath %s: %s", dbpath, strerror(errno));
            return NULL;
        }
        mkdir(ballots_path, 0700);
    }
    if (stat(ballots_path, &st) != 0) {
        if (mkdir(ballots_path, 0700) != 0) {
            SHC_ERROR("Can't create the ballots path %s: %s", ballots_path, strerror(errno));
            return NULL;
        }
    }
    
    if (!S_ISDIR(st.st_mode)) {
        SHC_ERROR("%s is not a directory", dbpath);
        return NULL;
    }

    /*
    DIR *dbdir = NULL;
    dbdir = opendir(dbpath);
    if (!dbdir) {
        SHC_ERROR("Can't open the dbpath %s: %s", dbpath, strerror(errno));
        return NULL;
    }
    */

    int ballot_path_size = strlen(dbpath) + 8;
    char ballot_path[ballot_path_size];
    snprintf(ballot_path, ballot_path_size, "%s/ballot", dbpath);

    FILE *ballot_file = fopen(ballot_path, "r+");
    if (!ballot_file) {
        ballot_file = fopen(ballot_path, "w+");
        if (!ballot_file) {
            SHC_ERROR("Can't open/create the ballot_file %s: %s", ballot_path, strerror(errno));
            //closedir(dbdir);
            return NULL;
        }
    }

    uint64_t ballot = 0;
    size_t nitems = fread(&ballot, sizeof(ballot), 1, ballot_file);
    if (nitems < 1 && ferror(ballot_file)) {
        SHC_ERROR("Error reading the ballot_file %s: %s", ballot_path, strerror(errno));
        //closedir(dbdir);
        fclose(ballot_file);
        return NULL;
    }

    kepaxos_log_t *log = calloc(1, sizeof(kepaxos_log_t));
    log->dbpath = strdup(dbpath);
    log->bfile = ballot_file;
    log->max_ballot = ballot;

    return log;
}

void
kepaxos_log_destroy(kepaxos_log_t *log)
{
    //closedir(log->dbdir);
    free(log->dbpath);
    fclose(log->bfile);
    free(log);
}

static inline void
kepaxos_compute_key_hashes(void *key, size_t klen, uint64_t *hash1, uint64_t *hash2)
{
    unsigned char auth1[16] = "0123456789ABCDEF";
    unsigned char auth2[16] = "ABCDEF0987654321";

    *hash1 = sip_hash24(auth1, key, klen);
    *hash2 = sip_hash24(auth2, key, klen);
}

uint64_t
kepaxos_max_ballot(kepaxos_log_t *log)
{
    return log->max_ballot;
}

uint64_t
kepaxos_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t *ballot)
{
    uint64_t keyhash1, keyhash2;
    uint64_t seq = 0;

    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);
    char kstr[35]; // 32 + 0x + null-terminator
    snprintf(kstr, sizeof(kstr), "%s%s",
             shardcache_hex_escape((char *)&keyhash1, sizeof(keyhash1), 16, 0),
             shardcache_hex_escape((char *)&keyhash2, sizeof(keyhash2), 16, 0));


    struct stat st;
    size_t kprefix_path_len = strlen(log->dbpath) + 6;
    char kprefix_path[kprefix_path_len];
    snprintf(kprefix_path, kprefix_path_len, "%s/%02x%02x", log->dbpath, ((char *)key)[0], ((char *)key)[klen-1]);

    size_t kpath_len = kprefix_path_len + 1 + strlen(kstr) + 1;
    char kpath[kpath_len];
    snprintf(kpath, sizeof(kpath), "%s/%s", kprefix_path, kstr);
    
    if (stat(kprefix_path, &st) != 0 || !S_ISDIR(st.st_mode))
        return 0;
    
    if (stat(kpath, &st) != 0 || !S_ISDIR(st.st_mode))
        return 0;

    char seq_path[kpath_len + 5]; // kpath_len + / + "seq" + null-terminator
    snprintf(seq_path, sizeof(seq_path), "%s/seq", kpath);

    FILE *seq_file = fopen(seq_path, "r");
    if (!seq_file)
        return 0;

    size_t nitems = fread(&seq, sizeof(seq), 1, seq_file);


    if (nitems < 1 && ferror(seq_file))
        SHC_ERROR("Error reading the seq_file %s: %s", seq_path, strerror(errno));

    fclose(seq_file);

    if (ballot) {
        char ballot_path[kpath_len + 8];
        snprintf(ballot_path, sizeof(ballot_path), "%s/ballot", kpath);

        FILE *ballot_file = fopen(ballot_path, "r");
        if (ballot_file) {
            nitems = fread(ballot, sizeof(*ballot), 1, ballot_file);
            if (nitems < 1 && ferror(ballot_file))
                SHC_ERROR("Error reading the ballot_file %s: %s", ballot_path, strerror(errno));
            fclose(ballot_file);
        } else {
            SHC_ERROR("Error reading the ballot_file %s: %s", ballot_path, strerror(errno));
        }
    }

    return seq;
}

void
kepaxos_set_last_seq_for_key(kepaxos_log_t *log, void *key, size_t klen, uint64_t ballot, uint64_t seq)
{
    // let's compute the path where this key should be stored in the log
    uint64_t keyhash1, keyhash2;
    kepaxos_compute_key_hashes(key, klen, &keyhash1, &keyhash2);
    char kstr[35]; // 32 + 0x + null-terminator
    snprintf(kstr, sizeof(kstr), "%s%s",
             shardcache_hex_escape((char *)&keyhash1, sizeof(keyhash1), 16, 0),
             shardcache_hex_escape((char *)&keyhash2, sizeof(keyhash2), 16, 0));


    struct stat st;
    size_t kprefix_path_len = strlen(log->dbpath) + 6;
    char kprefix_path[kprefix_path_len];
    snprintf(kprefix_path, kprefix_path_len, "%s/%02x%02x", log->dbpath, ((char *)key)[0], ((char *)key)[klen-1]);

    size_t kpath_len = kprefix_path_len + 1 + strlen(kstr) + 1;
    char kpath[kpath_len];
    snprintf(kpath, sizeof(kpath), "%s/%s", kprefix_path, kstr);
    
    // if the directory doesn't exist we must create it
    if (stat(kprefix_path, &st) != 0 && mkdir(kprefix_path, 0700) != 0) {
        SHC_ERROR("Can't create the key_prefix_path %s: %s", kprefix_path, strerror(errno));
        return;
    }

    if (stat(kpath, &st) != 0 || !S_ISDIR(st.st_mode)) {
        if (mkdir(kpath, 0700) != 0) {
            SHC_ERROR("Can't create the key_path %s: %s", kpath, strerror(errno));
            return;
        }
    }

    // first store the complete key
    size_t kfile_path_len = kpath_len + 5;
    char kfile_path[kfile_path_len];
    snprintf(kfile_path, sizeof(kfile_path), "%s/key", kpath);

    if (stat(kfile_path, &st) != 0 || !S_ISREG(st.st_mode)) {
        FILE *key_file = fopen(kfile_path, "w");
        if (key_file) {
            if (fwrite(key, klen, 1, key_file) != 1)
                SHC_ERROR("Can't update the key file %s: %s", kfile_path, strerror(errno));
            fclose(key_file);
        }
    }

    // store the seq
    char seq_path[kpath_len + 5];
    snprintf(seq_path, sizeof(seq_path), "%s/seq", kpath);

    FILE *seq_file = fopen(seq_path, "w");
    if (!seq_file) {
        SHC_ERROR("Can't open the seq_file %s for writing: %s", seq_path, strerror(errno));
        return;
    }

    size_t nitems = fwrite(&seq, sizeof(seq), 1, seq_file);
    if (nitems < 1) {
        SHC_ERROR("Error updating the seq_file %s: %s", seq_path, strerror(errno));
        return;
    }
    fclose(seq_file);

    // store the ballot to the key directory
    // note that we also need to remove the symlink to the previous ballot
    char ballot_path[kpath_len + 8];
    snprintf(ballot_path, sizeof(ballot_path), "%s/ballot", kpath);

    size_t ballot_link_path_len = strlen(log->dbpath) + 10;
    char ballot_link_path[ballot_link_path_len]; 
    snprintf(ballot_link_path, sizeof(ballot_link_path), "%s/ballots", log->dbpath);

    FILE *ballot_file = fopen(ballot_path, "r+");
    if (ballot_file) {
        // if a previous symlink exists, let's get it out of the way
        uint64_t old_ballot = 0;
        if (fread(&old_ballot, sizeof(old_ballot), 1, ballot_file) != 1) {
            SHC_ERROR("Can't read the ballot file %s: %s", ballot_path, strerror(errno));
        }
        if (old_ballot) {
            char old_ballot_path[ballot_link_path_len + 64];
            snprintf(old_ballot_path, sizeof(old_ballot_path), "%s/%" PRIu64, ballot_link_path, old_ballot);
            if (unlink(old_ballot_path) != 0)
                SHC_ERROR("Can't remove ballot symlink %s: %s", old_ballot_path, strerror(errno));
        }
        rewind(ballot_file);
    } else {
        ballot_file = fopen(ballot_path, "w");
    }

    if (ballot_file) {
        nitems = fwrite(&ballot, sizeof(ballot), 1, ballot_file);
        if (nitems == 1) {
            char new_ballot_path[ballot_link_path_len + 64];
            snprintf(new_ballot_path, sizeof(new_ballot_path), "%s/%" PRIu64, ballot_link_path, ballot);
            if (symlink(kpath, new_ballot_path) != 0) {
                SHC_ERROR("Error creating the ballot_file link %s (%s): %s", new_ballot_path, ballot_path, strerror(errno));
            }
        } else {
            SHC_ERROR("Error updating the ballot_file %s: %s", ballot_path, strerror(errno));
        }
        fclose(ballot_file);
    } else {
        SHC_ERROR("Error updating the ballot_file %s: %s", ballot_path, strerror(errno));
    }

    // and finally update the stored max_ballot if necessary (it should always be true)
    if (ballot > log->max_ballot) {
        rewind(log->bfile);
        size_t nitems = fwrite(&ballot, sizeof(ballot), 1, log->bfile);
        if (nitems < 1 && ferror(ballot_file))
            SHC_ERROR("Error updating the max_ballot file in the dbpath %s: %s", log->dbpath, strerror(errno));
        log->max_ballot = ballot;
    }
}


int 
kepaxos_diff_from_ballot(kepaxos_log_t *log, uint64_t ballot, kepaxos_log_item_t **items, int *num_items)
{

    // access the directory where we store the symlinks to all the keys in the log
    // indexed by their ballot, for easy comparison and access when calculating the diff
    size_t ballots_path_len = strlen(log->dbpath) + 9;
    char ballots_path[ballots_path_len];
    snprintf(ballots_path, strlen(ballots_path), "%s/ballots", log->dbpath);
    DIR *ballots_dir = opendir(ballots_path);
    if (!ballots_dir) {
        SHC_ERROR("Can't open the ballots dir %s: %s", ballots_path, strerror(errno));
        return -1;
    }

    int nitems = 0;
    kepaxos_log_item_t *itms = NULL;
    struct dirent *item = readdir(ballots_dir);
    while (item) {
        if (item->d_name[0] != '.' && item->d_type == DT_LNK) {
            uint64_t b = strtoll(item->d_name, NULL, 10);
            if (b && b > ballot) {
                struct stat st;

                // compute the path to this key in the replica log
                size_t kpath_len = ballots_path_len + 1 + strlen(item->d_name) + 1;
                char kpath[kpath_len];
                snprintf(kpath, sizeof(kpath), "%s/%s", ballots_path, item->d_name);

                // get the key
                size_t kfile_path_len = kpath_len + 5;
                char kfile_path[kfile_path_len];
                snprintf(kfile_path, sizeof(kfile_path), "%s/key", kpath);
                if (stat(kfile_path, &st) != 0 || !S_ISREG(st.st_mode)) {
                    SHC_ERROR("Can't stat the key file %s: %s", kfile_path, strerror(errno));
                    item = readdir(ballots_dir);
                    continue;
                }
                FILE *kfile = fopen(kfile_path, "r");
                if (!kfile) {
                    SHC_ERROR("Can't open the key file %s: %s", kfile_path, strerror(errno));
                    item = readdir(ballots_dir);
                    continue;
                }

                // we got a key
                char *key = malloc(st.st_size);
                if (fread(key, st.st_size, 1, kfile) != st.st_size) {
                    SHC_ERROR("Can't read the key file %s: %s", kfile_path, strerror(errno));
                    free(key);
                    fclose(kfile);
                    item = readdir(ballots_dir);
                    continue;

                }
                fclose(kfile);
 
                // get the seq
                size_t sfile_path_len = kpath_len + 5;
                char sfile_path[sfile_path_len];
                snprintf(sfile_path, sizeof(sfile_path), "%s/seq", kpath);
                FILE *sfile = fopen(sfile_path, "r");
                if (!sfile) {
                    SHC_ERROR("Can't read the key file %s: %s", kfile_path, strerror(errno));
                    free(key);
                    fclose(kfile);
                    item = readdir(ballots_dir);
                    continue;
                }
                uint64_t seq = 0;
                if (fread(&seq, sizeof(seq), 1, sfile) != sizeof(seq)) {
                    SHC_ERROR("Can't read the key file %s: %s", kfile_path, strerror(errno));
                    free(key);
                    fclose(kfile);
                    fclose(sfile);
                    item = readdir(ballots_dir);
                    continue;
                }
                fclose(sfile);

                // now that we have everything we can fill in the item to return
                itms = realloc(itms, sizeof(kepaxos_log_item_t) * (nitems + 1));
                kepaxos_log_item_t *item = &itms[nitems++];
                item->ballot = b;
                item->seq = seq;
                item->klen = st.st_size;
                item->key = key;
            }
        }
            
        item = readdir(ballots_dir);
    }
    closedir(ballots_dir);
    *items = itms;
    *num_items = nitems;

    return 0;
}

void kepaxos_release_diff_items(kepaxos_log_item_t *items, int num_items)
{
    int i;
    for (i = 0; i < num_items; i++)
        free(items[i].key);
    free(items);
}

/* vim: tabstop=4 shiftwidth=4 expandtab: */
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
