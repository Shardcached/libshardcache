#ifndef SHARDCACHE_LOG_H
#define SHARDCACHE_LOG_H

/*
 *******************************************************************************
 * LOG API 
 *******************************************************************************
 */

/**
 * @brief Initialize the internal log subsystem
 * @param ident The ident used in syslog messages
 * @param loglevel The loglevel to use
 */
void shardcache_log_init(char *ident, int loglevel);

/**
 * @brief Returns the actually configured log lovel
 * @return The loglevel configured at initialization time
 */
unsigned int shardcache_log_level();

/**
 * @brief Log a message at the specified prio and for the specified loglevel
 * @param prio The priority of the message
 * @param dbglevel The debuglevel of this message
 * @param fmt The message
 */
void shardcache_log_message(int prio, int dbglevel, const char *fmt, ...);

/**
 * @brief Convert a binary buffer to an hexstring
 * @param buf The buffer
 * @param len The size of the input buffer
 * @param limit Don't output more than 'limit' bytes
 * @param use_prefix instruct if using the 0x prefix or not
 */
char *shardcache_hex_escape(char *buf, int len, int limit, int use_prefix);

/**
 * @brief Escape all occurences of a specific byte using the provided escape character
 * @param ch     The byte to escape
 * @param esc    The escape byte to use when 'ch' is encountered
 * @param buffer The input buffer to scan
 * @param len    The size of the input buffer
 * @param dest   Where to store the escaped string
 * @param newlen The size of the escaped string
 * @return The number of input bytes scanned
 * @note  The caller MUST release the memory used of the output string when done
 */
unsigned long shardcache_byte_escape(char ch, char esc, char *buffer, unsigned long len, char **dest, unsigned long *newlen);

#define SHC_ERROR(_fmt, _args...)      do { shardcache_log_message(LOG_ERR,     0, _fmt, ## _args); } while (0)
#define SHC_WARNING(_fmt, _args...)    do { shardcache_log_message(LOG_WARNING, 0, _fmt, ## _args); } while (0)
#define SHC_WARN(_fmt, _args...) WARNING(_fmt, ## _args)
#define SHC_NOTICE(_fmt, _args...)     do { shardcache_log_message(LOG_NOTICE,  0, _fmt, ## _args); } while (0)
#define SHC_INFO(_fmt, _args...)       do { shardcache_log_message(LOG_INFO,    0, _fmt, ## _args); } while (0)
#define SHC_DIE(_fmt, _args...)        do { SHC_ERROR(_fmt, ## _args); exit(-1); } while (0)

#define _SHC_DEBUG(_n, _fmt, _args...)  do { if (shardcache_log_level() >= LOG_DEBUG + _n) \
    shardcache_log_message(LOG_DEBUG,   _n + 1, _fmt, ## _args); } while (0)

#define SHC_DEBUG(_fmt, _args...)  _SHC_DEBUG(0, _fmt, ## _args)
#define SHC_DEBUG1(_fmt, _args...) SHC_DEBUG(_fmt, ## _args)
#define SHC_DEBUG2(_fmt, _args...) _SHC_DEBUG(1, _fmt, ## _args)
#define SHC_DEBUG3(_fmt, _args...) _SHC_DEBUG(2, _fmt, ## _args)
#define SHC_DEBUG4(_fmt, _args...) _SHC_DEBUG(3, _fmt, ## _args)
#define SHC_DEBUG5(_fmt, _args...) _SHC_DEBUG(4, _fmt, ## _args)

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
