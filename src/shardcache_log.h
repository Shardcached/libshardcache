#ifndef __SHARDCACHE_LOG_H__
#define __SHARDCACHE_LOG_H__

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

#define SHC_ERROR(__fmt, __args...)      do { shardcache_log_message(LOG_ERR,     0, __fmt, ## __args); } while (0)
#define SHC_WARNING(__fmt, __args...)    do { shardcache_log_message(LOG_WARNING, 0, __fmt, ## __args); } while (0)
#define SHC_WARN(__fmt, __args...) WARNING(__fmt, ## __args)
#define SHC_NOTICE(__fmt, __args...)     do { shardcache_log_message(LOG_NOTICE,  0, __fmt, ## __args); } while (0)
#define SHC_INFO(__fmt, __args...)       do { shardcache_log_message(LOG_INFO,    0, __fmt, ## __args); } while (0)
#define SHC_DIE(__fmt, __args...)        do { SHC_ERROR(__fmt, ## __args); exit(-1); } while (0)

#define __SHC_DEBUG(__n, __fmt, __args...)  do { if (shardcache_log_level() >= LOG_DEBUG + __n) \
    shardcache_log_message(LOG_DEBUG,   __n + 1, __fmt, ## __args); } while (0)

#define SHC_DEBUG(__fmt, __args...)  __SHC_DEBUG(0, __fmt, ## __args)
#define SHC_DEBUG1(__fmt, __args...) SHC_DEBUG(__fmt, ## __args)
#define SHC_DEBUG2(__fmt, __args...) __SHC_DEBUG(1, __fmt, ## __args)
#define SHC_DEBUG3(__fmt, __args...) __SHC_DEBUG(2, __fmt, ## __args)
#define SHC_DEBUG4(__fmt, __args...) __SHC_DEBUG(3, __fmt, ## __args)
#define SHC_DEBUG5(__fmt, __args...) __SHC_DEBUG(4, __fmt, ## __args)

#endif

// vim: tabstop=4 shiftwidth=4 expandtab:
/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
