UNAME := $(shell uname)

LDFLAGS += $(LIBSHARDCACHE_DIR)/libshardcache.a \
	   $(LIBSHARDCACHE_DIR)/deps/.libs/libiomux.a \
	   $(LIBSHARDCACHE_DIR)/deps/.libs/libhl.a \
	   $(LIBSHARDCACHE_DIR)/deps/.libs/libchash.a \
	   $(LIBSHARDCACHE_DIR)/deps/.libs/libsiphash.a \
	   -L. 

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread -ldl
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: $(LIBSHARDCACHE_DIR)/libshardcache.a objects shardcached

$(LIBSHARDCACHE_DIR)/libshardcache.a:
	make -C $(LIBSHARDCACHE_DIR) static

shardcached: objects
	gcc src/*.o $(LDFLAGS) -o shardcached

objects: CFLAGS += -fPIC -I$(LIBSHARDCACHE_DIR)/src -I$(LIBSHARDCACHE_DIR)/deps/.incs -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f shardcached
