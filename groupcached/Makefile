UNAME := $(shell uname)

LDFLAGS += -L. $(LIBGROUPCACHE_DIR)/deps/.libs/libiomux.a \
	   $(LIBGROUPCACHE_DIR)/deps/.libs/libhl.a \
	   $(LIBGROUPCACHE_DIR)/deps/.libs/libchash.a \
	   $(LIBGROUPCACHE_DIR)/libgroupcache.a

ifeq ($(UNAME), Linux)
LDFLAGS +=
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: $(LIBGROUPCACHE_DIR)/libgroupcache.a objects groupcached

$(LIBGROUPCACHE_DIR)/libgroupcache.a:
	make -C $(LIBGROUPCACHE_DIR) static

groupcached: objects
	gcc $(LDFLAGS) src/*.o -o groupcached

objects: CFLAGS += -fPIC -I$(LIBGROUPCACHE_DIR)/src -I$(LIBGROUPCACHE_DIR)/deps/.incs -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f groupcached
