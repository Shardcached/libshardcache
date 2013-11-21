UNAME := $(shell uname)

LDFLAGS += -L. $(LIBGROUPCACHE_DIR)/deps/.libs/libhl.a $(LIBGROUPCACHE_DIR)/deps/.libs/libchash.a $(LIBGROUPCACHE_DIR)/libgroupcache.a

ifeq ($(UNAME), Linux)
LDFLAGS +=
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

ifeq ($(UNAME), Darwin)
SHAREDFLAGS = -dynamiclib
SHAREDEXT = dylib
else
SHAREDFLAGS = -shared
SHAREDEXT = so
endif


#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))

all: $(LIBGROUPCACHE_DIR)/libgroupcache.a objects groupcached

$(LIBGROUPCACHE_DIR)/libgroupcache.a:
	make -C $(LIBGROUPCACHE_DIR) static

groupcached: objects
	gcc $(LDFLAGS) src/*.o -o groupcached

objects: CFLAGS += -fPIC -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f groupcached
