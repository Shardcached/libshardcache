UNAME := $(shell uname)

DEPS += ../libshardcache.a \
        ../deps/.libs/libiomux.a \
        ../deps/.libs/libhl.a \
        ../deps/.libs/libchash.a \
        ../deps/.libs/libsiphash.a

LDFLAGS += -L. -ldl

ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
CFLAGS += -Wno-deprecated-declarations
endif

all: shardcachec

shardcachec: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
shardcachec:
	gcc shardcachec.c $(CFLAGS) $(LDFLAGS) $(DEPS) -o shardcachec

clean:
	rm -f shardcachec

