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

all: shardcachec shc_benchmark

dynamic: CFLAGS += -fPIC -I../src -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g
dynamic: shardcachec.c shc_benchmark.c
	gcc shardcachec.c $(CFLAGS) $(LDFLAGS)  -o shardcachec -lshardcache
	gcc shc_benchmark.c $(CFLAGS) $(LDFLAGS) -o shc_benchmark -lshardcache

shardcachec: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
shardcachec: shardcachec.c $(DEPS)
	gcc shardcachec.c $(CFLAGS) $(DEPS) $(LDFLAGS) -o shardcachec

shc_benchmark: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
shc_benchmark: shc_benchmark.c $(DEPS)
	gcc shc_benchmark.c $(CFLAGS) $(DEPS) $(LDFLAGS) -o shc_benchmark

clean:
	rm -f shardcachec
	rm -f shc_benchmark

