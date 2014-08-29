TARGETS := shardcachec shc_benchmark st_benchmark

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

all: $(TARGETS)

dynamic: CFLAGS += -fPIC -I../src -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g
dynamic: shardcachec.c shc_benchmark.c
	$(CC) shardcachec.c $(CFLAGS) $(LDFLAGS)  -o shardcachec -lshardcache
	$(CC) shc_benchmark.c $(CFLAGS) $(LDFLAGS) -o shc_benchmark -lshardcache

shardcachec: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
shardcachec: shardcachec.c $(DEPS)
	$(CC) shardcachec.c $(CFLAGS) $(DEPS) $(LDFLAGS) -o shardcachec

shc_benchmark: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3 -g
shc_benchmark: shc_benchmark.c $(DEPS)
	$(CC) shc_benchmark.c $(CFLAGS) $(DEPS) $(LDFLAGS) -o shc_benchmark

st_benchmark: CFLAGS += -fPIC -I../src -I../deps/.incs -Isrc -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g -std=c99
st_benchmark: st_benchmark.c $(DEPS)
	$(CC) st_benchmark.c $(CFLAGS) $(DEPS) $(LDFLAGS) -o st_benchmark

clean:
	rm -f $(TARGETS)
	rm -fr *.o *.dSYM

