UNAME := $(shell uname)
LIBSHARDCACHE_DIR := $(shell pwd)

DEPS = deps/.libs/libhl.a \
       deps/.libs/libchash.a \
       deps/.libs/libiomux.a \
       deps/.libs/libsiphash.a

LDFLAGS += -L.
ifeq ($(UNAME), Linux)
LDFLAGS += -pthread
else
LDFLAGS +=
endif

ifeq ($(UNAME), Darwin)
SHAREDFLAGS = -dynamiclib
SHAREDEXT = dylib
else
SHAREDFLAGS = -shared
SHAREDEXT = so
endif

ifeq ("$(LIBDIR)", "")
LIBDIR=/usr/local/lib
endif

ifeq ("$(INCDIR)", "")
INCDIR=/usr/local/include
endif

ifeq ("$(SHARDCACHE_INSTALL_LIBDIR)", "")
SHARDCACHE_INSTALL_LIBDIR=$(LIBDIR)
endif

ifeq ("$(SHARDCACHE_INSTALL_INCDIR)", "")
SHARDCACHE_INSTALL_INCDIR=$(INCDIR)
endif

TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))
TESTS = $(patsubst %.c, %, $(wildcard test/*.c))

TEST_EXEC_ORDER = 

allu: objects static shared

tsan:
	@export CC=gcc-4.8; \
	export LDFLAGS="-pie -ltsan"; \
	export CFLAGS="-fsanitize=thread -g -fPIC -pie"; \
	make all

.PHONY: build_deps
build_deps:
	@make -eC deps all;

update_deps:
	@make -C deps update

purge_deps:
	@make -C deps purge

static:  objects
	ar -r libshardcache.a src/*.o

standalone: objects
	@cwd=`pwd`; \
	dir="/tmp/libshardcache_build$$$$"; \
	mkdir $$dir; \
	cd $$dir; \
	ar x deps/.libs/libchash.a; \
	ar x deps/.libs/libhl.a; \
	ar x deps/.libs/libiomux.a; \
	ar x deps/.libs/libsiphash.a; \
	cd $$cwd; \
	ar -r libshardcache_standalone.a $$dir/*.o src/*.o; \
	rm -rf $$dir

shared: objects
	$(CC) src/*.o $(LDFLAGS) $(DEPS) $(SHAREDFLAGS) -o libshardcache.$(SHAREDEXT)

$(DEPS): build_deps

objects: CFLAGS += -fPIC -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g
objects: $(DEPS) $(TARGETS)

clean:
	rm -f src/*.o
	rm -f test/*_test
	rm -f libshardcache.a
	rm -f libshardcache.$(SHAREDEXT)
	rm -f support/testing.o
	make -C deps clean

support/testing.o:
	$(CC) $(CFLAGS) -Isrc -c support/testing.c -o support/testing.o

tests: CFLAGS += -Isrc -Isupport -Wall -Werror -Wno-parentheses -Wno-pointer-sign -g

tests: support/testing.o static
	@for i in $(TESTS); do\
	  echo "$(CC) $(CFLAGS) $$i.c -o $$i libshardcache.a $(LDFLAGS) $(DEPS) -lm";\
	  $(CC) $(CFLAGS) $$i.c -o $$i libshardcache.a support/testing.o $(LDFLAGS) -lm;\
	done;\
	for i in $(TEST_EXEC_ORDER); do echo; test/$$i; echo; done

perl_install:
	make -C perl install

perl_clean:
	make -C perl clean

perl_build:
	make -C perl all

install:
	 @echo "Installing libraries in $(SHARDCACHE_INSTALL_LIBDIR)"; \
	 cp -v libshardcache.a $(SHARDCACHE_INSTALL_LIBDIR)/;\
	 cp -v libshardcache.$(SHAREDEXT) $(SHARDCACHE_INSTALL_LIBDIR)/;\
	 echo "Installing headers in $(SHARDCACHE_INSTALL_INCDIR)"; \
	 cp -v src/shardcache.h $(SHARDCACHE_INSTALL_INCDIR)/; \

