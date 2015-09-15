UNAME := $(shell uname)
LIBSHARDCACHE_DIR := $(shell pwd)

DEPS = deps/.libs/libhl.a \
       deps/.libs/libchash.a \
       deps/.libs/libiomux.a \
       deps/.libs/libsiphash.a

LDFLAGS += -L. -ldl
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

ifeq ("$(SHARDCACHE_INSTALL_PREFIX)", "")
SHARDCACHE_INSTALL_PREFIX=/usr/local
endif

ifeq ("$(SHARDCACHE_INSTALL_LIBDIR)", "")
ifneq ("$(LIBDIR)", "")
SHARDCACHE_INSTALL_LIBDIR="$(LIBDIR)"
else
SHARDCACHE_INSTALL_LIBDIR="$(SHARDCACHE_INSTALL_PREFIX)/lib"
endif
endif

ifeq ("$(SHARDCACHE_INSTALL_INCDIR)", "")
ifneq ("$(INCDIR)", "")
SHARDCACHE_INSTALL_INCDIR="$(INCDIR)"
else
SHARDCACHE_INSTALL_INCDIR="$(SHARDCACHE_INSTALL_PREFIX)/include"
endif
endif

ifeq ("$(SHARDCACHE_INSTALL_BINDIR)", "")
SHARDCACHE_INSTALL_BINDIR="$(SHARDCACHE_INSTALL_PREFIX)/bin"
endif

IS_CLANG := $(shell $(CC) --version | grep clang)

TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))
TESTS = $(patsubst %.c, %, $(wildcard test/*.c))

TEST_EXEC_ORDER = kepaxos_test shardcache_test

all: CFLAGS += -Ideps/.incs  -DBUILD_INFO="$(BUILD_INFO)"
all: $(DEPS) objects static shared

tsan:
	@export CC=gcc-4.8; \
	export LDFLAGS="-pie -ltsan"; \
	export CFLAGS="-fsanitize=thread -g -fPIC -pie"; \
	make clean; \
	make test

.PHONY: build_deps
build_deps:
	@make -eC deps all

.PHONY: static
static: $(DEPS) objects
	ar -r libshardcache.a src/*.o

.PHONY: static-nodeps
static-nodeps: objects
	ar -r libshardcache.a src/*.o

standalone: $(DEPS) objects
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

shared: $(DEPS) objects
	$(CC) src/*.o $(LDFLAGS) $(DEPS) $(SHAREDFLAGS) -o libshardcache.$(SHAREDEXT)

dynamic: LDFLAGS += -lhl -lchash -liomux -lsiphash
dynamic: objects
	 $(CC) src/*.o $(LDFLAGS) $(SHAREDFLAGS) -o libshardcache.$(SHAREDEXT)

$(DEPS): build_deps

objects: $(TARGETS)

EXTRA_CFLAGS=-Wno-parentheses -Wno-pointer-sign


ifneq ("$(IS_CLANG)", "")
SQLITE_CFLAGS=-Wno-array-bounds -Wno-unused-const-variable -Wno-unknown-warning-option -DSQLITE_THREADSAFE=1
else
GCC_ISOLD := $(shell gcc --version  | head -1 | awk '{ if ("$3" < "4.3") print YES }')
ifneq ($(GCC_ISOLD), "")
SQLITE_CFLAGS=-fno-strict-aliasing -DSQLITE_THREADSAFE=1
else
SQLITE_CFLAGS=-Wno-array-bounds -fno-strict-aliasing -DSQLITE_THREADSAFE=1
endif
endif

$(TARGETS): CFLAGS += -fPIC -Isrc -Wall $(EXTRA_CFLAGS) $(SQLITE_CFLAGS) -g -O3

.PHONY: utils
utils: 
	@make -eC utils all

.PHONY: utils-dynamic
utils-dynamic: 
	@make -eC utils dynamic

clean:
	rm -f src/*.o
	rm -f test/*_test
	rm -f libshardcache.a
	rm -f libshardcache.$(SHAREDEXT)
	make -C deps clean
	make -C utils clean

.PHONY: buld_tests
build_tests: CFLAGS += -Isrc -Ideps/.incs -Wall -Werror -g -O3
build_tests: static shared
	@for i in $(TESTS); do\
	  if [ "X$(UNAME)" = "XDarwin" ]; then \
	      echo "$(CC) $(CFLAGS) $(EXTRA_CFLAGS) $(SQLITE_CFLAGS) $$i.c -o $$i -lshardcache -lhl -lsiphash -liomux -lchash $(LDFLAGS) deps/.libs/libut.a -lm";\
	      $(CC) $(CFLAGS) $(EXTRA_CFLAGS) $(SQLITE_CFLAGS) $$i.c -o $$i -Ldeps/.libs -lshardcache -lhl -lsiphash -liomux -lchash -lut $(LDFLAGS) -lm;\
	  else \
	      echo "$(CC) $(CFLAGS) $$i.c -o $$i libshardcache.a $(LDFLAGS) $(DEPS)  deps/.libs/libut.a -lm";\
	      $(CC) $(CFLAGS) $$i.c -o $$i libshardcache.a $(DEPS) deps/.libs/libut.a $(LDFLAGS) -lm;\
	  fi;\
	done;\

.PHONY: test
test: build_tests
	@for i in $(TEST_EXEC_ORDER); do \
	    echo; \
	    if [ "X$(UNAME)" = "XDarwin" ]; then \
		DYLD_LIBRARY_PATH=".:deps/.libs" test/$$i; \
	    else \
		test/$$i; \
	    fi; \
	    echo; \
	done

perl_install:
	make -C perl install

perl_clean:
	make -C perl clean

perl_build:
	make -C perl all

.PHONY: install
install:
	 @echo "Installing libraries in $(SHARDCACHE_INSTALL_LIBDIR)"; \
	 cp -v libshardcache.a "$(SHARDCACHE_INSTALL_LIBDIR)"/;\
	 cp -v libshardcache.$(SHAREDEXT) "$(SHARDCACHE_INSTALL_LIBDIR)/";\
	 echo "Installing headers in $(SHARDCACHE_INSTALL_INCDIR)"; \
	 cp -v src/shardcache.h "$(SHARDCACHE_INSTALL_INCDIR)/"; \
	 cp -v src/shardcache_client.h "$(SHARDCACHE_INSTALL_INCDIR)/"; \
	 cp -v src/shardcache_node.h "$(SHARDCACHE_INSTALL_INCDIR)/"; \
	 cp -v src/shardcache_storage.h "$(SHARDCACHE_INSTALL_INCDIR)/"; \
	 cp -v src/shardcache_log.h "$(SHARDCACHE_INSTALL_INCDIR)/";

.PHONY: install_utils
install_utils:
	@echo "Install utils in $(SHARDCACHE_INSTALL_BINDIR)"; \
	cp -v utils/shardcachec "$(SHARDCACHE_INSTALL_BINDIR)/"; \
	cp -v utils/shc_benchmark "$(SHARDCACHE_INSTALL_BINDIR)/";

