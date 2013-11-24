UNAME := $(shell uname)
LIBGROUPCACHE_DIR := $(shell pwd)

LDFLAGS += -L. deps/.libs/libhl.a deps/.libs/libchash.a deps/.libs/libiomux.a

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


#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))
TESTS = $(patsubst %.c, %, $(wildcard test/*.c))

TEST_EXEC_ORDER = 

all: build_deps objects static shared groupcache_daemon

build_deps:
	@make -C deps all

update_deps:
	@make -C deps update

purge_deps:
	@make -C deps purge

groupcache_daemon:
	@LIBGROUPCACHE_DIR="`pwd`" make -C groupcached all

static: objects
	ar -r libgroupcache.a src/*.o

standalone: objects
	@cwd=`pwd`; \
	dir="/tmp/libgroupcache_build$$$$"; \
	mkdir $$dir; \
	cd $$dir; \
	ar x $$cwd/deps/.libs/libchash.a; ar x $$cwd/deps/.libs/libhl.a; \
	cd $$cwd; \
	ar -r libgroupcache.a $$dir/*.o src/*.o; \
	rm -rf $$dir

shared: objects
	$(CC) src/*.o $(LDFLAGS) $(SHAREDFLAGS) -o libgroupcache.$(SHAREDEXT)

objects: CFLAGS += -fPIC -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f test/*_test
	rm -f libgroupcache.a
	rm -f libgroupcache.$(SHAREDEXT)
	rm -f support/testing.o
	make -C deps clean
	make -C groupcached clean

support/testing.o:
	$(CC) $(CFLAGS) -Isrc -c support/testing.c -o support/testing.o

tests: CFLAGS += -Isrc -Isupport -Wall -Werror -Wno-parentheses -Wno-pointer-sign -O3

tests: support/testing.o static
	@for i in $(TESTS); do\
	  echo "$(CC) $(CFLAGS) $$i.c -o $$i libgroupcache.a $(LDFLAGS) -lm";\
	  $(CC) $(CFLAGS) $$i.c -o $$i libgroupcache.a support/testing.o $(LDFLAGS) -lm;\
	done;\
	for i in $(TEST_EXEC_ORDER); do echo; test/$$i; echo; done

perl_install:
	make -C perl install

perl_clean:
	make -C perl clean

perl_build:
	make -C perl all

install:
	 @echo "Installing libraries in $(LIBDIR)"; \
	 cp -v libgroupcache.a $(LIBDIR)/;\
	 cp -v libgroupcache.$(SHAREDEXT) $(LIBDIR)/;\
	 echo "Installing headers in $(INCDIR)"; \
	 cp -v src/groupcache.h $(INCDIR)/; \

