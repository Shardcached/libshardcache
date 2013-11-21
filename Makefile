UNAME := $(shell uname)

LDFLAGS += -L. deps/.libs/libhl.a deps/.libs/libchash.a

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


#CC = gcc
TARGETS = $(patsubst %.c, %.o, $(wildcard src/*.c))
TESTS = $(patsubst %.c, %, $(wildcard test/*.c))

TEST_EXEC_ORDER = 

all: build_deps objects static shared

build_deps:
	@make -C deps all

static: objects
	ar -r libgroupcache.a src/*.o

shared: objects
	$(CC) $(LDFLAGS) $(SHAREDFLAGS) src/*.o -o libgroupcache.$(SHAREDEXT)

objects: CFLAGS += -fPIC -Isrc -Ideps/.incs -Wall -Werror -Wno-parentheses -Wno-pointer-sign -DTHREAD_SAFE -O3
objects: $(TARGETS)

clean:
	rm -f src/*.o
	rm -f test/*_test
	rm -f libgroupcache.a
	rm -f libgroupcache.$(SHAREDEXT)
	rm -f support/testing.o
	make -C deps clean

support/testing.o:
	$(CC) $(CFLAGS) -Isrc -c support/testing.c -o support/testing.o

tests: CFLAGS += -Isrc -Isupport -Wall -Werror -Wno-parentheses -Wno-pointer-sign -DTHREAD_SAFE -O3

tests: support/testing.o static
	@for i in $(TESTS); do\
	  echo "$(CC) $(CFLAGS) $$i.c -o $$i libgroupcache.a $(LDFLAGS) -lm";\
	  $(CC) $(CFLAGS) $$i.c -o $$i libgroupcache.a support/testing.o $(LDFLAGS) -lm;\
	done;\
	for i in $(TEST_EXEC_ORDER); do echo; test/$$i; echo; done

install:
	@if [ "X$$LIBDIR" == "X" ]; then LIBDIR="/usr/local/lib"; fi; \
	 if [ "X$$INCDIR" == "X" ]; then INCDIR="/usr/local/include"; fi; \
	 echo "Installing libraries in $$LIBDIR"; \
	 cp -v libgroupcache.a $$LIBDIR/;\
	 cp -v libgroupcache.$(SHAREDEXT) $$LIBDIR/;\
	 echo "Installing headers in $$INCDIR"; \
	 cp -v src/groupcache.h $$INCDIR/;

