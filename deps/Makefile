UNAME := $(shell uname)

EXTRA_CFLAGS += -DHAVE_UINT64_T
ifeq ($(UNAME), Linux)
EPOLL_CHECK := $(shell ls /usr/include/sys/timerfd.h)
ifdef EPOLL_CHECK
EXTRA_CFLAGS += -DHAVE_EPOLL
endif
else
ifeq ($(UNAME), Darwin)
EXTRA_CFLAGS += -DHAVE_KQUEUE
endif
endif

DEPS=https://github.com/dgryski/libchash \
     https://github.com/xant/libhl \
     https://github.com/xant/libiomux \
     https://github.com/xant/siphash-c \
     https://github.com/xant/libut.git

.PHONY: all clean

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ ! -f $$name/Makefile ]; then \
		cd ..;\
		git submodule init deps/$$name;\
		git submodule update;\
		cd -;\
	    fi;\
	    export LIBDIR="$(shell pwd)/.libs"; \
	    export INCDIR="$(shell pwd)/.incs"; \
	    export CFLAGS="$(CFLAGS) $(EXTRA_CFLAGS)"; \
	    make -eC $$name; \
	    if [ $$? -ne 0 ] ; then exit $$?; fi; \
	    make -eC $$name install; \
	done

clean:
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
		make -C $$name clean; \
	    fi; \
	done;
	@rm -rf "$(shell pwd)/.libs";
	@rm -rf "$(shell pwd)/.incs";

