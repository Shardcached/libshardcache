DEPS=https://github.com/dgryski/libchash \
     https://github.com/xant/libhl \
     https://github.com/xant/libiomux \
     https://github.com/xant/siphash-c \
     https://github.com/xant/libut.git

.PHONY: all clean

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	UNAME=`uname`; \
	if [ "$$UNAME" = "Darwin" ]; then \
	    PLATFORM_CFLAGS="-DHAVE_KQUEUE"; \
	elif [ "$$UNAME" = "Linux" ]; then \
	    KERNEL_VERSION=`uname -r | cut -d- -f1`; \
	    MIN_VERSION=2.6.27; \
	    if [[ $$KERNEL_VERSION > $$MIN_VERSION ]]; then \
		PLATFORM_CFLAGS="-DHAVE_EPOLL"; \
	    elif [ $$KERNEL_VERSION = $$MIN_VERSION ]; then \
		PLATFORM_CFLAGS="-DHAVE_EPOLL"; \
	    fi; \
	fi; \
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
	    export CFLAGS="$(CFLAGS) -DHAVE_UINT64_T $$PLATFORM_CFLAGS"; \
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

