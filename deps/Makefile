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
	    if [ ! -f $$name/.git ]; then \
		cd ..;\
		git submodule init deps/$$name;\
		git submodule update;\
		cd -;\
	    fi;\
	    export LIBDIR="$(shell pwd)/.libs"; \
	    export INCDIR="$(shell pwd)/.incs"; \
	    export CFLAGS="$(CFLAGS) -DHAVE_UINT64_T"; \
	    if [ $$? -ne 0 ] ; then exit $$?; fi; \
	    if [ -f $$name/Makefile ]; then \
		make -eC $$name; \
		make -eC $$name install; \
	    elif [ -d $$name/build ]; then \
		cd $$name/build; \
		if [ ! -f Makefile ]; then \
		    ./configure --libdir="$$LIBDIR" --includedir="$$INCDIR"; \
		fi; \
		make ; \
		make install; \
		cd -;\
	    else \
		echo "Don't know how to build $$name"; \
		exit -1; \
	    fi; \
	done

clean:
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
		if [ -f $$name/Makefile ]; then \
		    make -C $$name clean; \
		elif [ -f $$name/build/Makefile ]; then \
		    make -C $$name/build clean; \
		fi; \
	    fi; \
	done;
	@rm -rf "$(shell pwd)/.libs";
	@rm -rf "$(shell pwd)/.incs";

