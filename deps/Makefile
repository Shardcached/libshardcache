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
	    export CFLAGS="$(CFLAGS) -fPIC -DHAVE_UINT64_T"; \
	    if [ -f $$name/Makefile ]; then \
		make -eC $$name; \
		if [ $$? -ne 0 ]; then \
		    echo "Errors building dependencies"; \
		    exit -1; \
		fi; \
		make -eC $$name install; \
		if [ $$? -ne 0 ]; then \
		    echo "Errors installing dependencies"; \
		    exit -1; \
		fi; \
	    elif [ -d $$name/build ]; then \
		cd $$name/build; \
		if [ ! -f Makefile ]; then \
		    ./configure --libdir="$$LIBDIR" --includedir="$$INCDIR"; \
		fi; \
		make ; \
		if [ $$? -ne 0 ]; then \
		    echo "Errors building dependencies"; \
		    exit -1; \
		fi; \
		make install; \
		if [ $$? -ne 0 ]; then \
		    echo "Errors installing dependencies"; \
		    exit -1; \
		fi; \
		cd -; \
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

