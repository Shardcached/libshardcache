DEPS=https://github.com/xant/libchash \
     https://github.com/xant/libhl \
     https://github.com/xant/libiomux

LIBDIR=$(shell pwd)/.libs
INCDIR=$(shell pwd)/.incs

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ ! -d $$name ]; then \
		git clone $$i; \
	    fi; \
	    cd $$name; \
	    make; \
	    make install; \
	    cd -; \
	done

update:
	@for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ -d $$name ]; then \
	        cd $$name;\
		git pull;\
		cd -;\
	    fi;\
	done

clean:
	@for i in $(DEPS); do \
	    name=`echo $$i | cut -d/ -f5 `; \
	    if [ -d $$name ]; then \
		cd $$name; \
		make clean; \
		cd -; \
	    fi; \
	done;
	@rm -rf $(LIBDIR);
	@rm -rf $(INCDIR);

