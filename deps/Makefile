DEPS=https://github.com/dgryski/libchash \
     https://github.com/xant/libhl \
     https://github.com/xant/libiomux \
     https://github.com/xant/siphash-c

.PHONY: all update purge clean

all:
	@mkdir -p .libs; \
	mkdir -p .incs; \
	for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ ! -d "$$name" ]; then \
		git clone $$i; \
	    fi; \
	    export LIBDIR="$(shell pwd)/.libs"; \
	    export INCDIR="$(shell pwd)/.incs"; \
	    export CFLAGS="$(CFLAGS) -DHAVE_UINT64_T"; \
	    make -eC $$name; \
	    if [ $$? -ne 0 ] ; then exit $$?; fi; \
	    make -eC $$name install; \
	done

update:
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
	        cd $$name;\
		git pull;\
		cd -;\
	    fi;\
	done

purge: clean
	@for i in $(DEPS); do \
	    name=`echo $$i | sed 's,.*/,,;s/.git$$//' `; \
	    if [ -d "$$name" ]; then \
		rm -rf $$name; \
	    fi;\
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

