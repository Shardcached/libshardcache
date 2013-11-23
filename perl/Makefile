MODULES=p5-Groupcache p5-Groupcache-Client

PWD=$(shell pwd)

all:
	@export GROUPCACHE_INCDIR="$(PWD)/../src"; \
	 export GROUPCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; perl Makefile.PL; make; cd -; done

install:
	@export GROUPCACHE_INCDIR="$(PWD)/../src"; \
	 export GROUPCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; make install; cd - ; done

clean:
	@export GROUPCACHE_INCDIR="$(PWD)/../src"; \
	 export GROUPCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; make clean; cd - ; done
