MODULES=p5-Shardcache p5-Shardcache-Client

PWD=$(shell pwd)

all:
	@export SHARDCACHE_INCDIR="$(PWD)/../src"; \
	 export SHARDCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; perl Makefile.PL; make; cd -; done

install:
	@export SHARDCACHE_INCDIR="$(PWD)/../src"; \
	 export SHARDCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; make install; cd - ; done

clean:
	@export SHARDCACHE_INCDIR="$(PWD)/../src"; \
	 export SHARDCACHE_LIBDIR="$(PWD)/../"; \
	 for i in $(MODULES); do cd $$i; make clean; cd - ; done
