TARGETS := dummy_storage_module.storage

CFLAGS += -I../src -g -O3
LDFLAGS += -L..

all: $(TARGETS)

dummy_storage_module.storage: CFLAGS += -fPIC
dummy_storage_module.storage: LDFLAGS += -shared ../libshardcache.a
dummy_storage_module.storage: dummy_storage_module.c ../libshardcache.a
	$(CC) $(CFLAGS) $(LDFLAGS) -o dummy_storage_module.storage dummy_storage_module.c 

clean:
	@rm -f $(TARGETS) *.o
	@rm -fr *.dSYM

