CC=gcc
CFLAGS=-Wall -g -O3 #-DDEBUG-DEREDIS
ZOOKEEPER=$HOME/include/zookeeper

all: ftmemfs

ftmemfs: handle.o ftmemfs.o xxhash.o metadata_cache.o thredis.o
	$(CC) $(CFLAGS) -DTHREADED -lrt -lfuse -lcrypto -leredis -lpthread -lzookeeper_mt -L $(HOME)/lib/ -o ftmemfs ftmemfs.o handle.o xxhash.o thredis.o metadata_cache.o

thredis.o: thredis.c thredis.h
	$(CC) $(CFLAGS) -g -O3 -c -o thredis.o thredis.c

handle.o: handle.c handle.h
	$(CC) $(CFLAGS) -c handle.c -o handle.o

xxhash.o: xxhash.c xxhash.h
	$(CC) $(CFLAGS) -c xxhash.c -o xxhash.o

ftmemfs.o: ftmemfs.c ftmemfs.h
	$(CC) $(CFLAGS) -DTHREADED -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=29 -I. -g -c -o ftmemfs.o ftmemfs.c -I $(HOME)/include/zookeeper

metadata_cache.o: metadata_cache.c metadata_cache.h
	$(CC) $(CFLAGS) -c metadata_cache.c -o metadata_cache.o

clean:
	rm -f *.o ftmemfs
