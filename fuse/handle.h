// #include <hiredis.h>
#include <stdint.h>
#include <semaphore.h>


#pragma once
#define MAX_SIZE (1024 * 1024)
#define BUFFERED_CHUNKS 2
#define MAX_CHUNK (1024 * 1024)
#define HIDDEN "."
#define PREFETCH_CHUNKS 64
#define N_WORKER_THREADS_WRITE 8
#define N_WORKER_THREADS_READ 4
#define REDIS_SERVER_POOL_SIZE 1

typedef struct{
    int index;
    int use;
    size_t written_bytes;
    char filename[512];
    size_t f_size;
    char *local_buf;
    int pref_chunk;
    zinode metadata;
    char lpath[550];
} handle_t;

typedef struct{
    handle_t **handles;
    size_t num;
}handle_pool_t;

handle_pool_t *handle_pool_new(redisfs_opt_t *opt);
void handle_pool_free(handle_pool_t *);
handle_t *handle_get(handle_pool_t *pool);
void handle_release(handle_pool_t *pool, unsigned int index);
void handle_lock();
void handle_unlock();

//Locally check if the value exists (before checking remote)
//char * get_value(redisContext **redis_contexts, char *key, size_t keylen, size_t *vallen, redisReply *rc);
