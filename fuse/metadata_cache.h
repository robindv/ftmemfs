#ifndef METADATA_CACHE_H
#define METADATA_CACHE_H
#include <stdint.h>
#include <pthread.h>
#include "ftmemfs.h"

typedef struct {
    char path[255];
    struct timespec created;
    zinode metadata;
    int valid;

} metadata_cacheline_t;

typedef struct {
    metadata_cacheline_t *entries;
    uint32_t size;
    pthread_mutex_t mutex;
    uint32_t current;
    double timeout;

} metadata_cache_t;

int mdc_search(metadata_cache_t *mdc, char *path, zinode *target);
metadata_cache_t* mdc_new(uint32_t size, double timeout);
void mdc_destroy(metadata_cache_t* mdc);
void mdc_insert(metadata_cache_t* mdc, char *path, zinode *metadata);

#endif
