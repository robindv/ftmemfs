#include "metadata_cache.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>
#include "ftmemfs.h"


metadata_cache_t* mdc_new(uint32_t size, double timeout)
{
    metadata_cache_t *mdc = calloc(1, sizeof(metadata_cache_t));
    mdc->size = size;
    mdc->timeout = timeout;
    mdc->entries = calloc(size, sizeof(metadata_cacheline_t));
    pthread_mutex_init(&mdc->mutex, NULL);
    return mdc;
}

void mdc_destroy(metadata_cache_t* mdc)
{
    free(mdc->entries);
    free(mdc);
    pthread_mutex_destroy(&mdc->mutex);
}

int mdc_search(metadata_cache_t *mdc, char *path, zinode *target)
{
    // return 0;

    pthread_mutex_lock(&mdc->mutex);

    struct timespec current_t;
    struct timespec* start_t;
    clock_gettime(CLOCK_MONOTONIC, &current_t);

    int res = 0;
    int i;

    for(i = mdc->size - 1; i >= 0; i--)
    {
        int position = i + mdc->current;
        position = position >= mdc->size ? position - mdc->size : position;

        if(! mdc->entries[position].valid)
            continue;

        if(strcmp(path, mdc->entries[position].path) != 0)
            continue;

        start_t = &mdc->entries[position].created;
        double duration = ((double)current_t.tv_sec + 1.0e-9*current_t.tv_nsec) - ((double)start_t->tv_sec + 1.0e-9*start_t->tv_nsec);

        if(duration > mdc->timeout)
            continue;

        memcpy(target, &mdc->entries[position].metadata, sizeof(zinode));
        res = 1;
        break;
    }


    pthread_mutex_unlock(&mdc->mutex);
    return res;
}

void mdc_insert(metadata_cache_t* mdc, char *path, zinode *metadata)
{
    pthread_mutex_lock(&mdc->mutex);

    metadata_cacheline_t *line = &mdc->entries[mdc->current];
    clock_gettime(CLOCK_MONOTONIC, &line->created);
    line->valid = 1;
    strcpy(line->path, path);
    memcpy(&line->metadata, metadata, sizeof(zinode));

    mdc->current = mdc->current == mdc->size - 1 ? 0 : mdc->current + 1;

    pthread_mutex_unlock(&mdc->mutex);
}
