//#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pwd.h>
#include <unistd.h>
#include <assert.h>
#include "ftmemfs.h"
#include "handle.h"
//#include <hiredis.h>
#include <semaphore.h>

static pthread_mutex_t handles_mutex = PTHREAD_MUTEX_INITIALIZER;

handle_pool_t *handle_pool_new(redisfs_opt_t *opt)
{
    int i;
    handle_pool_t *pool;
    pool = (handle_pool_t*)malloc(sizeof(handle_pool_t));
    if (!pool)
        return NULL;

    pool->num = opt->maxhandle;
    pool->handles = (handle_t **)malloc(sizeof(handle_t *) * pool->num);
    if (! pool->handles)
        return NULL;

    for (i = 0; i < pool->num; i++)
    {
        pool->handles[i] = (handle_t *)malloc(sizeof(handle_t));
        if(!pool->handles[i])
            return NULL;

        memset(pool->handles[i], 0, sizeof(handle_t));
        pool->handles[i]->index = i;
        pool->handles[i]->local_buf = NULL;
        pool->handles[i]->use = 0;
    }
    return pool;
}

void handle_pool_free(handle_pool_t *pool)
{
    int i;
    for(i = 0; i < pool->num; i++)
        free(pool->handles[i]);

    free(pool->handles);
    free(pool);
}

/*
static void debug_statement(const char *output)
{
    struct passwd *p = getpwuid(getuid());
    if (p == NULL) return ;
    char file_name[2048];
    sprintf(file_name, "/home/%s/threads.txt", p->pw_name);
    FILE *f = fopen(file_name, "a");
    if (f == NULL) return;
    fprintf(f, output);
    fclose(f);
}*/

handle_t *handle_get(handle_pool_t *pool)
{
    int i;
    handle_t *ret = NULL;

    pthread_mutex_lock(&handles_mutex);
    //char s[512];
    //sprintf(s, "handle_get pool num %zu\n", pool->num);
    //debug_statement(s);

    for(i = 0; i < pool->num; i++) {
        if(!pool->handles[i]->use) {
            ret = pool->handles[i];
            ret->written_bytes = 0;
            if (ret->local_buf == NULL) {
                ret->local_buf = (char *) malloc(MAX_CHUNK * sizeof(char));
            }
            ret->pref_chunk = -1;
            ret->use = 1;
            ret->f_size = 0;
            break;
        }
    }
    if (ret == NULL)
    {
        pthread_mutex_unlock(&handles_mutex);
        return ret;
    }

    pthread_mutex_unlock(&handles_mutex);

    return ret;
}

void handle_lock()
{
    pthread_mutex_lock(&handles_mutex);
}

void handle_unlock()
{
    pthread_mutex_unlock(&handles_mutex);
}

void handle_release(handle_pool_t *pool, unsigned int index)
{
    pthread_mutex_lock(&handles_mutex);
    pool->handles[index]->written_bytes = 0;
    pool->handles[index]->use = 0;
    pool->handles[index]->lpath[0] = '\0';
    pthread_mutex_unlock(&handles_mutex);
    return;
}
