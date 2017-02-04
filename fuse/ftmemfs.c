#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fuse/fuse.h>
#include <sys/time.h>
#include "ftmemfs.h"
#include "handle.h"
#include <pthread.h>
#include <pwd.h>
#include <inttypes.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/resource.h>
#include "xxhash.h"
#include <stdint.h>
#include "xxhash.h"
#include "semaphore.h"
#include "metadata_cache.h"
#include "zookeeper.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifdef EREDIS
#include <eredis.h>
#else
#include "thredis.h"
#endif


static zhandle_t *zh;

#define MDC_SIZE 16
#define MDC_TIMEOUT 1.0

rserver *servers = NULL;
uint32_t num_servers = 0;
nodeset *nodeclasses = NULL;
uint32_t num_nodeclasses = 0;

/* default options */
redisfs_opt_t opt = {
    .host = NULL,
    .port = "11211",
    .verbose = 0,
    .maxhandle = 64000,
};

int show_times = 0;
int do_lock = 0;
int zoo_connected = 0;

int initialSizeFiles = 1024000;
typedef struct FileCount {
    char * filename;
    short count;
}FileCount;

metadata_cache_t *cache;

struct timeval t1, t2;
handle_pool_t *pool;
/*The server list for hrw*/


/*Mutex that stops all threads from executing operations in order to reconfigure*/
pthread_mutex_t mut_block  = PTHREAD_MUTEX_INITIALIZER;
/*Condition that stops all threads from executing operations, in order to reconfigure*/
pthread_cond_t cond_block  = PTHREAD_COND_INITIALIZER;

void watcher_fuse_block(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);

int fuse_block = 0;

sem_t sem_configuration;

inline void remove_trailing_slash(char * path)
{
     int len = strlen(path);
     if(len > 1 && path[len -1] == '/')
        path[len - 1] = '\0';
}

static void print_debug(char const *format, ...)
{
    #ifndef DEBUG
    return;
    #endif

    struct passwd *p = (struct passwd *)getpwuid(getuid());
    if (p == NULL) return;

    char tmp[2048];
    snprintf(tmp, 2048, "/local/%s/debug_fuse.txt", p->pw_name);
    FILE *f = fopen(tmp, "a");
    if (f == NULL) return;

    snprintf(tmp, 2048, "%s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(f, tmp, args);
    va_end(args);
    fclose(f);
}

static void print_debug2(char const *format, ...)
{
    #ifndef DEBUG
    return;
    #endif

    struct passwd *p = (struct passwd *)getpwuid(getuid());
    if (p == NULL) return;

    char tmp[2048];
    snprintf(tmp, 2048, "/local/%s/debug2.txt", p->pw_name);
    FILE *f = fopen(tmp, "a");
    if (f == NULL) return;

    snprintf(tmp, 2048, "%s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(f, tmp, args);
    va_end(args);
    fclose(f);
}

/* Dummy function to speedup the print_zoo command */
static void print_zoo_async(int rc, const char *value, const void *data)
{

}

static void print_zoo(char *sev, char const *format, ...)
{
    int len;
    char hostname[10], message[200], buffer[255];
    struct timespec time_t;
    double time_d;

    gethostname(hostname, 10);
    clock_gettime(CLOCK_REALTIME, &time_t);

    time_d = (double)time_t.tv_sec + 1.0e-9*time_t.tv_nsec;

    va_list args;
    va_start(args, format);
    vsnprintf(message, 200, format, args);
    va_end(args);

    len = snprintf(buffer, 255, "%s;%s;%lf;%s", sev, hostname, time_d, message);
    zoo_acreate(zh, "/logs/log", buffer, len,  &ZOO_OPEN_ACL_UNSAFE,ZOO_SEQUENCE, &print_zoo_async, NULL);
}

inline static void suspect_server(unsigned int server_id)
{
    print_debug("suspecting server: %d", server_id);

    char zpath[20];
    snprintf(zpath, 20, "/suspicions/%d", server_id);

    zoo_create(zh, zpath, NULL, 0,  &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
}

static void debug_nodeclasses()
{
    #ifndef DEBUG
    return;
    #endif

    int i;
    for(i = 0; i < num_nodeclasses; i++)
    {
        print_debug("Nodeset: #%d, Weight:%d, Servers: %d Set hash:%d ",i, nodeclasses[i].weight, nodeclasses[i].num_nodes, nodeclasses[i].set_hash);
    }

}

void session_watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    if(type != ZOO_SESSION_EVENT)
    {
        print_debug2("Global session watcher function got non-session event");
        return;
    }

    if(state == ZOO_CONNECTED_STATE)
    {
        struct sockaddr addr;
        socklen_t addr_len = sizeof(addr);

        print_debug2("session_watcher: Zookeeper connected now!");

        if(zookeeper_get_connected_host(zh, &addr, &addr_len) != NULL)
        {
            struct sockaddr_in *sin = (struct sockaddr_in *) &addr;
            char *ip = inet_ntoa(sin->sin_addr);


            print_zoo("INFO","Connected! FUSE %d, FT_CONSTANT: %d, ZK-host: %s", fuse_version(), FT_CONSTANT, ip);


        }

        print_debug2("Read information from zookeeper");
        fuse_block = (zoo_wexists(zh, "/fuse_block", &watcher_fuse_block, NULL, NULL) == ZNONODE) ? 0 : 1;
        read_nodeclasses();
        read_servers();
    }
    else
    {
        print_debug2("Zookeeper no longer connected");
        zoo_connected = 0;
    }


}

void watcher_fuse_block(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    int i = 0;
    char zpath[255];

    pthread_mutex_lock(&mut_block);

    if(type == ZOO_SESSION_EVENT && state != ZOO_CONNECTED_STATE)
    {
        if(state == ZOO_CONNECTED_STATE)
        {
            print_debug2("Fuseblock watcher: connected again!");
        }
        else
        {
            print_debug2("Fuseblock watcher: not connected");
            fuse_block = 1;
            pthread_mutex_unlock(&mut_block);
            return;
        }
    }

    if(zoo_wexists(zh, "/fuse_block", &watcher_fuse_block, NULL, NULL) == ZNONODE)
    {
        print_debug2("No fuse blocking, try open file handles");
        /* Test for every file handle that the file has not been removed. */
        handle_lock();
        for(i = 0; i < pool->num; i++)
        {
            if(pool->handles[i]->use == 1)
            {
                snprintf(zpath, 2048, "/data%s",pool->handles[i]->filename);
                if(zoo_exists(zh, zpath, 0, NULL) == ZNONODE)
                {
                    print_debug2("File %s doesnt exist anymore", zpath);
                    pool->handles[i]->use = 2;
                }
            }
        }
        handle_unlock();

        pthread_cond_broadcast(&cond_block);
        print_debug2("Unblocking;");
        fuse_block = 0;
    }
    else
    {
        fuse_block = 1;
        print_debug2("Fuse blocking!");
    }

    pthread_mutex_unlock(&mut_block);
}

void watcher_write_locks(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    if(zoo_wexists(zh, "/write_locks", &watcher_write_locks, NULL, NULL) == ZNONODE)
    {
        print_debug("No write locks");
        do_lock = 0;
    }
    else
    {
        do_lock = 1;
        print_debug("Write locks!");
    }
}

void watcher_nodeclasses(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    print_debug2("watcher_nodeclasses, type:%d, state:%d, path:%s",type,state, path);

    if(type == ZOO_SESSION_EVENT && state != ZOO_CONNECTED_STATE)
    {
        if(state == ZOO_CONNECTED_STATE)
        {
            print_debug2("watcher_nodeclasses: connected again!");
        }
        else
        {
            print_debug2("watcher_nodeclasses: not connected");
            return;
        }
    }

    debug_nodeclasses();
    read_nodeclasses();
    debug_nodeclasses();
}

void watcher_redis_server(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{

    if(type == ZOO_SESSION_EVENT)
    {
        print_debug2("watcher_redis_server: session event, not updating..");
        return;
    }
    else
    {
        print_debug2("watcher_redis_server: type: %d state: %d", type, state);
    }

    char buffer[2048];
    int buffer_length = 2048;
    int class = 0;
    int port = 0;
    char ip[2048];
    int status;

    status = zoo_wget(zh, path, &watcher_redis_server, NULL, buffer, &buffer_length, NULL);
    if(status != ZOK)
    {
        print_debug2("Not okay for path %s", path);
        return;
    }

    buffer[buffer_length] = '\0';

    int server_id = atoi(path + 12); // redis/redis000000...
    sscanf(buffer, "%d,%[^,],%d", &class, ip, &port);

    if(class == -1 && servers[server_id].active == 1)
    {
        print_debug2("Deactivated server %d %s:%d", server_id, ip, port);
        servers[server_id].active = 0;
    }
    else if(class != -1 && servers[server_id].active == 0)
    {
        print_debug2("Reactivated server?? %d %s:%d", server_id, ip, port);
        servers[server_id].active = 1;
    }
}


void watcher_redis(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    print_debug2("Watcher redis: type: %d state: %d path: %s", type, state, path);

    if(type == ZOO_SESSION_EVENT)
    {
        if(state == ZOO_CONNECTED_STATE)
        {
            print_debug2("watcher_redis: connected again!");
        }
        else
        {
            print_debug2("watcher_redis: not connected");
            return;
        }
    }

    // debug_nodeclasses();
    read_servers();
    // debug_nodeclasses();

    if(type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE)
    {
        print_debug2("Checking all redis servers, re-attaching watches");

        sem_wait(&sem_configuration);

        char zpath[2048];
        int i;
        int status;

        zoo_string *children_list = (zoo_string *) malloc(sizeof(zoo_string));
        status = zoo_wget_children(zh, "/redis", &watcher_redis, NULL, children_list);
        free(children_list);
        if(status != ZOK)
        {
            sem_post(&sem_configuration);
            return;
        }

        for(i = 0; i < num_servers; i++)
        {
            snprintf(zpath, 2048, "/redis/redis%010d", i);
            watcher_redis_server(zh, ZOO_CHANGED_EVENT, ZOO_CONNECTED_STATE, zpath, NULL);
        }

        sem_post(&sem_configuration);
    }

}

void read_nodeclasses()
{
    sem_wait(&sem_configuration);
    print_debug2("===========\nNodeclasses\n===========");

    char buffer[2048];
    int buffer_length = 2048;
    char *temp;
    int i;
    int status;

    status = zoo_wget(zh, "/nodeclasses", &watcher_nodeclasses, NULL, buffer, &buffer_length, NULL);
    if(status != ZOK)
    {
        sem_post(&sem_configuration);
        return;
    }

    buffer[buffer_length] = '\0';
    temp = strtok(buffer, ",");
    int new_num_nodeclasses = atoi(temp);

    print_debug2("Number of present nodeclasses: %d", new_num_nodeclasses);

    for(i = 0; i < new_num_nodeclasses; i++)
    {
        temp = strtok(NULL, ",");
        nodeclasses[i].weight = atoi(temp); // TODO check for segfault

        if(i >= num_nodeclasses)
            nodeclasses[i].num_nodes = 0;

        char class_name[64];
        sprintf(class_name, "class__%d", i);
        nodeclasses[i].set_hash = XXH32(class_name, strlen(class_name), XXHASH_SEED);
    }

    num_nodeclasses = new_num_nodeclasses;
    sem_post(&sem_configuration);
}

void read_servers()
{
    sem_wait(&sem_configuration);
    print_debug2("===========\nServers\n===========");


    char zpath[2048];
    int i;
    char buffer[2048];
    int buffer_length = 2048;
    int status;

    zoo_string *children_list = (zoo_string *) malloc(sizeof(zoo_string));
    status = zoo_wget_children(zh, "/redis", &watcher_redis, NULL, children_list);
    if(status != ZOK)
    {
        free(children_list);
        sem_post(&sem_configuration);
        return;
    }

    int new_num_servers = children_list->count;
    print_debug2("Old number: %d, new number: %d", num_servers, new_num_servers);

    /* Clearing new servers */
    for(i = num_servers; i < new_num_servers; i++)
    {
        int class = 0;
        int port = 0;
        char ip[2048];

        /* Reset buffer length */
        buffer_length = 2048;
        snprintf(zpath, 2048, "/redis/redis%010d", i);
        status = zoo_wget(zh, zpath, &watcher_redis_server, NULL, buffer, &buffer_length, NULL);
        if(status != ZOK)
        {
            sem_post(&sem_configuration);
            return;
        }

        buffer[buffer_length] = '\0';

        sscanf(buffer, "%d,%[^,],%d", &class, ip, &port);

        if(class == -1)
        {
            memset(&(servers[i]), 0, sizeof(rserver));
            continue;
        }

        // print_debug2("Connecting to %s", ip);
        servers[i].active  = 1;
        servers[i].hash = XXH32(ip, strlen(ip), XXHASH_SEED);

        #ifdef EREDIS
        servers[i].eredis = eredis_new();
        // print_debug2("Eredis pointer for %d %X", i, servers[i].eredis);
        eredis_timeout(servers[i].eredis, 200);
        eredis_r_max(servers[i].eredis, 50);
        eredis_host_add(servers[i].eredis, ip, port);
        eredis_run_thr(servers[i].eredis);
        #else
        servers[i].redis = redisConnect(ip, port);
        servers[i].thredis = thredis_new(servers[i].redis);
        #endif

        // print_debug2("Connected! class: %d", class);

        /* Add server to nodeset */
        nodeclasses[class].nodes[(nodeclasses[class].num_nodes++)] = i;
    }

    num_servers = new_num_servers;
    free(children_list);

    sem_post(&sem_configuration);
}

void watcher_locks(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    sem_t *semaphore = (sem_t *) watcherCtx;
    sem_post(semaphore);
}

int get_lock(char *zpath, char *lpath, int is_w_lock)
{
    print_debug("getting lock for %s", zpath);

    char tmp[2048];
    snprintf(tmp, 2048, is_w_lock ? "%s/write" : "%s/read", zpath);

    /* Step 1 */
    int create = zoo_create(zh, tmp, NULL, 0,  &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL |  ZOO_SEQUENCE, lpath, 2048);

    if(create != ZOK)
    {
        print_debug("Couldn't create lock for %s", zpath);
        return 0;
    }

    print_debug("created lock %s", lpath);

    /* Create format */
    snprintf(tmp, 2048, is_w_lock ? "%s/write%%d" : "%s/read%%d", zpath);

    int our_seq_number = -1;
    sscanf(lpath, tmp, &our_seq_number);

//    print_debug("our sequence number: %d",our_seq_number);

    zoo_string *children_list = (zoo_string *) malloc(sizeof(zoo_string));
    int i;

    sem_t semaphore;
    sem_init(&semaphore, 0, 0);

    while(1)
    {
        int lowest_child_seq = our_seq_number;
        char* lowest_child = NULL;
        int tmp_seq;

        /* Step 2 */
        zoo_get_children(zh, zpath, 0, children_list);
        for(i = 0; i < children_list->count; i++)
        {
            /* writ-->t<--en, not a lock.. */
            if(children_list->data[i][4] == 't')
                continue;

            if(children_list->data[i][0] == 'w')
                sscanf(children_list->data[i], "write%d", &tmp_seq);
            else if(!is_w_lock)
                continue;
            else
                sscanf(children_list->data[i], "read%d", &tmp_seq);

            if(tmp_seq < lowest_child_seq)
            {
                lowest_child_seq = tmp_seq;
                lowest_child = children_list->data[i];
            }
        }

        /* Step 3 */
        if(lowest_child_seq == our_seq_number)
            break;

        snprintf(tmp, 2048, "%s/%s", zpath, lowest_child);

        /* Step 4  + 5*/
        if(zoo_wexists(zh, tmp, &watcher_locks, &semaphore, NULL) == ZNONODE)
            continue;

        sem_wait(&semaphore);
    }

    sem_destroy(&semaphore);

    print_debug("have a lock! for %s", zpath);
    free(children_list);

    if(zoo_exists(zh, lpath, 0, NULL) == ZNONODE)
    {
        print_debug("or not...");
        return 0;
    }

    return 1;
}

/*
void watcher_removed_lock(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    if(type != ZOO_DELETED_EVENT)
        return;

    handle_t *handle = (handle_t *) watcherCtx;
    if(handle == NULL)
        return;

    handle_mark_deleted(handle, path);
}*/

void watcher_kill(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    if(zoo_wexists(zh, getenv("FTMEMFS_WATCH"), &watcher_kill, NULL, NULL) == ZNONODE)
    {
        print_debug2("FTMEMFS_WATCH: %s does not exist, exiting", getenv("FTMEMFS_WATCH"));
        exit(EXIT_FAILURE);
    }
}


static void* ftmemfs_init(struct fuse_conn_info *conn)
{
    print_debug2("===========\n  FTMEMFS\n===========");

    /* Connect to ZooKeeper server */
    zh = zookeeper_init(getenv("FTMEMFS_ZK"), session_watcher, 2000, 0, NULL, 0);
    if (zh == NULL)
    {
        print_debug("Error connecting to ZooKeeper server!");
        exit(EXIT_FAILURE);
    }

    sem_init(&sem_configuration, 0, 1);

    struct rlimit limit;

    limit.rlim_cur = 60000;
    limit.rlim_max = 60000;
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0)
    {
        printf("setrlimit() failed with errno=%d", errno);
        fflush(stdout);
        // return NULL;
    }

    if(zoo_wexists(zh, getenv("FTMEMFS_WATCH"), &watcher_kill, NULL, NULL) == ZNONODE)
    {
        print_debug2("FTMEMFS_WATCH: %s does not exist, exiting", getenv("FTMEMFS_WATCH"));
        exit(EXIT_FAILURE);
    }

    nodeclasses = calloc(MAX_NUM_NODECLASSES, sizeof(nodeset));
    servers     = calloc(MAX_NUM_SERVERS, sizeof(rserver));

    /*
    snprintf(s, 2048, "init_memcachefs soft limit: %llu, hard limit: %llu", limit.rlim_cur, limit.rlim_max);
    debug_statement(s);*/

    // debug_nodeclasses();

    sem_wait(&sem_configuration);
    pool = handle_pool_new(&opt);
    // cache = mdc_new(MDC_SIZE, MDC_TIMEOUT);
    sem_post(&sem_configuration);

    return NULL;
}

static int ftmemfs_getattr(const char *path, struct stat *stbuf)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("-\ngetattr %s", path);

    /*The file is the root directory*/
    if(!strcmp(path, "/"))
    {
        stbuf->st_ino = 1;
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 1;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 2000000000;
        stbuf->st_size = 0;
        return 0;
    }

    int retval;
    char zpath[2048];
    char buffer[2048];
    int buffer_len = 2048;

    snprintf(zpath, 2048, "/data%s", path);
    remove_trailing_slash(zpath);
    struct zinode metadata;

//    int cache_hit = mdc_search(cache, zpath, &metadata);

//    if(!cache_hit)
//    {
        retval = zoo_get(zh, zpath, 0, buffer, &buffer_len, NULL);

        if(retval != ZOK)
            return -ENOENT;

        memcpy(&metadata, buffer, sizeof(struct zinode));
        //print_debug("Cache mis! %s\n", zpath);
    //    mdc_insert(cache, zpath, &metadata);
//    }
//    else
    //    print_debug("Cache hit! %s\n", zpath);

    if(metadata.is_dir)
    {
        stbuf->st_ino = 1;
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 2;
        stbuf->st_atime = 0;
        stbuf->st_mtime = metadata.mtime;
        stbuf->st_size = 0;
        return 0;
    }
    else
    {
        stbuf->st_ino = 1;
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
        stbuf->st_nlink = 1;
        stbuf->st_atime = 0;
        stbuf->st_mtime = metadata.mtime;
        stbuf->st_size = metadata.size;
        return 0;
    }

    return -ENOENT;
}

static int ftmemfs_opendir(const char *path, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("opendir %s", path);

    return 0;
}


static int ftmemfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{

    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    zoo_string *children_list = (zoo_string *) malloc(sizeof(zoo_string));

    int i;
    int retval;

    print_debug("readdir %s", path);

    char zpath[2048];
    snprintf(zpath, 2048, "/data%s", path);
    remove_trailing_slash(zpath);

    filler(buf, ".", NULL, 0);
    /*.. is added when the directory key is first created*/
    filler(buf, "..", NULL, 0);

    print_debug("read_dir_get_children");
    retval = zoo_get_children(zh, zpath, 0, children_list);

    if(retval != ZOK) {
        free(children_list);
        print_zoo("ERROR", "805");
        return -EIO;
    }

    for(i = 0; i < children_list->count; i++)
        filler(buf, children_list->data[i], NULL, 0);

    free(children_list);
    return 0;
}

static int ftmemfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

/*
static void ftmemfs_mknod_async(int rc, const char *value, const void *data)
{
    print_debug("mknod async %d %c", rc, value);
}*/


static int ftmemfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("--\nmknod %s", path);
    int status;
    char zpath[2048];
    snprintf(zpath, 2048, "/data%s", path);
    char temp[2048];
    remove_trailing_slash(zpath);

    char buffer[2048];

    zinode metadata;
    memset(&metadata, 0, sizeof(zinode));
    metadata.is_dir = 0;
    metadata.mtime  = time(NULL);
    metadata.size   = 0;
    metadata.node_class = 0; //num_nodeclasses == 1 ? 0 : get_hrw_nodeset(path, nodeclasses, num_nodeclasses);

    /* Select ips for file */
    if(! get_hrw_servers(path, nodeclasses, servers, &metadata))
    {
        print_zoo("ERROR", "854");
        return -EIO;
    }

    memcpy(buffer, &metadata, sizeof(struct zinode));

    //mdc_insert(cache, zpath, &metadata);

    // ASYNC
    // zoo_acreate(zh, zpath, buffer, sizeof(struct zinode), &ZOO_OPEN_ACL_UNSAFE, 0, &ftmemfs_mknod_async, NULL);
    // get_reverse_index_path(zpath, 2048, path + 1, metadata.servers[0]);
    // zoo_acreate(zh, zpath, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, &ftmemfs_mknod_async, NULL);

    // SYNC
    status = zoo_create(zh, zpath, buffer, sizeof(struct zinode), &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if(status != ZOK)
    {
        snprintf(temp, 2048, "871: zpath: %s status: %d", zpath, status);
        print_zoo("ERROR", temp);
        return -EIO;
    }

    get_reverse_index_path(zpath, 2048, path + 1, metadata.servers[0]);
    status = zoo_create(zh, zpath, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if(status != ZOK)
    {
        print_zoo("ERROR", "879");
        return -EIO;
    }

    return 0;
}

static int ftmemfs_mkdir(const char *path, mode_t mode) {
    print_debug("mkdir ");

    int retval;

    print_debug("%s",path);

    struct zinode metadata;
    memset(&metadata, 0, sizeof(zinode));

    metadata.is_dir = 1;
    metadata.mtime  = time(NULL);

    char content[sizeof(struct zinode)];
    memcpy(content, &metadata, sizeof(struct zinode));

    char zpath[2048];
    snprintf(zpath, 2048, "/data%s",path);
    remove_trailing_slash(zpath);

    retval = zoo_create(zh, zpath, content, sizeof(struct zinode), &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    if(retval == ZOK)
    {
        print_debug("mkdir ok! ");
    }
    else
    {
        print_debug("mkdir error %d", retval);
        print_zoo("ERROR", "915");
        return -EIO;
    }

    return 0;
}

static int ftmemfs_rmdir(const char *path)
{
    print_debug("rmdir");

    char zpath[2048];
    snprintf(zpath, 2048, "/data%s", path);
    remove_trailing_slash(zpath);

    int status = zoo_delete(zh, zpath, -1);

    if(status != ZOK)
    {
        char temp[2048];
        snprintf(temp, 2048, "932 status %d zpath %s", status, zpath);

        print_zoo("ERROR", temp);
        return -EIO;
    }

    return 0;
}

void remove_chunks(const char *path, zinode *metadata)
{
    print_debug("Removing chunks!");

    /* Remove content.. */
    #ifdef EREDIS
    eredis_reply_t *rc = NULL;
    eredis_reader_t *redis_context = NULL;
    #else
    redisReply *rc = NULL;
    thredis_t *redis_context = NULL;
    #endif

    /* remove all file contents */
    int crnt_chunk = 0;
    char key[4096], str[4096];
    do
    {
        #ifdef EREDIS
        if(redis_context != NULL)
            eredis_r_release(redis_context);
        #endif

        memset(str, 0, sizeof(str));
        memset(key, 0, sizeof(key));

        sprintf(str, "___chunk-%d", crnt_chunk);
        // strcpy(key, HIDDEN);
        strcat(key, path + 1);
        strcat(key, str);

        unsigned int n_server = metadata->servers[crnt_chunk % metadata->num_ips];

        if(servers[n_server].active == 0)
        {
            crnt_chunk++;
            continue;
        }

        #ifdef EREDIS
        redis_context = eredis_r(servers[n_server].eredis);
        #else
        redis_context = servers[n_server].thredis;
        #endif

        //print_debug("Current chunk is %d", crnt_chunk);

        crnt_chunk++;

        #ifdef EREDIS
        rc = eredis_r_cmd(redis_context, "DEL %s", key);
        #else
        rc = thredis_command(redis_context, "DEL %s", key);
        #endif

    } while (rc == NULL || rc->integer != 0);

    #ifdef EREDIS
    if(redis_context != NULL)
        eredis_r_release(redis_context);
    #endif

}

static int ftmemfs_unlink(const char *path)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("unlink %s", path);

    int retval, i;
    char zpath[2048];
    char buffer[2048];
    int buffer_len = 2048;

    snprintf(zpath, 2048, "/data%s",path);
    remove_trailing_slash(zpath);

    zinode metadata;
    retval = zoo_get(zh, zpath, 0, buffer, &buffer_len, NULL);

    if(retval != ZOK)
    {
        print_zoo("ERROR", "1025");
        return -EIO;
    }

    memcpy(&metadata, buffer, sizeof(zinode));

    /* Get write lock */
    // char lpath[2048];
    // if(do_lock && !get_lock(zpath, lpath, 1))
    //     return -EIO;

    // if(do_lock)
    //     print_debug("unlink, get lock: %s", lpath);

    zoo_string *children_list = (zoo_string *) malloc(sizeof(zoo_string));
    retval = zoo_get_children(zh, zpath, 0, children_list);
    if(retval != ZOK)
    {
        free(children_list);
        print_zoo("ERROR", "1043");
        return -EIO;
    }

    /* Remove all children, locks, etc */
    for(i = 0; i < children_list->count; i++)
    {
        snprintf(buffer, 2048, "/data%s/%s", path, children_list->data[i]);
        zoo_delete(zh, buffer, -1);
    }
    free(children_list);

    remove_chunks(path, &metadata);

    get_reverse_index_path(buffer, 2048, path + 1, metadata.servers[0]);
    zoo_delete(zh, buffer, -1);
    retval = zoo_delete(zh, zpath, -1);

    if(retval != ZOK)
    {
        print_zoo("ERROR", "1062");
    }

    return 0 ; //retval == ZOK ? 0 : -EIO;
}

static int ftmemfs_chmod(const char* path, mode_t mode)
{
    return 0;
}

static int ftmemfs_chown(const char *path, uid_t uid, gid_t gid)
{
    return -ENOSYS;
}

static int ftmemfs_truncate(const char* path, off_t length)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);


    print_debug2("\n\n\n---truncate %s offset: %d", path+1, length);

    /*char *key;
    redisReply *rc;

    if(length != 0) {
        return -ENOSYS;
    }

    key = (char *)path + 1;
    thredis_t *redis_context = identify_metadata_server(key);

    // set into the metadata file the current server weights
    char *file_info = construct_file_weight_str_array(s_list[n_lists], 0);

    rc = thredis_command(redis_context, "SET %s %s", key, file_info);
    int n_tries = 100;
    while (rc->type == REDIS_REPLY_ERROR && n_tries --)
    {
        usleep(10);
        freeReplyObject(rc);
        rc = thredis_command(redis_context, "SET %s %s", key, file_info);
    }
    free(file_info);

    char s[2048];
    snprintf(s, 2048, "truncate SET key:%s value:%s/???????", key, "0");
    debug_statement(s);

    freeReplyObject(rc);*/
    return 0;
}

static int ftmemfs_utime(const char *path, struct utimbuf *time)
{
    return 0;
}

static int ftmemfs_open(const char *path, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    char zpath[2048];
    char buffer[2048];
    int buffer_len = 2048;

    print_debug("-\nopen %s", path);

    /* bind file handle to a local handle*/
    handle_t *phandle = handle_get(pool);

    if (phandle == NULL)
    {
        print_debug("open NULL => exit");
        print_zoo("ERROR", "1143");
        return -EIO;
    }

    snprintf(zpath, 2048, "/data%s", path);
    remove_trailing_slash(zpath);
    // int need_lock = do_lock;

    // Check if zpath does exists..
    // char lpath[2048];
    // if(need_lock && !get_lock(zpath, lpath, (fi->flags & O_WRONLY || fi->flags & O_RDWR)))
    //     return -EIO;

    /* Set watch for lock */
    // if(need_lock)
    //     zoo_wexists(zh, lpath, &watcher_removed_lock, phandle, NULL);

//    int cache_hit;

    /*cache_hit = mdc_search(cache, zpath, &(phandle->metadata));
    if(!cache_hit)
    {*/
    zoo_get(zh, zpath, 0, buffer, &buffer_len, NULL);
    memcpy(&(phandle->metadata), buffer, sizeof(zinode));
    //    print_debug("Cache mis?? %s", zpath);
        //print_zoo("NOTICE", "Cache miss %s", zpath);
//    }
    //else
    //    print_zoo("NOTICE", "Cache hit %s", zpath);

    phandle->f_size = phandle->metadata.size;
    fi->fh = phandle->index;
    memset(phandle->filename, 0, 512);
    memcpy(phandle->filename, path, strlen(path) + 1);

    // if(need_lock)
    //     strcpy(phandle->lpath, lpath);
    // else
    //phandle->lpath[0] = '\0';


    /* Handle truncing on deletion */
    if(fi->flags & O_TRUNC && phandle->metadata.size !=0)
    {
        #ifdef DEBUG
        print_zoo("NOTICE","Truncating file %s", zpath);
        #endif
        print_debug("Handling truncation!");
        remove_chunks(path, &(phandle->metadata));

        /* Remove reverse index */
        get_reverse_index_path(buffer, 2048, path + 1, phandle->metadata.servers[0]);
        zoo_delete(zh, buffer, -1);

        /* Refresh servers */
        if(! get_hrw_servers(path, nodeclasses, servers, &(phandle->metadata)))
        {
            print_zoo("ERROR", "1198");
            return -EIO;
        }

        phandle->metadata.size = 0;
        phandle->f_size = 0;
        memcpy(buffer, &(phandle->metadata), sizeof(zinode));
        zoo_set(zh, zpath, buffer, sizeof(zinode), -1);

        /* Create reverse index */
        get_reverse_index_path(buffer, 2048, path + 1, phandle->metadata.servers[0]);
        zoo_create(zh, buffer, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

        /* Remove the written file */
        char zpath[2048];
        snprintf(zpath, 2048, "/data%s/written", path);
        zoo_delete(zh, zpath, -1);
    }

    return 0;
}

/*
static void ftmemfs_flush_async(int rc, const struct Stat *stat, const void *data)
{
    print_debug("flush async %d %c", rc);
}*/



static int ftmemfs_flush(const char *path, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    char zpath[2048];
    char buffer[2048];

    print_debug("flush %s", path);

    /*Get current handle*/
    handle_t *phandle = pool->handles[fi->fh];

    if (phandle->written_bytes == 0)
        return 0;

    snprintf(zpath, 2048, "/data%s", path);
    remove_trailing_slash(zpath);

    phandle->metadata.size += phandle->written_bytes;

    print_debug("Flush: updating!, size: %d", phandle->metadata.size);

    /* This is very necessary to clean up the older cache line */
    //mdc_insert(cache, zpath, &phandle->metadata);

    memcpy(buffer, &phandle->metadata, sizeof(zinode));

    // ASYNC
/*    if(zoo_aset(zh, zpath, buffer, sizeof(zinode), -1, &ftmemfs_flush_async, NULL) != ZOK)
    {
        print_debug("Zookeeper complains!");
        return -EIO;
    }*/

    // SYNC
    if(zoo_set(zh, zpath, buffer, sizeof(zinode), -1) != ZOK)
    {
        print_zoo("ERROR", "1268");
        return -EIO;
    }

    if(phandle->use == 2)
    {
        print_zoo("ERROR", "1274");
        return -EIO;
    }

    return 0;
}


static int ftmemfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("read %s, size=%zu, offset=%zu f_index_offset=%zu, fi->fh = %d,"
            " chunk_index = %zu", path, (size_t)size, (size_t)offset,
            (size_t) offset % MAX_CHUNK, (int)fi->fh, (size_t)offset/MAX_CHUNK);

    char str[2048], key[2048];

    size_t file_index = (size_t)offset / MAX_CHUNK;
    size_t file_index_offset = (size_t)offset % MAX_CHUNK;

    handle_t *phandle = pool->handles[fi->fh];

    if(phandle->use == 2)
    {
        print_zoo("ERROR", "1302");
        return -EIO;
    }

    /*Last piece of data to read*/
    if ((size_t)offset + (size_t)size > (size_t)phandle->f_size && phandle->f_size > 0)
    {
        size = (size_t)phandle->f_size - (size_t)offset;
    }
    // some sort of error
    if (phandle->f_size < offset)
    {
        // print_zoo("ERROR","some sort of error?");
        return 0;
    }

    size_t bytes_to_copy = size;
    // current chunk not prefetched
    if (file_index != phandle->pref_chunk) {
        print_debug("read %s not prefetched ", path);

        // prefetch the next chunk
        memset(str, 0, sizeof(str));
        memset(key, 0, sizeof(key));
        sprintf(str, "___chunk-%zu", file_index);
        // strcpy(key, HIDDEN);
        strcat(key, path + 1);
        strcat(key, str);

        unsigned int n_server = phandle->metadata.servers[file_index % phandle->metadata.num_ips];

        if(servers[n_server].active == 0)
        {
            // print_zoo("ERROR", "1335");
            return -EIO;
        }

        #ifdef EREDIS
        eredis_reader_t *redis_context = eredis_r(servers[n_server].eredis);
        eredis_reply_t *rc = eredis_r_cmd(redis_context, "GET %s", key);
        #else
        thredis_t *redis_context = servers[n_server].thredis;
        redisReply *rc = thredis_command(redis_context, "GET %s", key);
        #endif


        if(rc == NULL || rc->type == REDIS_REPLY_ERROR)
        {
            #ifdef EREDIS
            eredis_r_release(redis_context);
            #endif

            suspect_server(n_server);
            print_zoo("ERROR", "1355");
            return -EIO;
        }
        //// TODO check?!
        memcpy(phandle->local_buf, rc->str, rc->len);
        #ifdef EREDIS
        eredis_r_release(redis_context);
        #else
        freeReplyObject(rc);
        #endif

        phandle->pref_chunk = file_index;
    }

    // do the read, but check if the read is in between chunks
    if ((size_t)bytes_to_copy + (size_t)file_index_offset > (size_t)MAX_CHUNK) {
        bytes_to_copy = (size_t)MAX_CHUNK - (size_t)file_index_offset;
    }
    memcpy(buf, phandle->local_buf + file_index_offset, bytes_to_copy);
    print_debug("read: after memcpy %s", path);
    // read is in between chunks
    if (bytes_to_copy < size) {
        // prefetch the next chunk as well
        memset(str, 0, sizeof(str));
        memset(key, 0, sizeof(key));
        sprintf(str, "___chunk-%zu", file_index + 1);
        // strcpy(key, HIDDEN);
        strcat(key, path + 1);
        strcat(key, str);

        unsigned int n_server = phandle->metadata.servers[(file_index + 1) % phandle->metadata.num_ips];
        if(servers[n_server].active == 0)
        {
            print_debug("1386");
            return -EIO;
        }

        #ifdef EREDIS
        eredis_reader_t *redis_context = eredis_r(servers[n_server].eredis);
        eredis_reply_t *rc = eredis_r_cmd(redis_context, "GET %s", key);
        #else
        thredis_t *redis_context = servers[n_server].thredis;
        redisReply *rc = thredis_command(redis_context, "GET %s", key);
        #endif

        if(rc == NULL || rc->type == REDIS_REPLY_ERROR)
        {
            #ifdef EREDIS
            eredis_r_release(redis_context);
            #else
            freeReplyObject(rc);
            #endif

            suspect_server(n_server);
            print_zoo("ERROR", "1405");
            return -EIO;
        }

        memcpy(phandle->local_buf, rc->str, rc->len);
        phandle->pref_chunk = file_index + 1;

        #ifdef EREDIS
        eredis_r_release(redis_context);
        #else
        freeReplyObject(rc);
        #endif

        // copy the needed data
        memcpy(buf + bytes_to_copy, phandle->local_buf, (size - bytes_to_copy));
    }

    return (int)size;
}

static int ftmemfs_write(const char *path, const char *buf, size_t size,
        off_t offset, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("write %s, size=%d, offset=%zu", path, (int)size, (size_t)offset);

    handle_t *phandle = pool->handles[fi->fh];

    if(phandle->use == 2)
    {
        // print_zoo("ERROR", "1516");
        return -EIO;
    }

    /*If the file is too big*/
    //if (offset + size >= 10000000000)
    //{
    //  return -EFBIG;
    //}
    int crnt_chunk = offset / MAX_CHUNK;
    size_t new_offset = offset % MAX_CHUNK;

    char key[4096], str[4096];
    memset(str, 0, sizeof(str));
    memset(key, 0, sizeof(key));

    sprintf(str, "___chunk-%d", crnt_chunk);
    // strcpy(key, HIDDEN);
    strcat(key, path + 1);
    strcat(key, str);

    unsigned int n_server = phandle->metadata.servers[crnt_chunk % phandle->metadata.num_ips];
    if(servers[n_server].active == 0)
    {
        // print_zoo("ERROR", "1540");
        return -EIO;
    }

    #ifdef EREDIS
    eredis_reader_t *redis_context = eredis_r(servers[n_server].eredis);
    #else
    thredis_t *redis_context = servers[n_server].thredis;
    #endif

    // if write is in between two chunks
    if (new_offset + size >= MAX_CHUNK)
    {

        size_t size1 = MAX_CHUNK - new_offset;
        size_t size2 = size - size1;

        // first chunk
        redisReply *rc;
        #ifdef EREDIS
        rc = eredis_r_cmd(redis_context,"SETRANGE %s %d %b", key, (int)new_offset, buf, size1);
        #else
        rc = thredis_command(redis_context,"SETRANGE %s %d %b", key, (int)new_offset, buf, size1);
        #endif

        int n_tries = 100;

        //sprintf(s, "write: %s , rc_type = %d", path, rc->type);
        //debug_statement2(s);

        while ((rc == NULL || rc->type == REDIS_REPLY_ERROR) && n_tries --)
        {
            usleep(10);
            #ifdef EREDIS
            rc = eredis_r_cmd(redis_context,"SETRANGE %s %d %b", key, (int)new_offset, buf, size1);
            #else
            rc = thredis_command(redis_context,"SETRANGE %s %d %b", key, (int)new_offset, buf, size1);
            #endif

        }
        if(n_tries == -1)
        {
            #ifdef EREDIS
            eredis_r_release(redis_context);
            #else
            freeReplyObject(rc);
            #endif
            suspect_server(n_server);
            print_zoo("ERROR", "1586");
            return -EIO;
        }

        //sprintf(s, "2write: %s , rc_type = %d", path, rc->type);
        //debug_statement2(s);
        #ifdef EREDIS
        eredis_r_release(redis_context);
        #else
        freeReplyObject(rc);
        #endif

        // second chunk
        memset(str, 0, sizeof(str));
        memset(key, 0, sizeof(key));

        sprintf(str, "___chunk-%d", crnt_chunk + 1);
        // strcpy(key, HIDDEN);
        strcat(key, path + 1);
        strcat(key, str);

        n_server = phandle->metadata.servers[(crnt_chunk + 1) % phandle->metadata.num_ips];
        if(servers[n_server].active == 0)
        {
            // print_zoo("ERROR", "1608");
            return -EIO;
        }

        #ifdef EREDIS
        redis_context = eredis_r(servers[n_server].eredis);
        rc = eredis_r_cmd(redis_context, "SETRANGE %s %d %b", key, 0, buf + size1, size2);
        #else
        redis_context = servers[n_server].thredis;
        rc = thredis_command(redis_context, "SETRANGE %s %d %b", key, 0, buf + size1, size2);
        #endif

        n_tries = 100;
        while ((rc == NULL || rc->type == REDIS_REPLY_ERROR) && n_tries --)
        {
            usleep(10);
            #ifdef EREDIS
            rc = eredis_r_cmd(redis_context, "SETRANGE %s %d %b", key, 0, buf + size1, size2);
            #else
            rc = thredis_command(redis_context, "SETRANGE %s %d %b", key, 0, buf + size1, size2);
            #endif
        }
        if(n_tries == -1)
        {
            print_debug("Nop.. %s",key);
            #ifdef EREDIS
            eredis_r_release(redis_context);
            #else
            freeReplyObject(rc);
            #endif
            suspect_server(n_server);
            print_zoo("ERROR", "1637");
            return -EIO;
        }

        #ifdef EREDIS
        eredis_r_release(redis_context);
        #else
        freeReplyObject(rc);
        #endif
    }
    else
    {
        redisReply *rc;

        #ifdef EREDIS
        rc = eredis_r_cmd(redis_context, "SETRANGE %s %d %b", key, (int)new_offset, buf, size);
        #else
        rc = thredis_command(redis_context, "SETRANGE %s %d %b", key, (int)new_offset, buf, size);
        #endif

        int n_tries = 100;
        while ((rc == NULL || rc->type == REDIS_REPLY_ERROR) && n_tries --)
        {
            usleep(10);
            #ifdef EREDIS
            rc = eredis_r_cmd(redis_context, "SETRANGE %s %d %b", key, (int)new_offset, buf, size);
            #else
            rc = thredis_command(redis_context, "SETRANGE %s %d %b", key, (int)new_offset, buf, size);
            #endif
        }

        if(n_tries == -1)
        {
            print_debug("Nop.. %s",key);
            #ifdef EREDIS
            eredis_r_release(redis_context);
            #else
            freeReplyObject(rc);
            #endif
            suspect_server(n_server);
            print_zoo("ERROR", "1673");
            return -EIO;
        }
        #ifdef EREDIS
        eredis_r_release(redis_context);
        #else
        freeReplyObject(rc);
        #endif
    }
    if (offset + size > phandle->written_bytes)
        phandle->written_bytes += size;

    return size;
}

static int ftmemfs_release(const char *path, struct fuse_file_info *fi)
{
    pthread_mutex_lock(&mut_block);
    while (fuse_block == 1)
        pthread_cond_wait(&cond_block, &mut_block);
    pthread_mutex_unlock(&mut_block);

    print_debug("release %s", path);
    int ret = 0;

    //handle_t *phandle = pool->handles[fi->fh];

/*    if(phandle->lpath[0] != '\0' && zoo_delete(zh, phandle->lpath, -1) != ZOK)
    {
        print_debug("release went wrong: %s",path);
        print_zoo("ERROR", "1701");
        ret = -EIO;
    }*/

    int index = fi->fh;
    handle_release(pool, index);
    fi->fh = -1;

    print_debug("returning release %s", path);

    return ret;
}

static int ftmemfs_fsync(const char *path, int i, struct fuse_file_info *fi)
{
    print_debug("fsync");

    //ftmemfs_flush(path, fi);
    return 0;
}

static int ftmemfs_link(const char *from, const char *to)
{
    return -ENOSYS;
}

static int ftmemfs_symlink(const char *from, const char *to)
{
    return -ENOSYS;
}

static int ftmemfs_readlink(const char *path, char *buf, size_t size)
{
    return -ENOSYS;
}

static int ftmemfs_rename(const char *from, const char *to)
{
    print_debug("rename");

    return 0;
}


static void ftmemfs_destroy()
{
    print_debug("destroy!");
    zookeeper_close(zh);

    handle_pool_free(pool);
    // mdc_destroy(cache);
    free(nodeclasses);
    free(servers);



    print_debug("Destroyed!");
}

static struct fuse_operations ftmemfs_oper = {
    .init           = ftmemfs_init,
    .destroy        = ftmemfs_destroy,
    .getattr        = ftmemfs_getattr,
    .opendir        = ftmemfs_opendir,
    .readdir        = ftmemfs_readdir,
    .releasedir     = ftmemfs_releasedir,
    .mknod          = ftmemfs_mknod,
    .mkdir          = ftmemfs_mkdir,
    .unlink         = ftmemfs_unlink,
    .rmdir          = ftmemfs_rmdir,
    .chmod          = ftmemfs_chmod,
    .chown          = ftmemfs_chown,
    .truncate       = ftmemfs_truncate,
    .utime          = ftmemfs_utime,
    .open           = ftmemfs_open,
    .read           = ftmemfs_read,
    .write          = ftmemfs_write,
    .flush          = ftmemfs_flush,
    .release        = ftmemfs_release,
    .fsync          = ftmemfs_fsync,
    .link           = ftmemfs_link,
    .symlink        = ftmemfs_symlink,
    .readlink       = ftmemfs_readlink,
    .rename         = ftmemfs_rename,
};

int get_hrw_nodeset(const char *path, nodeset * nodesets, int num_nodesets)
{
    uint32_t file_hash = XXH32(path, strlen(path), XXHASH_SEED);

    int i, max_pos = 0;
    uint32_t max_val = 0;
    for (i = 0; i < num_nodesets; i++)
    {
        uint32_t crnt_val = get_hrw_hash(file_hash, nodesets[i].set_hash) + nodesets[i].weight;
        if (crnt_val > max_val)
        {
            max_val = crnt_val;
            max_pos = i;
        }
    }
    return max_pos;
}

int sk_compare(const void *p1, const void *p2)
{
    sortable_serverkeypair *k1 = (sortable_serverkeypair*) p1;
    sortable_serverkeypair *k2 = (sortable_serverkeypair*) p2;

    return k1->key - k2->key;
}

int get_hrw_servers(const char *path, nodeset* nodesets, rserver * servers, zinode *metadata)
{
    uint32_t file_hash = XXH32(path, strlen(path), XXHASH_SEED);

    int nodeset = metadata->node_class;
    int i;
    int valid_nodes = 0;

    /* Determine the N nodes within the class that the segments should be placed on. */
    sortable_serverkeypair *skpairs = malloc(sizeof(sortable_serverkeypair) * nodesets[nodeset].num_nodes);
    for(i = 0; i < nodesets[nodeset].num_nodes; i++)
    {
        if(servers[nodesets[nodeset].nodes[i]].active == 0)
            continue;

         skpairs[valid_nodes].server = nodesets[nodeset].nodes[i];
         skpairs[valid_nodes].hash = servers[(skpairs[valid_nodes].server)].hash;
         skpairs[valid_nodes].key = get_hrw_hash(file_hash, skpairs[valid_nodes].hash);

         valid_nodes++;
    }

    qsort(skpairs, valid_nodes, sizeof(sortable_serverkeypair), sk_compare);

    /* So now the top N servers can be used.. */
    metadata->num_ips = min(valid_nodes,  FT_CONSTANT);
    if(valid_nodes == 0)
    {
        free(skpairs);
        return 0;
    }

    for (i = 0; i < metadata->num_ips; i++)
        metadata->servers[i] = skpairs[i].server;
    for (i = metadata->num_ips; i < FT_CONSTANT; i++)
        metadata->servers[i] = 0;

    free(skpairs);

    return 1;
}

/*
unsigned int get_thredis_server(char *plaintext_key, rserver* servers, zinode *metadata)
{
    size_t length = strlen(plaintext_key);
    uint32_t key = XXH32(plaintext_key, length, 0);
    //print_debug("get_thredis_server(key: %s; %u)", plaintext_key, key);
    int i, max_pos = 0;
    uint32_t max_val = 0;
    for (i = 0; i < metadata->num_ips; ++ i)
    {
        int server = metadata->servers[i];
        uint32_t crnt_val = get_hrw_hash(key, servers[server].hash);
        //print_debug("server: %i, hash: %u", server, crnt_val);
        if (crnt_val > max_val)
        {
            max_val = crnt_val;
            max_pos = server;
        }
    }
    //print_debug("selected server: %u", max_pos);


    return max_pos;
}*/

uint32_t get_hrw_hash(uint32_t key, uint32_t object)
{
    return (1103515245 * ((1103515245 * object + 12345) ^ key) + 12345) % (1 << 31);
}

void get_reverse_index_path(char *target, int target_len, const char *source, unsigned int server)
{
	char temp[2048];
	int i, t = 0;
	int len = strlen(source);

    /* Up to the length, so it will also copy the null terminating byte */
	for(i = 0; i <= len; i++)
	{
		if(source[i] == '/')
		{
			temp[t++] = '$';
			temp[t++] = '_';
			temp[t++] = '$';
		}
		else
			temp[t++] = source[i];

	}
	snprintf(target, target_len, "/redis/redis%010d/%s", server, temp);
}


/*
 * main
 */
int main(int argc, char *argv[])
{
    print_debug("\n\nStarting FUSE");
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_main(args.argc, args.argv, &ftmemfs_oper, NULL);
    fuse_opt_free_args(&args);

    return EXIT_SUCCESS;
}
