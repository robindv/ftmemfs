#include <stdint.h>

#ifdef EREDIS
#include <eredis.h>
#else
#include "thredis.h"
#endif

#ifndef FTMEMFS_H
#define FTMEMFS_H

typedef struct{
    char *host;
    char *port;
    short verbose;
    unsigned int maxhandle;
}redisfs_opt_t;

#define FT_CONSTANT 8

typedef struct zinode {
    unsigned int is_dir;
    unsigned int mtime;
    unsigned int size;
    unsigned int node_class;
    unsigned int num_ips;
    unsigned int servers[FT_CONSTANT];
} zinode;

#define XXHASH_SEED 0
//#define BUFFERED_CHUNKS 16
//#define MAX_CHUNK (2 * 512 * 1000)
#define HIDDEN "."
#define MAX_NUM_SERVERS 64
#define MAX_NUM_NODECLASSES 3

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define XXHASH_SEED 0
#define IP_PORT_DELIMITER ":"


#define min(a, b) (((a) < (b)) ? (a) : (b))

/* New structs */
typedef struct _rserver
{
    uint32_t hash;
    #ifdef EREDIS
    eredis_t* eredis;
    #else
    redisContext *redis;
    thredis_t *thredis;
    #endif
    int active;
} rserver;

typedef struct _nodeset
{
    uint32_t num_nodes;
    uint32_t nodes[64];
    uint32_t weight;
    uint32_t set_hash;
} nodeset;

typedef struct _sortable_serverkeypair {
    int server;
    uint32_t key;
    uint32_t hash;
} sortable_serverkeypair;


int get_hrw_servers(const char *, nodeset* , rserver * , zinode *);
int get_hrw_nodeset(const char *, nodeset *, int);
uint32_t get_hrw_hash(uint32_t key, uint32_t object);
unsigned int get_thredis_server(char *plaintext_key, rserver* servers, zinode *metadata);

void get_reverse_index_path(char *target, int target_len, const char *source, unsigned int server);
void read_nodeclasses();
void read_servers();
typedef struct String_vector zoo_string;

void release_socket(int server_id, int socket_id);
int get_unused_socket(int server_id);

#endif
