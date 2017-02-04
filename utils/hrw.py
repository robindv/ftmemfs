import xxhash
import struct

def get_zinode(zk, filename):

    try:
        metadata, _ = zk.get("/data/" + filename)
    except:
        print "Node {} does not exist".format(filename)
        return None

    t = struct.unpack("IIIII{}I".format(len(metadata) / 4 - 5), metadata)

    zinode = {"is_dir" : t[0], "mtime": t[1], "size": t[2], "node_class": t[3], "servers" : t[5:(5+t[4])]}

    return zinode

def get_hashes(zk):

    hashes = {}

    for server in zk.get_children("/redis"):
        data, _ = zk.get("/redis/" + server)
        hashes[int(server[5:])] = xxhash.xxh32(data.split(',')[1]).intdigest()

    return hashes

def get_server_for_chunk(filename, segment , zinode, hashes):

    return zinode["servers"][int(segment) % len(zinode["servers"])]

    # key = xxhash.xxh32("{}___chunk-{}".format(filename, segment)).intdigest()
    #
    # max_val = -1
    # max_pos = -1
    #
    # for server in zinode["servers"]:
    #     crnt_val = hrw_hash(key, hashes[server])
    #     if crnt_val > max_val:
    #         max_val = crnt_val
    #         max_pos = server
    #
    # return max_pos

def hrw_hash(key, object):
    """Calculate the higest-random-value hash of a given key for a server"""
    return (1103515245 * ((1103515245 * object + 12345) ^ key) + 12345 ) & 0x7FFFFFFF
