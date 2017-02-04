#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: ./mount.sh <zkstring>"
    exit 1
fi

zk=$1
watch=/zookeeper
mkdir -p /local/rdevries/ftmemfs

fusermount -u /local/rdevries/ftmemfs

FTMEMFS_ZK=$zk FTMEMFS_WATCH=$watch $HOME/ftmemfs/fuse/ftmemfs /local/rdevries/ftmemfs -o sync_read -o max_read=131072 -o max_write=131072 -o direct_io -o atomic_o_trunc
