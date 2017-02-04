#!/bin/bash

if [[ $# != 3 ]]; then
    echo "Usage: ./mount_ftmemfs.sh <dir> <watch> <zkstring>"
    exit 1
fi

dir=$1

mkdir -p $dir
fusermount -u $dir
FTMEMFS_ZK=$3 FTMEMFS_WATCH=$2 /local/$USER/bin/ftmemfs $dir -o sync_read -o max_read=131072 -o max_write=131072  -o atomic_o_trunc -o direct_io
