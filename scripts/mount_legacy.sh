#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: ./mount_legacy.sh <dir>"
    exit 1
fi

dir=$1

mkdir -p $dir
fusermount -u $dir

/local/$USER/bin/memcachefs $dir -o max_read=131072 -o max_write=131072  -o direct_io
