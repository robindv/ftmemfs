#!/bin/bash

if [ $0 != "ftmemfs/scripts/start_locally.sh" ]; then
    echo "Usage ftmemfs/scripts/start_locally.sh"
    exit 0
fi


zk=$(cat $HOME/ftstate/zookeeper_hosts)
mkdir -p /local/$USER/bin
cp $HOME/bin/redis-server /local/$USER/bin
cp $HOME/ftmemfs/fuse/ftmemfs /local/$USER/bin

# Legacy binary
#cp $HOME/code/memcachefs /local/$USER/bin
#cp $HOME/ftstate/ip_redis_table.txt /local/$USER/ip_redis_table.txt

$HOME/ftenv/bin/python -m ftmemfs.localmanager $zk
