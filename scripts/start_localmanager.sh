#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage ./start_localmanager.sh <host> <zkstring>"
    exit 0
fi

host=$1
zk=$2
ssh $host "mkdir -p /local/$USER/bin"
scp $HOME/bin/redis-server $host:/local/$USER/bin
scp $HOME/ftmemfs/fuse/ftmemfs $host:/local/$USER/bin

# Legacy binary
#scp $HOME/code/memcachefs $host:/local/$USER/bin
#scp $HOME/ftstate/ip_redis_table.txt $host:/local/$USER/ip_redis_table.txt

ssh $host "sh -c 'nohup $HOME/ftenv/bin/python -m ftmemfs.localmanager $zk  > /local/$USER/local_manager.log 2>&1 &'"
