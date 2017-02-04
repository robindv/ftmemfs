#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Usage ./start_redis <port_number> <redis_dir> <bin> <base_config>"
    exit 0
fi

# Create configuration file

port=$1
dir=$2
bin=$3
base=$4
config="$dir/redis.conf"

rm -rf $dir
mkdir $dir

echo "include $base" > $config
echo "pidfile $dir/redis.pid" >> $config
#  echo "unixsocket /tmp/redis-${app}.sock"            >> $config
echo "dbfilename dump-${port}.rdb" >> $config
#  echo "vm-swap-file /tmp/redis-${app}.swap"          >> $config
echo "port ${port}" >> $config

sed -i 's/$USER/'"$USER"'/g' $config

#Start redisserver
$bin $config
