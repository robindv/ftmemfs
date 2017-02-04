#!/bin/bash

if [[ $# != 4 ]]; then
    echo "Usage: start_zookeeper <host> <source_dir> <target_dir> <config_file>"
    exit 1
fi

host=$1
source_dir=$2
target_dir=$3
config_file=$4

ssh $host "rm -rf $target_dir && mkdir -p $target_dir/conf && mkdir -p $target_dir/lib && mkdir -p $target_dir/data && mkdir -p $target_dir/bin && mkdir -p $target_dir/logs"
ssh $host "hostname | sed 's/node0*//' > $target_dir/data/myid"
scp $source_dir/conf/* $host:$target_dir/conf
scp $source_dir/bin/*.sh $host:$target_dir/bin
scp $source_dir/*.jar $host:$target_dir
scp $config_file $host:$target_dir/conf/zoo.cfg
scp $source_dir/lib/*.jar $host:$target_dir/lib

ssh $host "cd $target_dir && ZOO_LOG_DIR=$target_dir/logs ZOO_LOG4J_PROP='ERROR,ROLLINGFILE' ZOOCFGDIR=$target_dir/conf $target_dir/bin/zkServer.sh start"
