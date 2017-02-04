#!/bin/bash
if [[ $# != 5 ]]; then
    echo "Invalid number of parameters"
    exit 1
fi

NODES=$1
RESERVATION_FILE=$2
HOSTS=$3
SSD=$4
RESTIME=$5

if [[ $SSD -eq 1 ]]; then
    if [[ `hostname -A | grep das5`  ]]; then
        export RESERVATION=`preserve -1 -# $NODES -t $RESTIME -native '-C ssd' | grep "Reservation number" | awk '{ print $3 }' | sed 's/://'`
    else
        export RESERVATION=`preserve -1 -# $NODES -t $RESTIME -native '-l ssd=1' | grep "Reservation number" | awk '{ print $3 }' | sed 's/://'`
    fi
else
    export RESERVATION=`preserve -1 -# $NODES -t $RESTIME | grep "Reservation number" | awk '{ print $3 }' | sed 's/://'`
fi

echo $RESERVATION > $RESERVATION_FILE
echo "Made reservation $RESERVATION"
> $HOSTS

while [ `preserve -llist | grep $RESERVATION | awk '{ print $9 }'` == "-" ]
do
echo "WAITING FOR THE NODES TO BE AVAILABLE....";
sleep 2;
done

IPS=`preserve -llist | grep $RESERVATION | tr -d '\t' | awk -F"node" '{ for(i = 0; i < 100; i++) { if ($i) print $i; } }' | grep -v $USER |  sed 's/0*//' | tr -d '\n'`

for ip in $IPS ; do
    echo "10.149.0.$ip" >> $HOSTS
done
