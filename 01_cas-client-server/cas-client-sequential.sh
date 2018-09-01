#!/bin/bash
# Program:
#       Record data about cpu time and throughput under cas-sequential-read mode automatically

ip=192.168.0.120
port=9999

if [ -f "data-cas-sequential" ]; then
    rm data-cas-sequential
fi

for blocksize in 64 512 1024 2048 4096 16384 65536 131072
do
    i=5
    while ["$i" != "0"]
    do
        ./rdma-client read $ip $port $blocksize
        i=$(($i-1))
    done
done

exit 0