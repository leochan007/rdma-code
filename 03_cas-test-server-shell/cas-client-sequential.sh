#!/bin/bash
# Program:
#       Record data about cpu time and throughput under cas-sequential-read mode automatically
#       Resolve the condition that program in server also need blocksize

ip=192.168.0.13
port=12345

if [ -f "data-cas-sequential" ]; then
    rm data-cas-sequential
fi

count=0
blocksize=(64 512 1024 2048 4096 16384 65536 131072)

while ((1))
do
    if [ -f "~/jyh/start" ]; then
        rm ~/jyh/start
        ./rdma-client read $ip $port ${blocksize[$((count/5))]}
        count=$(($count+1))
    fi
done

exit 0