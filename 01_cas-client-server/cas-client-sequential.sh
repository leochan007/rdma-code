#!/bin/bash
# Program:
#       Record data about cpu time and throughput under cas-sequential-read mode automatically

ip = 192.168.0.120
port = 9999

rm data-cas-sequential

for blocksize in 64 512 1024 2048 4096 16384 65536 131072
do
    for ((i = 0; i < 5; i = i + 1))
    do
        ./rdma-client read $ip $port $blocksize
    done
done