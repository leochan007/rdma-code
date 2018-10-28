#!/bin/bash
# Program:
#       Resolve the condition that program in server also need blocksize
#       记得将server的disconnect函数返回值设为1，结束server程序

touch start

for blocksize in 64 512 1024 2048 4096 16384 65536 131072
do
    i=5
    while [ "$i" != "0" ]
    do
        scp start lab@192.168.0.15:~/jyh
        ./rdma-server read 12345 $blocksize
        i=$(($i-1))
    done
done

exit 0