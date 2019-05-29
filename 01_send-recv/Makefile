.PHONY: clean

CFLAGS	:= -Wall -g
CC		:= gcc
LDLIBS	:= ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS	:= server

all: ${APPS}

server:	server.o rdma_global.o
	${CC} -o $@ $^ ${LDLIBS}

clean:
	rm *.o ${APPS}
