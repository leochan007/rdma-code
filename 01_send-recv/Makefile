.PHONY: clean

CFLAGS	:= -Wall -Werror -g
CC		:= gcc
LDLIBS	:= ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS	:= server client

all: ${APPS}

server:	server.o rdma_global.o
	${CC} -o $@ $^ ${LDLIBS}

client: client.o rdma_global.o
	${CC} -o $@ $^ ${LDLIBS}

clean:
	rm *.o ${APPS}
