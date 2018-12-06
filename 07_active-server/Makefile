.PHONY: clean

# CFLAGS  := -Wall -Werror -g
CFLAGS	:= -Wall -g
LD      := gcc
LDFLAGS := ${LDFLAGS} -lrdmacm -libverbs -lpthread

APPS    := rdma-client rdma-server

all: ${APPS}

rdma-client: rdma-client.o get_clock.o
	${LD} -o $@ $^ ${LDFLAGS}

rdma-server: rdma-server.o get_clock.o
	${LD} -o $@ $^ ${LDFLAGS}


clean:
	rm -f *.o ${APPS}

