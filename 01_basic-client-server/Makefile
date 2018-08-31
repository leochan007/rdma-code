.PHONY: clean

CFLAGS  := -Wall -Werror -g 
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := server client

all: ${APPS}


clean:
	rm -f ${APPS}