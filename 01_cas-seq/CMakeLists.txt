CMAKE_MINIMUM_REQUIRED(VERSION 3.13.1)

SET(EXECUTABLE_OUTPUT_PATH ./bin)

add_executable(
	client

	./get_clock.c 
    ./rdma-client.c
)

add_executable(
	server

	./get_clock.c
    ./rdma-server.c
)

target_link_libraries (	
	client
	
	pthread
	ibverbs
    rdmacm
)

target_link_libraries (	
	server
	
	pthread
	ibverbs
    rdmacm
)

set_target_properties(client PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(server PROPERTIES LINKER_LANGUAGE CXX)
