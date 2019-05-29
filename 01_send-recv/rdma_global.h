#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <rdma/rdma_cma.h>
#include <stdio.h>
#include <stdlib.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define TEST_NP(x) do{ if((x) <= 0) die("error " #x " failed (return non-positive)."); }while(0)

#define __INFO
#ifdef __INFO
#define INFO(format, ...) printf(format, ##__VA_ARGS__)
#else
#define INFO(format, ...)
#endif

// User specified context associated with rdma_cm_id
struct rdma_connection {
    struct rdma_cm_id *id;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;

    char *recv_region;
    char *send_region;

    struct ibv_cq* cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};

extern void die(const char *reason);

#endif
