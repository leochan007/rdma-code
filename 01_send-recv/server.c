#include "rdma_global.h"
#include <stdint.h>

const uint16_t RDMA_PORT = 12580;
const int BUFFER_SIZE = 1024;

void post_receive(struct rdma_connection *conn)
{
    struct ibv_sge sge = {
        .addr = (uint64_t)conn->recv_region,
        .length = BUFFER_SIZE,
        .lkey = conn->recv_mr->lkey
    };

    struct ibv_recv_wr wr = {
        .wr_id = (uint64_t)conn,
        .next = NULL,
        .sg_list = &sge,
        .num_sge = 1
    };
    struct ibv_recv_wr *bad_wr = NULL;

    TEST_NZ(ibv_post_recv(conn->id->qp, &wr, &bad_wr));
}

void on_completion(struct ibv_wc *wc)
{
    if (wc->status != IBV_WC_SUCCESS)
        die("on_completion: status is not IBV_WC_SUCCESS.");
    
    if (wc->opcode & IBV_WC_RECV) {
        struct rdma_connection *conn = (struct rdma_connection *)wc->wr_id;
        printf("received message: %s\n", conn->recv_region);
    } else if (wc->opcode == IBV_WC_SEND) {
        printf("send completed successfully.\n");
    }
}

void *poll_cq(void *context)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;
    struct rdma_connection *conn = (struct rdma_connection *)context;

    while (1) {
        TEST_NZ(ibv_get_cq_event(conn->comp_channel, &cq, NULL));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc))
            on_completion(&wc);
    }

    return NULL;
}

int on_connect_request(struct rdma_cm_id *id)
{
    struct rdma_connection *conn;

    INFO("Server has received connection request.\n");

    // build rdma context
    id->context = conn = (void *)malloc(sizeof(struct rdma_connection));

    conn->id = id;
    TEST_Z(id->pd = ibv_alloc_pd(id->verbs));
    TEST_Z(conn->comp_channel = ibv_create_comp_channel(id->verbs));
    TEST_Z(conn->cq = ibv_create_cq(id->verbs, 10, NULL, conn->comp_channel, 0));
    TEST_NZ(ibv_req_notify_cq(conn->cq, 0));

    TEST_NZ(pthread_create(&conn->cq_poller_thread, NULL, poll_cq, conn));

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = conn->cq,
        .recv_cq = conn->cq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = 10,
            .max_recv_wr = 10,
            .max_send_sge = 1,
            .max_recv_sge = 1
        }
    };
    TEST_NZ(rdma_create_qp(id, id->pd, &qp_attr));

    // register memory
    conn->send_region = (char *)malloc(BUFFER_SIZE);
    conn->recv_region = (char *)malloc(BUFFER_SIZE);

    TEST_Z(conn->send_mr = ibv_reg_mr(id->pd, 
                                    conn->send_region, 
                                    BUFFER_SIZE, 
                                    0));

    TEST_Z(conn->recv_mr = ibv_reg_mr(id->pd, 
                                    conn->recv_region, 
                                    BUFFER_SIZE, 
                                    IBV_ACCESS_LOCAL_WRITE));

    post_receive(conn);

    TEST_NZ(rdma_accept(id, NULL));

    return 0;
}

int on_connection(struct rdma_cm_id *id)
{
    struct rdma_connection *conn = (struct rdma_connection *)id->context;
    
    struct ibv_sge sge = {
        .addr = (uint64_t)conn->send_region,
        .length = BUFFER_SIZE,
        .lkey = conn->send_mr->lkey
    };

    struct ibv_send_wr wr = {
        .opcode = IBV_WR_SEND,
        .sg_list = &sge,
        .num_sge = 1,
        .send_flags = IBV_SEND_SIGNALED
    };
    struct ibv_send_wr *bad_wr = NULL;

    INFO("Server has been connected. Post send...\n");

    ibv_post_send(id->qp, &wr, &bad_wr);

    return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
    struct rdma_connection *conn = (struct rdma_connection *)id->context;

    printf("peer disconnected.\n");

    rdma_destroy_qp(id);
    
    ibv_destroy_cq(conn->cq);
    ibv_destroy_comp_channel(conn->comp_channel);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);

    free(conn->send_region);
    free(conn->recv_region);

    ibv_dealloc_pd(id->pd);

    free(conn);
    rdma_destroy_id(id);

    return 0;
}

int on_event(struct rdma_cm_event *event)
{
    int ret = 0;

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        ret = on_connect_request(event->id);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        ret = on_connection(event->id);
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        ret = on_disconnect(event->id);
    else
        die("on_event: unknown event.");

    return ret;
}

int main(int argc, char **argv)
{
    struct sockaddr_in addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;

    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    TEST_Z(addr.sin_port = htons(RDMA_PORT));

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(listener, 10));

    while (rdma_get_cm_event(ec, &event) == 0) {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event(&event_copy))
            break;
    }

    rdma_destroy_id(listener);
    rdma_destroy_event_channel(ec);

    return 0;
}
