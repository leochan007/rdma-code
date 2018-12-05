#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include "get_clock.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

static const int RDMA_BUFFER_SIZE = 1 * 1024 * 1024;
static const int DATA_BUFFER_SIZE = 1 * 1024 * 1024;
static const int DATA_BLOCK_SIZE = 4 * 1024;
static int RDMA_BLOCK_SIZE = 4 * 1024;
char *app_data;
unsigned long *data_mapping_table;
unsigned long pre_index = 0;

/*
    fix port:
        func main

    build app data with mapping table:
        func register_memory

    design  message:
        struct message

    send write data:
        func send_write_data

    look up data:
        func look_up_addr

    send post rdma post write:
        func send_post_rdma_write

    send rdma write finish:
        func send_mr_rdma_write_finish

    Q:
        RDMA_BUFFER_SIZE need 1024 * 1024 * 1024 ?
*/

enum mode {
    M_WRITE,
    M_READ
};

/* design message */
struct message {
    enum {
        MSG_READ_DATA,
        MSG_RDMA_WRITE_FINISH,
        MSG_READ_DONE
    } type;

    union {
        struct ibv_mr mr;
        unsigned long index;
    } data;
};
/* end */

struct context {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};

struct connection {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    struct ibv_mr *rdma_local_mr;
    struct ibv_mr *rdma_remote_mr;

    struct ibv_mr peer_mr;

    struct message *recv_msg;
    struct message *send_msg;

    char *rdma_local_region;
    char *rdma_remote_region;

    enum {
        SS_INIT,
        SS_MR_SENT,
        SS_RDMA_SENT,
        SS_DONE_SENT
    } send_state;

    enum {
        RS_INIT,
        RS_MR_RECV,
        RS_DONE_RECV
    } recv_state;
};

static void usage(const char *argv0);
static void set_mode(enum mode m);
static void die(const char *reason);
static int on_event(struct rdma_cm_event *event);

static int on_connect_request(struct rdma_cm_id *id);
static void build_connection(struct rdma_cm_id *id);
static void build_context(struct ibv_context *verbs);
static void * poll_cq(void *ctx);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void register_memory(struct connection *conn);
static void post_receives(struct connection *conn);
static void build_params(struct rdma_conn_param *params);

static int on_connection(struct rdma_cm_id *id);
static void on_connect(void *context);

static int on_disconnect(struct rdma_cm_id *id);
static void destroy_connection(void *context);

static void on_completion(struct ibv_wc *wc);
static void send_write_data(struct connection *conn, unsigned long index);
static unsigned long look_up_addr(unsigned long *p, unsigned long index, unsigned long pre);
static void send_post_rdma_write(struct connection *conn);
static void send_mr_rdma_write_finish(void *context);
static void send_message(struct connection *conn);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;

int main(int argc, char **argv)
{
    struct sockaddr_in addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;
    uint16_t port = 0;

    if (argc != 3)
        usage(argv[0]);

    if (strcmp(argv[1], "write") == 0)
        set_mode(M_WRITE);
    else if (strcmp(argv[1], "read") == 0)
        set_mode(M_READ);
    else
        usage(argv[0]);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    /* fix port */
    TEST_Z(port = atoi(argv[2]));
    addr.sin_port = htons(port);
    /* end */

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

    port = ntohs(rdma_get_src_port(listener));

    printf("listening on port %d.\n", port);

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

void usage(const char *argv0)
{
    fprintf(stderr, "usage: %s <mode> port \n  mode = \"read\", \"write\"\n", argv0);
    exit(1);
}

void set_mode(enum mode m)
{
  s_mode = m;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

int on_event(struct rdma_cm_event *event)
{
    int r = 0;

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        r = on_connect_request(event->id);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection(event->id);
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect(event->id);
    else
        die("on_event: unknown event.");

    return r;
}

int on_connect_request(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    printf("received connection request.\n");
    build_connection(id);
    build_params(&cm_params);
    TEST_NZ(rdma_accept(id, &cm_params));

    return 0;
}

void build_connection(struct rdma_cm_id *id)
{
    struct connection *conn;
    struct ibv_qp_init_attr qp_attr;

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

    id->context = conn = (struct connection *)malloc(sizeof(struct connection));

    conn->id = id;
    conn->qp = id->qp;

    conn->send_state = SS_INIT;
    conn->recv_state = RS_INIT;

    conn->connected = 0;

    register_memory(conn);
    post_receives(conn);
}

void build_context(struct ibv_context *verbs)
{
    if (s_ctx) {
        if (s_ctx->ctx != verbs)
            die("cannot handle events in more than one context.");

        return;
    }

    s_ctx = (struct context *)malloc(sizeof(struct context));

    s_ctx->ctx = verbs;

    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void * poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1) {
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc))
            on_completion(&wc);
    }

    return NULL;
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void register_memory(struct connection *conn)
{
    /* build app data with mapping table */
    unsigned long i, j;
    app_data = malloc(DATA_BUFFER_SIZE);
    for (i = 0, j = 0; i < DATA_BUFFER_SIZE; i++, j++) {
        *(app_data + i) = 'a' + j;
        if (j == 15)
            j = -1;
    }

    unsigned long num_entries = DATA_BUFFER_SIZE / DATA_BLOCK_SIZE;
    data_mapping_table = malloc(num_entries * 8);
    unsigned long *p = data_mapping_table;
    for (i = 0; i < num_entries; i++) {
        *p = (unsigned long)(app_data + i * DATA_BLOCK_SIZE);
        p++;
    }
    /* end */

    conn->send_msg = malloc(sizeof(struct message));
    conn->recv_msg = malloc(sizeof(struct message));

    conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
    conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);

    TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));

    TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    RDMA_BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region, 
    RDMA_BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));
}

void post_receives(struct connection *conn)
{
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t)conn;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->recv_mr->lkey;

    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void build_params(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

int on_connection(struct rdma_cm_id *id)
{
    on_connect(id->context);

    return 0;
}

void on_connect(void *context)
{
    ((struct connection *)context)->connected = 1;
}

int on_disconnect(struct rdma_cm_id *id)
{
    printf("peer disconnected.\n");

    destroy_connection(id->context);
    return 0;
}

void destroy_connection(void *context)
{
    struct connection *conn = (struct connection *)context;

    rdma_destroy_qp(conn->id);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);
    ibv_dereg_mr(conn->rdma_local_mr);
    ibv_dereg_mr(conn->rdma_remote_mr);

    free(conn->send_msg);
    free(conn->recv_msg);
    free(conn->rdma_local_region);
    free(conn->rdma_remote_region);

    rdma_destroy_id(conn->id);

    free(conn);
}

void on_completion(struct ibv_wc *wc)
{
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS)
        die("on_completion: status is not IBV_WC_SUCCESS.");

    if (wc->opcode & IBV_WC_RECV) {
        if (conn->recv_msg->type == MSG_READ_DATA) {
            memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
            send_write_data(conn, conn->recv_msg->data.index);
            send_mr_rdma_write_finish(conn);
            conn->send_state = SS_DONE_SENT;
        }
        if (conn->recv_msg->type == MSG_READ_DONE) {
            on_disconnect(conn->id);
        }
    } else {
        if (conn->send_state == SS_DONE_SENT){
            post_receives(conn);
            conn->send_state = SS_INIT;
        }
    }
}

void send_write_data(struct connection *conn, unsigned long index)
{
    char *data_addr = (char *)look_up_addr(data_mapping_table, index, pre_index);
    memcpy(conn->rdma_local_region, data_addr, RDMA_BLOCK_SIZE);
    send_post_rdma_write(conn);
}

unsigned long look_up_addr(unsigned long *p, unsigned long index, unsigned long pre)
{
    while (pre != index) {
        pre++;
        if (pre >= DATA_BUFFER_SIZE)
            pre = 0;
    }
    return *(p + pre);
} 

void send_post_rdma_write(struct connection *conn)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_local_region;
    sge.length = RDMA_BLOCK_SIZE;
    sge.lkey = conn->rdma_local_mr->lkey;

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr_rdma_write_finish(void *context)
{
    struct connection *conn = (struct connection *)context;

    conn->send_msg->type = MSG_RDMA_WRITE_FINISH;

    send_message(conn);
}

void send_message(struct connection *conn)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)conn->send_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->send_mr->lkey;

    while (!conn->connected);

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}