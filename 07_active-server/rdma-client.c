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
static int RDMA_BLOCK_SIZE;
const int TIMEOUT_IN_MS = 500;
char *app_data;

cycles_t start, end;

/*
    set RDMA_BLOCK_SIZE:
        func main
    build app data:
        func register_memory
    design message:
        struct message
    initial index:
        struct context
        func build_context
    send read data message:
        func send_mr_read_data
    send read done message:
        func send_mr_read_done
*/

enum mode {
    M_WRITE,
    M_READ
};

/* design message */
struct message {
    enum {
        MSG_READ_DATA,
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
    /* initialize index */
    unsigned long index;
    /* end */

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

static int on_addr_resolved(struct rdma_cm_id *id);
static void build_connection(struct rdma_cm_id *id);
static void build_context(struct ibv_context *verbs);
static void * poll_cq(void *ctx);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void register_memory(struct connection *conn);
static void post_receives(struct connection *conn);

static int on_route_resolved(struct rdma_cm_id *id);
static void build_params(struct rdma_conn_param *params);

static int on_connection(struct rdma_cm_id *id);
static void on_connect(void *context);
static void send_mr_read_data(void *context, unsigned long index);
static void send_message(struct connection *conn);

static int on_disconnect(struct rdma_cm_id *id);
static void destroy_connection(void *context);

static void on_completion(struct ibv_wc *wc);
static void send_mr_read_done(void *context);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;

int main(int argc, char **argv)
{
    struct addrinfo *addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn= NULL;
    struct rdma_event_channel *ec = NULL;

    if (argc != 4)
        usage(argv[0]);

    if (strcmp(argv[1], "write") == 0)
        set_mode(M_WRITE);
    else if (strcmp(argv[1], "read") == 0)
        set_mode(M_READ);
    else
        usage(argv[0]);

    /* set RDMA_BLOCK_SIZE */
    RDMA_BLOCK_SIZE = 4 * 1024;
    /* end */

    TEST_NZ(getaddrinfo(argv[2], argv[3], NULL, &addr));

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

    freeaddrinfo(addr);

    while (rdma_get_cm_event(ec, &event) == 0) {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event(&event_copy))
            break;
    }

    rdma_destroy_event_channel(ec);

    return 0;
}

void usage(const char *argv0)
{
    fprintf(stderr, "usage: %s <mode> <server-address> <server-port>\n  mode = \"read\", \"write\"\n", argv0);
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

    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        r = on_addr_resolved(event->id);
    else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        r = on_route_resolved(event->id);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        r = on_connection(event->id);
    else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
        r = on_disconnect(event->id);
    else {
        fprintf(stderr, "on_event: %d\n", event->event);
        die("on_event: unknown event.");
    }

    return r;
}

int on_addr_resolved(struct rdma_cm_id *id)
{
    printf("address resolved.\n");

    build_connection(id);
    TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

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

    /* initialize index */
    s_ctx->index = 0;
    /* end */

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
    /* build app data */
    app_data = malloc(DATA_BUFFER_SIZE);
    memset(app_data, 0, DATA_BUFFER_SIZE);
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

int on_route_resolved(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    printf("route resolved.\n");
    build_params(&cm_params);
    TEST_NZ(rdma_connect(id, &cm_params));

    return 0;
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
    start = get_cycles();
    send_mr_read_data(id->context, s_ctx->index);

    return 0;
}

void on_connect(void *context)
{
    ((struct connection *)context)->connected = 1;
}

void send_mr_read_data(void *context, unsigned long index)
{
    struct connection *conn = (struct connection *)context;

    conn->send_msg->type = MSG_READ_DATA;

    memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
    conn->send_msg->data.index = s_ctx->index;
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

int on_disconnect(struct rdma_cm_id *id)
{
    printf("disconnected.\n");

    destroy_connection(id->context);
    return 1; /* exit event loop */
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
            memcpy(app_data + s_ctx->index * RDMA_BLOCK_SIZE, conn->rdma_remote_region, RDMA_BLOCK_SIZE);
            s_ctx->index++;
            if (s_ctx->index == DATA_BUFFER_SIZE / RDMA_BLOCK_SIZE) {
                end = get_cycles();
                double total_cycles = (double)(end - start);
                double cycles_to_units = get_cpu_mhz(0) * 1000000;
                double bw_avg = ((double) (RDMA_BUFFER_SIZE + (s_ctx->time + 1) * sizeof(struct message)) * cycles_to_units) / (total_cycles * 0x100000);
                double tp_avg = ((double) RDMA_BUFFER_SIZE * cycles_to_units) / (total_cycles * 0x100000);
                printf("\ncpu time : %lf s, bandwidth : %lf MB/s, throughput : %lf MB/s\n", total_cycles / cycles_to_units, bw_avg, tp_avg);
                
                send_mr_read_done(conn);
            } else {
                send_mr_read_data(conn, s_ctx->index);
            }
        }
    } else {
        post_receives(conn);
    }
}

void send_mr_read_done(void *context)
{
    struct connection *conn = (struct connection *)context;

    conn->send_msg->type = MSG_READ_DONE;

    send_message(conn);
}