#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

unsigned long RDMA_BUFFER_SIZE = 1024 * 1024 * 1024;

struct message
{
    enum
    {
        MSG_MR,
        MSG_DONE
    } type;

    union {
        struct ibv_mr mr;
    } data;
};

struct connection_server
{
    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *rdma_remote_mr;
    struct message *send_msg;
    struct ibv_mr *send_mr;
    char *rdma_remote_region;
	
    struct message *recv_msg;
    struct ibv_mr *recv_mr;
	
    enum
    {
        SS_INIT,
        SS_MR_SENT,
        SS_DONE,
    } send_state;
};

struct context
{
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};

static struct context *s_ctx = NULL;

static int on_connect_request(struct rdma_cm_id *id);
static int on_connection_server(struct rdma_cm_id *id);
static int on_disconnect_server(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void usage(const char *argv0);
void *poll_cq(void *context);



void die(const char *reason)
{
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

void build_context_server(struct ibv_context *verbs)
{
    if (s_ctx)
    {
        if (s_ctx->ctx != verbs)
            die("Error build context");

        return;
    }

    s_ctx = (struct context *)malloc(sizeof(struct context));
    s_ctx->ctx = verbs;

    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0));
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void post_receives_server(struct connection_server *conn)
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

void register_memory_server(struct connection_server *conn)
{
    conn->send_msg = malloc(sizeof(struct message));
    conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);
    memset(conn->rdma_remote_region, 'a', RDMA_BUFFER_SIZE);

    TEST_Z(conn->send_mr = ibv_reg_mr(s_ctx->pd, conn->send_msg, sizeof(struct message), IBV_ACCESS_LOCAL_WRITE));
    TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(s_ctx->pd, conn->rdma_remote_region, RDMA_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ));

    conn->recv_msg = malloc(sizeof(struct message));
    memset(conn->recv_msg, 0, sizeof(struct message));
    TEST_Z(conn->recv_mr = ibv_reg_mr(s_ctx->pd, conn->recv_msg, sizeof(struct message), IBV_ACCESS_LOCAL_WRITE));
}

void build_qp_attr_server(struct ibv_qp_init_attr *qp_attr)
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

void build_params_server(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7;
}

void build_connection_server(struct rdma_cm_id *id)
{
    struct connection_server *conn;
    struct ibv_qp_init_attr qp_attr;

    build_context_server(id->verbs);
    build_qp_attr_server(&qp_attr);
    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
    id->context = conn = (struct connection_server *)malloc(sizeof(struct connection_server));
    conn->id = id;
    conn->qp = id->qp;
    conn->connected = 0;
    register_memory_server(conn);
}

int main(int argc, char **argv)
{
    struct sockaddr_in addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;
    uint16_t port = 0;

    if (argc != 3)
        usage(argv[0]);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    TEST_Z(port = atoi(argv[2]));
    addr.sin_port = htons(port);

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
    TEST_NZ(rdma_listen(listener, 10));

    port = ntohs(rdma_get_src_port(listener));

    printf("listening on port %d.\n", port);

    while (rdma_get_cm_event(ec, &event) == 0)
    {
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

int on_connect_request(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    build_connection_server(id);
    build_params_server(&cm_params);

    TEST_NZ(rdma_accept(id, &cm_params));

    return 0;
}

void on_connect_server(void *context)
{
    ((struct connection_server *)context)->connected = 1;
}

void send_message(struct connection_server *conn)
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

void send_mr(void *context)
{
    struct connection_server *conn = (struct connection_server *)context;
    conn->send_msg->type = MSG_MR;
    memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));
    send_message(conn);
}

int on_connection_server(struct rdma_cm_id *id)
{
    on_connect_server(id->context);
    send_mr(id->context);
    return 0;
}

void destroy_connection_server(void *context)
{
    struct connection_server *conn = (struct connection_server *)context;

    rdma_destroy_qp(conn->id);
    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->rdma_remote_mr);

    free(conn->send_msg);
    free(conn->rdma_remote_region);

    rdma_destroy_id(conn->id);

    free(conn);
}

int on_disconnect_server(struct rdma_cm_id *id)
{
    printf("peer disconnected.\n");
    destroy_connection_server(id->context);
    return 1;
}

int on_event(struct rdma_cm_event *event)
{
    int r = 0;

    switch (event->event)
    {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        r = on_connect_request(event->id);
        break;
    case RDMA_CM_EVENT_ESTABLISHED:
        r = on_connection_server(event->id);
        break;
    case RDMA_CM_EVENT_DISCONNECTED:
        r = on_disconnect_server(event->id);
        break;
    default:
        die("on_event: unknown event.");
        break;
    }
    return r;
}

void usage(const char *argv0)
{
    fprintf(stderr, "usage: %s <mode> <server-port>\n  mode = \"read\", \"write\"\n", argv0);
    exit(1);
}

void on_completion_server(struct ibv_wc *wc)
{
    struct connection_server *conn = (struct connection_server *)(uintptr_t)wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS)
        die("not success wc");

    if (wc->opcode & IBV_WC_RECV)
    {
        printf("\nrecv success\n");
        if (conn->recv_msg->type == MSG_DONE){
            printf("Client read finish\n");
            rdma_disconnect(conn->id);
        }
    }
    else
    {
        printf("send success\n");
		post_receives_server(conn);
    }
}

void *poll_cq(void *context)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1)
    {
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &context));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc))
            on_completion_server(&wc);
    }

    return NULL;
}
