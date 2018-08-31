#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <rdma/rdma_cma.h>
#include "get_clock.h"

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const int TIMEOUT_IN_MS = 500;
unsigned long RDMA_BUFFER_SIZE = 1024 * 1024 * 1024;
unsigned long RDMA_BLOCK_SIZE;
int offset = 0;
char *point;
unsigned long *rand_offset;

cycles_t start, end;
double cycles_to_units, sum_of_test_cycles;

struct connection_client
{
    struct rdma_cm_id *id;
    struct ibv_qp *qp;
    int connected;

    struct ibv_mr *rdma_local_mr;
    struct message *recv_msg;
    struct ibv_mr *recv_mr;
    char *rdma_local_region;
	
	struct message *send_msg;
	struct ibv_mr *send_mr;

    struct ibv_mr server_mr;

    enum
    {
        RS_INIT,
        RS_DONE,
    } recv_state;
};

struct message
{
    enum
    {
        MSG_MR,
        MSG_DONE,
    } type;

    union {
        struct ibv_mr mr;
    } data;
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

int on_addr_resolved(struct rdma_cm_id *id);
int on_connection_client(struct rdma_cm_id *id);
int on_disconnect_client(struct rdma_cm_id *id);
int on_event(struct rdma_cm_event *event);
int on_route_resolved(struct rdma_cm_id *id);
void usage(const char *argv0);
void *poll_cq(void *context);
void destroy_connection_client(void *context);
void on_connect_client(void *context);

void die(const char *reason)
{
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

void post_receives(struct connection_client *conn)
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

int on_connection_client(struct rdma_cm_id *id)
{
    on_connect_client(id->context);
    return 0;
}

void register_memory_client(struct connection_client *conn)
{
    conn->recv_msg = malloc(sizeof(struct message));
    conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
    TEST_Z(conn->recv_mr = ibv_reg_mr(s_ctx->pd, conn->recv_msg, sizeof(struct message), IBV_ACCESS_LOCAL_WRITE));
    TEST_Z(conn->rdma_local_mr = ibv_reg_mr(s_ctx->pd, conn->rdma_local_region, RDMA_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ));
	conn->send_msg = malloc(sizeof(struct message));
    TEST_Z(conn->send_mr = ibv_reg_mr(s_ctx->pd, conn->send_msg, sizeof(struct message), IBV_ACCESS_LOCAL_WRITE));
}

void build_qp_attr_client(struct ibv_qp_init_attr *qp_attr)
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

void build_params_client(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7;
}

void build_context_client(struct ibv_context *verbs)
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

void build_connection_client(struct rdma_cm_id *id)
{
    struct connection_client *conn;
    struct ibv_qp_init_attr qp_attr;

    build_context_client(id->verbs);
    build_qp_attr_client(&qp_attr);
    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
    id->context = conn = (struct connection_client *)malloc(sizeof(struct connection_client));
    conn->id = id;
    conn->qp = id->qp;
    conn->connected = 0;
    register_memory_client(conn);
    post_receives(conn);
}

int largest_prime_smaller_n(int n)
{
	for(int i = n; i >= 2; i--)
	{
		int j = 2;
		for(j = 2; j < i; j++)
		{
			if(i % j == 0)
				break;
		}
		if(j >= i)
			return i;
	}
	return 2;
}

void random_num(int offset_num)
{
    unsigned long addr = 0;
    unsigned long step = 513 * 4096;
    rand_offset = (unsigned long*)malloc(sizeof(unsigned long)*offset_num);
    for(int i = 0; i < offset_num; i++)
    {
        addr += step;
        while(addr >= offset_num)
            addr -= offset_num;
        rand_offset[i] = addr;
    }
}

int main(int argc, char **argv)
{
    struct addrinfo *addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;

    if (argc != 5)
        usage(argv[0]);

    TEST_Z(RDMA_BLOCK_SIZE = atoi(argv[4]));

    TEST_NZ(getaddrinfo(argv[2], argv[3], NULL, &addr));

    TEST_Z(ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS));

    freeaddrinfo(addr);

    while (rdma_get_cm_event(ec, &event) == 0)
    {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (on_event(&event_copy))
            break;
    }

    rdma_destroy_event_channel(ec);

    return 0;
}

int on_addr_resolved(struct rdma_cm_id *id)
{
    build_connection_client(id);
    TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));
    return 0;
}

void on_connect_client(void *context)
{
    ((struct connection_client *)context)->connected = 1;
}

int on_connection(struct rdma_cm_id *id)
{
    on_connect_client(id->context);
    return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
    printf("disconnected.\n");
    destroy_connection_client(id->context);
    return 1; /* exit event loop */
}

void destroy_connection_client(void *context)
{
    struct connection_client *conn = (struct connection_client *)context;

    rdma_destroy_qp(conn->id);
    ibv_dereg_mr(conn->recv_mr);
    ibv_dereg_mr(conn->rdma_local_mr);

    free(conn->recv_msg);
    free(conn->rdma_local_region);

    rdma_destroy_id(conn->id);

    free(conn);
}

int on_disconnect_client(struct rdma_cm_id *id)
{
    printf("peer disconnected.\n");
    destroy_connection_client(id->context);
    return 1;
}

int on_event(struct rdma_cm_event *event)
{
    int r = 0;
    switch (event->event)
    {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
        r = on_addr_resolved(event->id);
        break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
        r = on_route_resolved(event->id);
        break;
    case RDMA_CM_EVENT_ESTABLISHED:
        r = on_connection_client(event->id);
        break;
    case RDMA_CM_EVENT_DISCONNECTED:
        r = on_disconnect_client(event->id);
        break;
    default:
        die("on_event: unknown event.");
        break;
    }
    return r;
}

int on_route_resolved(struct rdma_cm_id *id)
{
    struct rdma_conn_param cm_params;

    build_params_client(&cm_params);
    TEST_NZ(rdma_connect(id, &cm_params));

    return 0;
}

void usage(const char *argv0)
{
    fprintf(stderr, "usage: %s <mode> <server-address> <server-port> <block-size>\n  mode = \"read\", \"write\"\n", argv0);
    exit(1);
}

void send_message(struct connection_client *conn)
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

    while(!conn->connected);

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_read_finish(struct connection_client *conn)
{
    memset(conn->send_msg, 0, sizeof(struct message));
    conn->send_msg->type = MSG_DONE;
    send_message(conn);
}


void post_rdma_read_client(struct connection_client *conn)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->server_mr.addr;
    wr.wr.rdma.rkey = conn->server_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_local_region;
    sge.length = RDMA_BUFFER_SIZE;
    sge.lkey = conn->rdma_local_mr->lkey;

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void on_completion_client(struct ibv_wc *wc)
{
    struct connection_client *conn = (struct connection_client *)(uintptr_t)wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS)
        die("not success wc");

    if (wc->opcode & IBV_WC_RECV)
    {
        printf("recv success\n");
        
        if (conn->recv_msg->type == MSG_MR)
        {
            memcpy(&conn->server_mr, &conn->recv_msg->data.mr, sizeof(conn->server_mr));
        }
        start = get_cycles();   
        post_rdma_read_client(conn);
    }
    else
    {
        for(int i = 0; i < RDMA_BUFFER_SIZE; i = i + RDMA_BLOCK_SIZE) {
            point = conn->rdma_local_region + i;
        }
        
        end = get_cycles();
        cycles_to_units = get_cpu_mhz(0) * 1000000;
        sum_of_test_cycles = (double)(end - start);
        double tp_avg = ((double) RDMA_BUFFER_SIZE * cycles_to_units) / (sum_of_test_cycles * 0x100000);
        double bw_avg = ((double) RDMA_BUFFER_SIZE * cycles_to_units) / (sum_of_test_cycles * 0x100000);
        printf("\nsum_of_test_cycles : %lf\n", sum_of_test_cycles);
        printf("\ncpu time : %lf, cpu frequency : %lf hz\n bandwidth : %lf MB/s, throughput : %lf MB/s\n", sum_of_test_cycles/cycles_to_units, cycles_to_units, bw_avg, tp_avg);
        rdma_disconnect(conn->id);
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
            on_completion_client(&wc);
    }

    return NULL;
}
