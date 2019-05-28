#include "rdma_global.h"

void die(const char* reason)
{
    fprintf(stderr, "%s\n",reason);
    exit(EXIT_FAILURE);
}

