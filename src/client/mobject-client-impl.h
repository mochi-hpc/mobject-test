/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_CLIENT_IMPL_H
#define __MOBJECT_CLIENT_IMPL_H

#include "mobject-store-config.h"

#include <stdlib.h>
#include <margo.h>
#include <ssg.h>

#include "mobject-client.h"

struct mobject_client {

    margo_instance_id mid;

    char* client_addr;

    hg_id_t mobject_write_op_rpc_id;
    hg_id_t mobject_read_op_rpc_id;
    hg_id_t mobject_shutdown_rpc_id;

    uint64_t num_provider_handles;
};

struct mobject_provider_handle {
    mobject_client_t client;
    hg_addr_t        addr;
    uint16_t         provider_id;
    uint64_t         refcount;
};

typedef enum mobject_op_req_type
{
    MOBJECT_AIO_WRITE,
    MOBJECT_AIO_READ
} mobject_op_req_type;

struct mobject_request {
    mobject_op_req_type type; // type of operation that initiated the request
    union {
        mobject_store_read_op_t  read_op;
        mobject_store_write_op_t write_op;
    } op;                  // operation that initiated the request
    margo_request request; // margo request to wait on
    hg_handle_t   handle;  // handle of the RPC sent for this operation
};

#endif
