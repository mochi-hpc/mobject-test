/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/client/mobject-client-impl.h"
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/prepare-read-op.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/util/log.h"

int mobject_aio_write_op_operate(
        mobject_provider_handle_t mph,
        mobject_store_write_op_t write_op,
        const char *pool_name,
        const char *oid,
        time_t *mtime,
        int flags,
        mobject_request_t* req)
{   
    hg_return_t ret;

    write_op_in_t in;
    in.client_addr = mph->client->client_addr;
    in.object_name = oid;
    in.pool_name   = pool_name;
    in.write_op    = write_op;
    // TODO take mtime into account

    prepare_write_op(mph->client->mid, write_op);

    hg_addr_t svr_addr = mph->addr;
    if(svr_addr == HG_ADDR_NULL) {
        fprintf(stderr, "[MOBJECT] NULL provider address passed to mobject_aio_write_op_operate\n");
        return -1;
    }

    hg_handle_t h;
    ret = margo_create(mph->client->mid, svr_addr, mph->client->mobject_write_op_rpc_id, &h);
    if(ret != HG_SUCCESS) {
        fprintf(stderr, "[MOBJECT] margo_create() failed in mobject_aio_write_op_operate()\n");
        return -1;
    }

    margo_request mreq;
    ret = margo_provider_iforward(mph->provider_id, h, &in, &mreq);
    if(ret != HG_SUCCESS) {
        fprintf(stderr, "[MOBJECT] margo_iforward() failed in mobject_aio_write_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    mobject_request_t tmp_req = calloc(1, sizeof(*tmp_req));
    tmp_req->type             = MOBJECT_AIO_WRITE;
    tmp_req->op.write_op      = write_op;
    tmp_req->request          = mreq;
    tmp_req->handle           = h;

    *req = tmp_req;

    return 0;
}

int mobject_aio_read_op_operate(
        mobject_provider_handle_t mph,
        mobject_store_read_op_t read_op,
        const char *pool_name,
        const char *oid,
        int flags,
        mobject_request_t* req)
{   
    hg_return_t ret;

    read_op_in_t in; 
    in.client_addr = mph->client->client_addr;
    in.object_name = oid;
    in.pool_name   = pool_name;
    in.read_op     = read_op;

    prepare_read_op(mph->client->mid, read_op);

    hg_addr_t svr_addr = mph->addr; 
    if(svr_addr == HG_ADDR_NULL) {
        fprintf(stderr, "[MOBJECT] NULL provider address passed to mobject_aio_read_op_operate\n");
        return -1;
    }

    hg_handle_t h;
    ret = margo_create(mph->client->mid, svr_addr, mph->client->mobject_read_op_rpc_id, &h);
    if(ret != HG_SUCCESS) {
        fprintf(stderr, "[MOBJECT] margo_forward() failed in mobject_write_op_operate()\n");
        return -1;
    }

    margo_request mreq;
    ret = margo_provider_iforward(mph->provider_id, h, &in, &mreq);
    if(ret != HG_SUCCESS) {
        fprintf(stderr, "[MOBJECT] margo_iforward() failed in mobject_aio_write_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    mobject_request_t tmp_req = calloc(1, sizeof(*tmp_req));
    tmp_req->type             = MOBJECT_AIO_READ;
    tmp_req->op.read_op       = read_op;
    tmp_req->request          = mreq;
    tmp_req->handle           = h;

    *req = tmp_req;

    return 0;
}

int mobject_aio_wait(mobject_request_t req, int* ret)
{
    if(req == MOBJECT_REQUEST_NULL)
        return -1;

    int r = margo_wait(req->request);
    if(r != HG_SUCCESS) {
        return r;
    }
    req->request = MARGO_REQUEST_NULL;

    switch(req->type) {

        case MOBJECT_AIO_WRITE: {
            write_op_out_t resp;
            r = margo_get_output(req->handle, &resp);
            if(r != HG_SUCCESS) {
                *ret = r;
                margo_destroy(req->handle);
                free(req);
                return r;
            }
            *ret = resp.ret;
            r = margo_free_output(req->handle,&resp);
            if(r != HG_SUCCESS) {
                *ret = r;
            }
            margo_destroy(req->handle);
            free(req);
            return r;
        } break;

        case MOBJECT_AIO_READ: {
            read_op_out_t resp;
            r = margo_get_output(req->handle, &resp);
            if(r != HG_SUCCESS) {
                *ret = r;
                margo_destroy(req->handle);
                free(req);
                return r;
            }
            feed_read_op_pointers_from_response(req->op.read_op, resp.responses);
            r = margo_free_output(req->handle, &resp);
            if(r != HG_SUCCESS) {
                *ret = r;
            }
            margo_destroy(req->handle);
            free(req);
            return r;
        } break;
    }

    return(0);
}

int mobject_aio_test(mobject_request_t req, int* flag)
{
    if(req == MOBJECT_REQUEST_NULL) return -1;
    return margo_test(req->request, flag);
}
