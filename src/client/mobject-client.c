/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "mobject-store-config.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <margo.h>
#include <ssg.h>

#include "mobject-client.h"
#include "src/client/mobject-client-impl.h"
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/prepare-read-op.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
#include "src/util/log.h"

static int mobject_client_register(mobject_client_t  client,
                                   margo_instance_id mid)
{
    int       ret;
    hg_addr_t client_addr = HG_ADDR_NULL;
    char      client_addr_str[128];
    hg_size_t client_addr_len = 128;

    ret = margo_addr_self(mid, &client_addr);
    if (ret != HG_SUCCESS) return -1;

    ret = margo_addr_to_string(mid, client_addr_str, &client_addr_len,
                               client_addr);
    if (ret != HG_SUCCESS) {
        margo_addr_free(mid, client_addr);
        return -1;
    }

    margo_addr_free(mid, client_addr);

    client->mid         = mid;
    client->client_addr = strdup(client_addr_str);

    /* check if RPCs have already been registered */
    hg_bool_t flag;
    hg_id_t   id;
    margo_registered_name(mid, "mobject_write_op", &id, &flag);

    if (flag == HG_TRUE) { /* RPCs already registered */

        margo_registered_name(mid, "mobject_write_op",
                              &client->mobject_write_op_rpc_id, &flag);
        margo_registered_name(mid, "mobject_read_op",
                              &client->mobject_read_op_rpc_id, &flag);

    } else {

        client->mobject_write_op_rpc_id = MARGO_REGISTER(
            mid, "mobject_write_op", write_op_in_t, write_op_out_t, NULL);
        client->mobject_read_op_rpc_id = MARGO_REGISTER(
            mid, "mobject_read_op", read_op_in_t, read_op_out_t, NULL);
    }

    return 0;
}

int mobject_client_init(margo_instance_id mid, mobject_client_t* client)
{
    mobject_client_t c = (mobject_client_t)calloc(1, sizeof(*c));
    if (!c) return -1;

    c->num_provider_handles = 0;

    int ret = mobject_client_register(c, mid);
    if (ret != 0) return ret;

    *client = c;
    return 0;
}

int mobject_client_finalize(mobject_client_t client)
{
    if (client->num_provider_handles != 0) {
        fprintf(stderr,
                "[MOBJECT] Warning: %lu provider handles not released before "
                "mobject_client_finalize was called\n",
                client->num_provider_handles);
    }
    free(client->client_addr);
    free(client);
    return 0;
}

int mobject_provider_handle_create(mobject_client_t           client,
                                   hg_addr_t                  addr,
                                   uint16_t                   provider_id,
                                   mobject_provider_handle_t* handle)
{
    if (client == MOBJECT_CLIENT_NULL) return -1;

    mobject_provider_handle_t provider
        = (mobject_provider_handle_t)calloc(1, sizeof(*provider));

    if (!provider) return -1;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(provider->addr));
    if (ret != HG_SUCCESS) {
        free(provider);
        return -1;
    }

    provider->client      = client;
    provider->provider_id = provider_id;
    provider->refcount    = 1;

    client->num_provider_handles += 1;

    *handle = provider;
    return 0;
}

int mobject_provider_handle_ref_incr(mobject_provider_handle_t handle)
{
    if (handle == MOBJECT_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount += 1;
    return 0;
}

int mobject_provider_handle_release(mobject_provider_handle_t handle)
{
    if (handle == MOBJECT_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount -= 1;
    if (handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_provider_handles -= 1;
        free(handle);
    }
    return 0;
}

int mobject_shutdown(mobject_client_t client, hg_addr_t addr)
{
    return margo_shutdown_remote_instance(client->mid, addr);
}

int mobject_write_op_operate(mobject_provider_handle_t mph,
                             mobject_store_write_op_t  write_op,
                             const char*               pool_name,
                             const char*               oid,
                             time_t*                   mtime,
                             int                       flags)
{
    hg_return_t ret;

    write_op_in_t in;
    in.object_name = oid;
    in.pool_name   = pool_name;
    in.write_op    = write_op;
    in.client_addr = mph->client->client_addr;
    // TODO take mtime into account

    prepare_write_op(mph->client->mid, write_op);

    hg_addr_t svr_addr = mph->addr;
    if (svr_addr == HG_ADDR_NULL) {
        fprintf(stderr,
                "[MOBJECT] NULL provider address passed to "
                "mobject_write_op_operate\n");
        return -1;
    }

    hg_handle_t h;
    ret = margo_create(mph->client->mid, svr_addr,
                       mph->client->mobject_write_op_rpc_id, &h);
    if (ret != HG_SUCCESS) {
        fprintf(
            stderr,
            "[MOBJECT] margo_create() failed in mobject_write_op_operate()\n");
        return -1;
    }

    ret = margo_provider_forward(mph->provider_id, h, &in);
    if (ret != HG_SUCCESS) {
        fprintf(
            stderr,
            "[MOBJECT] margo_forward() failed in mobject_write_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    write_op_out_t resp;
    ret = margo_get_output(h, &resp);
    if (ret != HG_SUCCESS) {
        fprintf(stderr,
                "[MOBJECT] margo_get_output() failed in "
                "mobject_write_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    margo_free_output(h, &resp);

    margo_destroy(h);

    return 0;
}

int mobject_read_op_operate(mobject_provider_handle_t mph,
                            mobject_store_read_op_t   read_op,
                            const char*               pool_name,
                            const char*               oid,
                            int                       flags)
{
    hg_return_t ret;

    read_op_in_t in;
    in.object_name = oid;
    in.pool_name   = pool_name;
    in.read_op     = read_op;
    in.client_addr = mph->client->client_addr;
    ;

    prepare_read_op(mph->client->mid, read_op);

    hg_addr_t svr_addr = mph->addr;

    if (svr_addr == HG_ADDR_NULL) {
        fprintf(stderr,
                "[MOBJECT] NULL provider address passed to "
                "mobject_write_op_operate\n");
        return -1;
    }

    hg_handle_t h;
    ret = margo_create(mph->client->mid, svr_addr,
                       mph->client->mobject_read_op_rpc_id, &h);
    if (ret != HG_SUCCESS) {
        fprintf(
            stderr,
            "[MOBJECT] margo_create() failed in mobject_read_op_operate()\n");
        return -1;
    }

    ret = margo_provider_forward(mph->provider_id, h, &in);
    if (ret != HG_SUCCESS) {
        fprintf(
            stderr,
            "[MOBJECT] margo_forward() failed in mobject_read_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    read_op_out_t resp;
    ret = margo_get_output(h, &resp);
    if (ret != HG_SUCCESS) {
        fprintf(stderr,
                "[MOBJECT] margo_get_output() failed in "
                "mobject_read_op_operate()\n");
        margo_destroy(h);
        return -1;
    }

    feed_read_op_pointers_from_response(read_op, resp.responses);

    margo_free_output(h, &resp);
    margo_destroy(h);

    return 0;
}
