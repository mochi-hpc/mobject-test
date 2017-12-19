/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

//#define FAKE_CPP_SERVER

#include <assert.h>
#include <mpi.h>
#include <abt.h>
#include <margo.h>
//#include <sds-keyval.h>
#include <bake-bulk-server.h>
#include <bake-bulk-client.h>
//#include <libpmemobj.h>
#include <ssg-mpi.h>

#include "mobject-server.h"
#include "src/server/mobject-server-context.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
//#include "src/server/print-write-op.h"
//#include "src/server/print-read-op.h"
#include "src/io-chain/write-op-impl.h"
#include "src/io-chain/read-op-impl.h"
#include "src/server/visitor-args.h"
#ifdef FAKE_CPP_SERVER
#include "src/server/fake/fake-read-op.h"
#include "src/server/fake/fake-write-op.h"
#else
#include "src/server/core/core-read-op.h"
#include "src/server/core/core-write-op.h"
#endif

static int mobject_server_register(mobject_server_context_t *srv_ctx);
static void mobject_server_cleanup(mobject_server_context_t *srv_ctx);

DECLARE_MARGO_RPC_HANDLER(mobject_write_op_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_read_op_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_shutdown_ult)

/* mobject RPC IDs */
static hg_id_t mobject_write_op_rpc_id;
static hg_id_t mobject_read_op_rpc_id;
static hg_id_t mobject_shutdown_rpc_id;

static int mobject_server_is_initialized = 0;

mobject_server_context_t* mobject_server_init(margo_instance_id mid, const char *cluster_file)
{
    mobject_server_context_t *srv_ctx;
    int my_id;
    int ret;

    if (mobject_server_is_initialized)
    {
        fprintf(stderr, "Error: mobject server has already been initialized\n");
        return NULL;
    }

    srv_ctx = calloc(1, sizeof(*srv_ctx));
    if (!srv_ctx)
        return NULL;
    srv_ctx->mid = mid;
    srv_ctx->ref_count = 1;
    ABT_mutex_create(&srv_ctx->shutdown_mutex);
    ABT_cond_create(&srv_ctx->shutdown_cond);

    ret = ssg_init(mid);
    if (ret != SSG_SUCCESS)
    {
        free(srv_ctx);
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        return NULL;
    }

    /* server group create */
    srv_ctx->gid = ssg_group_create_mpi(MOBJECT_SERVER_GROUP_NAME, MPI_COMM_WORLD,
        NULL, NULL); /* XXX membership update callbacks unused currently */
    if (srv_ctx->gid == SSG_GROUP_ID_NULL)
    {
        fprintf(stderr, "Error: Unable to create the mobject server group\n");
        ssg_finalize();
        free(srv_ctx);
        return NULL;
    }
    my_id = ssg_get_group_self_id(srv_ctx->gid);

    /* register mobject & friends RPC handlers */
    mobject_server_register(srv_ctx);

    /* one proccess writes cluster connect info to file for clients to find later */
    if (my_id == 0)
    {
        ret = ssg_group_id_store(cluster_file, srv_ctx->gid);
        if (ret != 0)
        {
            fprintf(stderr, "Error: unable to store mobject cluster info to file %s\n",
                cluster_file);
            /* XXX: this call is performed by one process, and we do not currently
             * have an easy way to propagate this error to the entire cluster group
             */
            ssg_group_destroy(srv_ctx->gid);
            ssg_finalize();
            free(srv_ctx);
            return NULL;
        }
    }

    /* initialize bake-bulk */
    /* server part */
    bake_server_init(mid, "/dev/shm/mobject.dat");
    // XXX: check return values for the above call

    /* client part */
    hg_addr_t self_addr = ssg_get_addr(srv_ctx->gid, my_id);
    bake_probe_instance(mid, self_addr, &(srv_ctx->bake_id));
    // XXX: check return value of the above calls

    /* TODO setup sds-keyval */

    mobject_server_is_initialized = 1;

    return srv_ctx;
}

void mobject_server_shutdown(mobject_server_context_t *srv_ctx)
{   
    int do_cleanup;

    assert(srv_ctx);

    ABT_mutex_lock(srv_ctx->shutdown_mutex);
    srv_ctx->shutdown_flag = 1;
    ABT_cond_broadcast(srv_ctx->shutdown_cond);

    srv_ctx->ref_count--;
    do_cleanup = srv_ctx->ref_count == 0;

    ABT_mutex_unlock(srv_ctx->shutdown_mutex);

    if (do_cleanup)
        mobject_server_cleanup(srv_ctx);

    return;
}

void mobject_server_wait_for_shutdown(mobject_server_context_t* srv_ctx)
{
    int do_cleanup;

    assert(srv_ctx);

    ABT_mutex_lock(srv_ctx->shutdown_mutex);

    srv_ctx->ref_count++;
    while(!srv_ctx->shutdown_flag)
        ABT_cond_wait(srv_ctx->shutdown_cond, srv_ctx->shutdown_mutex);
    srv_ctx->ref_count--;
    do_cleanup = srv_ctx->ref_count == 0;

    ABT_mutex_unlock(srv_ctx->shutdown_mutex);

    if (do_cleanup)
        mobject_server_cleanup(srv_ctx);

    return;
}

static int mobject_server_register(mobject_server_context_t *srv_ctx)
{
    int ret=0;
    margo_instance_id mid = srv_ctx->mid;

    mobject_write_op_rpc_id = MARGO_REGISTER(mid, "mobject_write_op", 
	write_op_in_t, write_op_out_t, mobject_write_op_ult);

    margo_register_data(mid, mobject_write_op_rpc_id, srv_ctx, NULL);

    mobject_read_op_rpc_id  = MARGO_REGISTER(mid, "mobject_read_op",
        read_op_in_t, read_op_out_t, mobject_read_op_ult);

    margo_register_data(mid, mobject_read_op_rpc_id, srv_ctx, NULL);

    mobject_shutdown_rpc_id = MARGO_REGISTER(mid, "mobject_shutdown",
        void, void, mobject_shutdown_ult);

    margo_register_data(mid, mobject_shutdown_rpc_id, srv_ctx, NULL);

#if 0
    bake_server_register(mid, pool_info);
    metadata = kv_server_register(mid);
#endif

    return ret;
}

static hg_return_t mobject_write_op_ult(hg_handle_t h)
{
    hg_return_t ret;

    write_op_in_t in;
    write_op_out_t out;

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    const struct hg_info* info = margo_get_info(h);
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    server_visitor_args vargs;
    vargs.object_name = in.object_name;
    vargs.pool_name   = in.pool_name;
    vargs.srv_ctx     = margo_registered_data(mid, info->id);
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr = info->addr;
    vargs.bulk_handle = in.write_op->bulk_handle;

    /* Execute the operation chain */
    //print_write_op(in.write_op, in.object_name);
#ifdef FAKE_CPP_SERVER
    fake_write_op(in.write_op, &vargs);
#else
    core_write_op(in.write_op, &vargs);
#endif

    // set the return value of the RPC
    out.ret = 0;

    ret = margo_respond(h, &out);
    assert(ret == HG_SUCCESS);

    /* Free the input data. */
    ret = margo_free_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* We are not going to use the handle anymore, so we should destroy it. */
    ret = margo_destroy(h);
 
    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_write_op_ult)

/* Implementation of the RPC. */
static hg_return_t mobject_read_op_ult(hg_handle_t h)
{
    hg_return_t ret;

    read_op_in_t in;
    read_op_out_t out;

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* Create a response list matching the input actions */
    read_response_t resp = build_matching_read_responses(in.read_op);

    const struct hg_info* info = margo_get_info(h);
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    server_visitor_args vargs;
    vargs.object_name = in.object_name;
    vargs.pool_name   = in.pool_name;
    vargs.srv_ctx     = margo_registered_data(mid,info->id);
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr = info->addr;
    vargs.bulk_handle = in.read_op->bulk_handle;

    /* Compute the result. */
    //print_read_op(in.read_op, in.object_name);
#ifdef FAKE_CPP_SERVER
    fake_read_op(in.read_op, &vargs);
#else
    core_read_op(in.read_op, &vargs);
#endif

    out.responses = resp;

    ret = margo_respond(h, &out);
    assert(ret == HG_SUCCESS);

    free_read_responses(resp);

    /* Free the input data. */
    ret = margo_free_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* We are not going to use the handle anymore, so we should destroy it. */
    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_read_op_ult)

static void mobject_shutdown_ult(hg_handle_t h)
{
    hg_return_t ret;

    const struct hg_info *info = margo_get_info(h);
    margo_instance_id mid = margo_hg_handle_get_instance(h);
    mobject_server_context_t* srv_ctx = margo_registered_data(mid, info->id);

    ret = margo_respond(h, NULL);
    assert(ret == HG_SUCCESS);

    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    /* TODO: propagate shutdown to other servers */
    mobject_server_shutdown(srv_ctx);

    return;
}
DEFINE_MARGO_RPC_HANDLER(mobject_shutdown_ult)

static void mobject_server_cleanup(mobject_server_context_t *srv_ctx)
{
    // cleanup bake-bulk
    bake_shutdown_service(srv_ctx->bake_id);
    bake_release_instance(srv_ctx->bake_id);
    // XXX: check the return value of these calls

    ssg_group_destroy(srv_ctx->gid);
    ssg_finalize();

    //pmemobj_close(NULL);

    ABT_mutex_free(&srv_ctx->shutdown_mutex);
    ABT_cond_free(&srv_ctx->shutdown_cond);
    free(srv_ctx);

    return;
}
