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

DECLARE_MARGO_RPC_HANDLER(mobject_write_op_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_read_op_ult)

static void mobject_finalize_cb(void* data);

int mobject_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        ABT_pool pool,
        bake_provider_handle_t bake_ph,
        sdskv_provider_handle_t sdskv_ph,
        ssg_group_id_t gid,
        const char *cluster_file,
        mobject_provider_t* provider)
{
    mobject_provider_t srv_ctx;
    int my_id;
    int ret;

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t id;
        hg_bool_t flag;
        margo_provider_registered_name(mid, "mobject_write_op", provider_id, &id, &flag);
        if(flag == HG_TRUE) {
            fprintf(stderr, "mobject_provider_register(): a provider with the same id (%d) already exists\n", provider_id);
            return -1;
        }
    }


    srv_ctx = calloc(1, sizeof(*srv_ctx));
    if (!srv_ctx)
        return -1;
    srv_ctx->mid = mid;
    srv_ctx->provider_id = provider_id;
    srv_ctx->pool = pool;
    srv_ctx->ref_count = 1;

    srv_ctx->gid = gid; 
    my_id = ssg_get_group_self_id(srv_ctx->gid);

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
            free(srv_ctx);
            return -1;
        }
    }

    /* Bake settings initialization */
    bake_provider_handle_ref_incr(bake_ph);
    srv_ctx->bake_ph = bake_ph;
    uint64_t num_targets;
    ret = bake_probe(bake_ph, 1, &(srv_ctx->bake_tid), &num_targets);
    if(ret != 0) {
        fprintf(stderr, "Error: unable to probe bake server for targets\n");
        return -1;
    }
    if(num_targets < 1) {
        fprintf(stderr, "Error: unable to find a target on bake provider\n");
        free(srv_ctx);
        return -1;
    }
    /* SDSKV settings initialization */
    sdskv_provider_handle_ref_incr(sdskv_ph);
    srv_ctx->sdskv_ph = sdskv_ph;
    ret = sdskv_open(sdskv_ph, "oid_map", &(srv_ctx->oid_db_id));
    if(ret != SDSKV_SUCCESS) {
        fprintf(stderr, "Error: unable to open oid_map from SDSKV provider\n");
        bake_provider_handle_release(srv_ctx->bake_ph);
        sdskv_provider_handle_release(srv_ctx->sdskv_ph);
        free(srv_ctx);
    }
    ret = sdskv_open(sdskv_ph, "name_map", &(srv_ctx->name_db_id));
    if(ret != SDSKV_SUCCESS) {
        fprintf(stderr, "Error: unable to open name_map from SDSKV provider\n");
        bake_provider_handle_release(srv_ctx->bake_ph);
        sdskv_provider_handle_release(srv_ctx->sdskv_ph);
        free(srv_ctx);
    }
    ret = sdskv_open(sdskv_ph, "seg_map", &(srv_ctx->segment_db_id));
    if(ret != SDSKV_SUCCESS) {
        fprintf(stderr, "Error: unable to open seg_map from SDSKV provider\n");
        bake_provider_handle_release(srv_ctx->bake_ph);
        sdskv_provider_handle_release(srv_ctx->sdskv_ph);
        free(srv_ctx);
    }
    ret = sdskv_open(sdskv_ph, "omap_map", &(srv_ctx->omap_db_id));
    if(ret != SDSKV_SUCCESS) {
        fprintf(stderr, "Error: unable to open omap_map from SDSKV provider\n");
        bake_provider_handle_release(srv_ctx->bake_ph);
        sdskv_provider_handle_release(srv_ctx->sdskv_ph);
        free(srv_ctx);
    }

    hg_id_t rpc_id;

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_write_op", 
            write_op_in_t, write_op_out_t, mobject_write_op_ult,
            provider_id, pool);
    margo_register_data(mid, rpc_id, srv_ctx, NULL);

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_read_op",
            read_op_in_t, read_op_out_t, mobject_read_op_ult,
            provider_id, pool);
    margo_register_data(mid, rpc_id, srv_ctx, NULL);

    margo_push_finalize_callback(mid, mobject_finalize_cb, (void*)srv_ctx);

    *provider = srv_ctx;

    return 0;
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
    vargs.oid         = 0;
    vargs.pool_name   = in.pool_name;
    vargs.srv_ctx     = margo_registered_data(mid, info->id);
    if(vargs.srv_ctx == NULL) return HG_OTHER_ERROR;
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr = info->addr;
    vargs.bulk_handle = in.write_op->bulk_handle;

    char *test123 = "TEST123";
    oid_t oid = 0;
    hg_size_t s = sizeof(oid);
    sdskv_get(vargs.srv_ctx->sdskv_ph, vargs.srv_ctx->name_db_id, test123, strlen(test123)+1,
        &oid, &s);
#if 0
    /* Execute the operation chain */
    //print_write_op(in.write_op, in.object_name);
#ifdef FAKE_CPP_SERVER
    fake_write_op(in.write_op, &vargs);
#else
    core_write_op(in.write_op, &vargs);
#endif
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
    vargs.oid         = 0;
    vargs.pool_name   = in.pool_name;
    vargs.srv_ctx     = margo_registered_data(mid, info->id);
    if(vargs.srv_ctx == NULL) return HG_OTHER_ERROR;
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr = info->addr;
    vargs.bulk_handle = in.read_op->bulk_handle;

#if 0
    /* Compute the result. */
    //print_read_op(in.read_op, in.object_name);
#ifdef FAKE_CPP_SERVER
    fake_read_op(in.read_op, &vargs);
#else
    core_read_op(in.read_op, &vargs);
#endif
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

static void mobject_finalize_cb(void* data)
{
    mobject_provider_t srv_ctx = (mobject_provider_t)data;

    sdskv_provider_handle_release(srv_ctx->sdskv_ph);
    bake_provider_handle_release(srv_ctx->bake_ph);

    free(srv_ctx);
}
