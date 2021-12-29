/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

//#define FAKE_CPP_SERVER

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <abt.h>
#include <margo.h>

#include "mobject-server.h"
#include "src/server/mobject-provider.h"
#include "src/rpc-types/write-op.h"
#include "src/rpc-types/read-op.h"
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
DECLARE_MARGO_RPC_HANDLER(mobject_server_clean_ult)
DECLARE_MARGO_RPC_HANDLER(mobject_server_stat_ult)

static void mobject_finalize_cb(void* data);

int mobject_provider_register(margo_instance_id                  mid,
                              uint16_t                           provider_id,
                              bake_provider_handle_t             bake_ph,
                              sdskv_provider_handle_t            sdskv_ph,
                              struct mobject_provider_init_args* args,
                              mobject_provider_t*                provider)
{
    mobject_provider_t tmp_provider;
    int                my_rank;
    int                ret;

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t   id;
        hg_bool_t flag;
        margo_provider_registered_name(mid, "mobject_write_op", provider_id,
                                       &id, &flag);
        if (flag == HG_TRUE) {
            margo_error(
                mid,
                "mobject_provider_register(): a provider with the same id "
                "(%d) already exists",
                provider_id);
            return -1;
        }
    }

    tmp_provider = calloc(1, sizeof(*tmp_provider));
    if (!tmp_provider) return -1;
    tmp_provider->mid         = mid;
    tmp_provider->provider_id = provider_id;
    tmp_provider->pool        = args ? args->pool : ABT_POOL_NULL;
    tmp_provider->ref_count   = 1;
    ABT_mutex_create(&tmp_provider->mutex);
    ABT_mutex_create(&tmp_provider->stats_mutex);

    /* Bake settings initialization */
    bake_provider_handle_ref_incr(bake_ph);
    tmp_provider->bake_ph = bake_ph;
    uint64_t num_targets;
    ret = bake_probe(bake_ph, 1, &(tmp_provider->bake_tid), &num_targets);
    if (ret != 0) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to probe bake server for targets");
        bake_provider_handle_release(tmp_provider->bake_ph);
        free(tmp_provider);
        return -1;
    }
    if (num_targets < 1) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to find a target on bake provider");
        bake_provider_handle_release(tmp_provider->bake_ph);
        free(tmp_provider);
        return -1;
    }
    /* SDSKV settings initialization */
    sdskv_provider_handle_ref_incr(sdskv_ph);
    tmp_provider->sdskv_ph = sdskv_ph;
    ret = sdskv_open(sdskv_ph, "mobject_oid_map", &(tmp_provider->oid_db_id));
    if (ret != SDSKV_SUCCESS) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to open mobject_oid_map from SDSKV provider\n");
        bake_provider_handle_release(tmp_provider->bake_ph);
        sdskv_provider_handle_release(tmp_provider->sdskv_ph);
        free(tmp_provider);
    }
    ret = sdskv_open(sdskv_ph, "mobject_name_map", &(tmp_provider->name_db_id));
    if (ret != SDSKV_SUCCESS) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to open mobject_name_map from SDSKV provider\n");
        bake_provider_handle_release(tmp_provider->bake_ph);
        sdskv_provider_handle_release(tmp_provider->sdskv_ph);
        free(tmp_provider);
    }
    ret = sdskv_open(sdskv_ph, "mobject_seg_map",
                     &(tmp_provider->segment_db_id));
    if (ret != SDSKV_SUCCESS) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to open mobject_seg_map from SDSKV provider\n");
        bake_provider_handle_release(tmp_provider->bake_ph);
        sdskv_provider_handle_release(tmp_provider->sdskv_ph);
        free(tmp_provider);
    }
    ret = sdskv_open(sdskv_ph, "mobject_omap_map", &(tmp_provider->omap_db_id));
    if (ret != SDSKV_SUCCESS) {
        margo_error(mid,
                    "mobject_provider_register(): "
                    "unable to open mobject_omap_map from SDSKV provider\n");
        bake_provider_handle_release(tmp_provider->bake_ph);
        sdskv_provider_handle_release(tmp_provider->sdskv_ph);
        free(tmp_provider);
    }

    hg_id_t rpc_id;

    /* read/write op RPCs */
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_write_op", write_op_in_t,
                                     write_op_out_t, mobject_write_op_ult,
                                     provider_id, tmp_provider->pool);
    margo_register_data(mid, rpc_id, tmp_provider, NULL);
    tmp_provider->write_op_id = rpc_id;

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_read_op", read_op_in_t,
                                     read_op_out_t, mobject_read_op_ult,
                                     provider_id, tmp_provider->pool);
    margo_register_data(mid, rpc_id, tmp_provider, NULL);
    tmp_provider->read_op_id = rpc_id;

    /* server ctl RPCs */
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_server_clean", void, void,
                                     mobject_server_clean_ult, provider_id,
                                     tmp_provider->pool);
    margo_register_data(mid, rpc_id, tmp_provider, NULL);
    tmp_provider->clean_id = rpc_id;

    rpc_id = MARGO_REGISTER_PROVIDER(mid, "mobject_server_stat", void, void,
                                     mobject_server_stat_ult, provider_id,
                                     tmp_provider->pool);
    margo_register_data(mid, rpc_id, tmp_provider, NULL);
    tmp_provider->stat_id = rpc_id;

    margo_push_finalize_callback(mid, mobject_finalize_cb, (void*)tmp_provider);

    *provider = tmp_provider;

    return 0;
}

static hg_return_t mobject_write_op_ult(hg_handle_t h)
{
    hg_return_t ret;

    write_op_in_t  in;
    write_op_out_t out;

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    const struct hg_info* info = margo_get_info(h);
    margo_instance_id     mid  = margo_hg_handle_get_instance(h);

    server_visitor_args vargs;
    vargs.object_name = in.object_name;
    vargs.oid         = 0;
    vargs.pool_name   = in.pool_name;
    vargs.provider    = margo_registered_data(mid, info->id);
    if (vargs.provider == NULL) return HG_OTHER_ERROR;
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr     = info->addr;
    vargs.bulk_handle     = in.write_op->bulk_handle;

    /* Execute the operation chain */
    // print_write_op(in.write_op, in.object_name);
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

    read_op_in_t  in;
    read_op_out_t out;

    /* Deserialize the input from the received handle. */
    ret = margo_get_input(h, &in);
    assert(ret == HG_SUCCESS);

    /* Create a response list matching the input actions */
    read_response_t resp = build_matching_read_responses(in.read_op);

    const struct hg_info* info = margo_get_info(h);
    margo_instance_id     mid  = margo_hg_handle_get_instance(h);

    server_visitor_args vargs;
    vargs.object_name = in.object_name;
    vargs.oid         = 0;
    vargs.pool_name   = in.pool_name;
    vargs.provider    = margo_registered_data(mid, info->id);
    if (vargs.provider == NULL) return HG_OTHER_ERROR;
    vargs.client_addr_str = in.client_addr;
    vargs.client_addr     = info->addr;
    vargs.bulk_handle     = in.read_op->bulk_handle;

    /* Compute the result. */
    // print_read_op(in.read_op, in.object_name);
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

static hg_return_t mobject_server_clean_ult(hg_handle_t h)
{
    hg_return_t       ret;
    margo_instance_id mid = margo_hg_handle_get_instance(h);

    /* XXX clean up mobject data */
    margo_error(mid, "mobject_server_cleab_ult(): operation not supported");

    ret = margo_respond(h, NULL);
    assert(ret == HG_SUCCESS);

    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_server_clean_ult)

static hg_return_t mobject_server_stat_ult(hg_handle_t h)
{
    hg_return_t ret;
    char        my_hostname[256] = {0};

    const struct hg_info* info = margo_get_info(h);
    margo_instance_id     mid  = margo_hg_handle_get_instance(h);

    struct mobject_provider* provider = margo_registered_data(mid, info->id);
    gethostname(my_hostname, sizeof(my_hostname));

    ABT_mutex_lock(provider->stats_mutex);
    margo_info(mid,
               "Server (host: %s):\n"
               "\tSegments allocated: %u\n"
               "\tTotal segment size: %lu bytes\n"
               "\tTotal segment write time: %.4lf s\n"
               "\tTotal segment write b/w: %.4lf MiB/s",
               my_hostname, provider->segs, provider->total_seg_size,
               provider->total_seg_wr_duration,
               (provider->total_seg_size / (1024.0 * 1024.0)
                / provider->total_seg_wr_duration));
    ABT_mutex_unlock(provider->stats_mutex);

    ret = margo_respond(h, NULL);
    assert(ret == HG_SUCCESS);

    ret = margo_destroy(h);
    assert(ret == HG_SUCCESS);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(mobject_server_stat_ult)

static void mobject_finalize_cb(void* data)
{
    mobject_provider_t provider = (mobject_provider_t)data;

    margo_deregister(provider->mid, provider->write_op_id);
    margo_deregister(provider->mid, provider->read_op_id);
    margo_deregister(provider->mid, provider->clean_id);
    margo_deregister(provider->mid, provider->stat_id);

    sdskv_provider_handle_release(provider->sdskv_ph);
    bake_provider_handle_release(provider->bake_ph);
    ABT_mutex_free(&provider->mutex);
    ABT_mutex_free(&provider->stats_mutex);

    free(provider);
}
