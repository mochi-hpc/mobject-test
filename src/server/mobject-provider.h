/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SERVER_MOBJECT_PROVIDER_H
#define __SERVER_MOBJECT_PROVIDER_H

#include <margo.h>
#include <bake-client.h>
#include <yokan/client.h>
#include <yokan/database.h>
#include <yokan/provider-handle.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOBJECT_SEQ_ID_MAX UINT32_MAX

struct mobject_bake_target {
    bake_provider_handle_t ph;
    bake_target_id_t       tid;
};

struct mobject_provider {
    /* margo/ABT state */
    margo_instance_id mid;
    uint16_t          provider_id;
    ABT_pool          pool;
    ABT_mutex_memory  mutex;
    ABT_mutex_memory  stats_mutex;
    /* bake-related data */
    unsigned                    num_bake_targets;
    struct mobject_bake_target* bake_targets;
    /* yokan-related data */
    yk_database_handle_t oid_dbh;
    yk_database_handle_t name_dbh;
    yk_database_handle_t segment_dbh;
    yk_database_handle_t omap_dbh;
    /* other data */
    uint32_t seq_id;
    int      ref_count;
    /* stats/counters/timers and helpers */
    uint32_t segs;
    uint64_t total_seg_size;
    double   total_seg_wr_duration;
    double   last_wr_start;
    double   last_wr_end;
    /* RPC ids */
    hg_id_t write_op_id;
    hg_id_t read_op_id;
    hg_id_t clean_id;
    hg_id_t stat_id;
};

#ifdef __cplusplus
}
#endif

#endif
