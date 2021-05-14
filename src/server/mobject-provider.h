/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SERVER_MOBJECT_PROVIDER_H
#define __SERVER_MOBJECT_PROVIDER_H

#include <margo.h>
#include <bake-client.h>
#include <sdskv-client.h>
#include <ssg.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOBJECT_SEQ_ID_MAX UINT32_MAX

struct mobject_provider {
    /* margo/ABT state */
    margo_instance_id mid;
    uint16_t          provider_id;
    ABT_pool          pool;
    ABT_mutex         mutex;
    ABT_mutex         stats_mutex;
    /* ssg-related data */
    ssg_group_id_t gid;
    /* bake-related data */
    bake_provider_handle_t bake_ph;
    bake_target_id_t       bake_tid;
    /* sdskv-related data */
    sdskv_provider_handle_t sdskv_ph;
    sdskv_database_id_t     oid_db_id;
    sdskv_database_id_t     name_db_id;
    sdskv_database_id_t     segment_db_id;
    sdskv_database_id_t     omap_db_id;
    /* other data */
    uint32_t seq_id;
    int      ref_count;
    /* stats/counters/timers and helpers */
    uint32_t segs;
    uint64_t total_seg_size;
    double   total_seg_wr_duration;
    double   last_wr_start;
    double   last_wr_end;
};

#ifdef __cplusplus
}
#endif

#endif
