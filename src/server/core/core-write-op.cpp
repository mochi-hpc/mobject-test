/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <map>
#include <cstring>
#include <string>
#include <iostream>
#include <limits>
#include <bake-client.h>
#include "src/server/visitor-args.h"
#include "src/io-chain/write-op-visitor.h"

#define ENTERING margo_trace(mid, "[mobject] Entering function %s", __func__);
#define LEAVING  margo_trace(mid, "[mobject] Leaving function %s", __func__);

static void write_op_exec_begin(void*);
static void write_op_exec_end(void*);
static void write_op_exec_create(void*, int);
static void write_op_exec_write(void*, buffer_u, size_t, uint64_t);
static void write_op_exec_write_full(void*, buffer_u, size_t);
static void write_op_exec_writesame(void*, buffer_u, size_t, size_t, uint64_t);
static void write_op_exec_append(void*, buffer_u, size_t);
static void write_op_exec_remove(void*);
static void write_op_exec_truncate(void*, uint64_t);
static void write_op_exec_zero(void*, uint64_t, uint64_t);
static void write_op_exec_omap_set(
    void*, char const* const*, char const* const*, const size_t*, size_t);
static void write_op_exec_omap_rm_keys(void*, char const* const*, size_t);

static oid_t get_or_create_oid(struct mobject_provider* provider,
                               yk_database_handle_t     oid_dbh,
                               yk_database_handle_t     name_dbh,
                               const char*              object_name);

static void insert_region_log_entry(struct mobject_provider*   provider,
                                    oid_t                      oid,
                                    uint64_t                   offset,
                                    uint64_t                   len,
                                    const region_descriptor_t* region,
                                    time_t                     ts = 0);

static void insert_small_region_log_entry(struct mobject_provider* provider,
                                          oid_t                    oid,
                                          uint64_t                 offset,
                                          uint64_t                 len,
                                          const char*              data,
                                          time_t                   ts = 0);

static void insert_zero_log_entry(struct mobject_provider* provider,
                                  oid_t                    oid,
                                  uint64_t                 offset,
                                  uint64_t                 len,
                                  time_t                   ts = 0);

static void insert_punch_log_entry(struct mobject_provider* provider,
                                   oid_t                    oid,
                                   uint64_t                 offset,
                                   time_t                   ts = 0);

uint64_t mobject_compute_object_size(struct mobject_provider* provider,
                                     yk_database_handle_t     seg_dbh,
                                     oid_t                    oid,
                                     time_t                   ts);

static struct write_op_visitor write_op_exec
    = {.visit_begin        = write_op_exec_begin,
       .visit_create       = write_op_exec_create,
       .visit_write        = write_op_exec_write,
       .visit_write_full   = write_op_exec_write_full,
       .visit_writesame    = write_op_exec_writesame,
       .visit_append       = write_op_exec_append,
       .visit_remove       = write_op_exec_remove,
       .visit_truncate     = write_op_exec_truncate,
       .visit_zero         = write_op_exec_zero,
       .visit_omap_set     = write_op_exec_omap_set,
       .visit_omap_rm_keys = write_op_exec_omap_rm_keys,
       .visit_end          = write_op_exec_end};

extern "C" void core_write_op(mobject_store_write_op_t write_op,
                              server_visitor_args_t    vargs)
{
    /* Execute the operation chain */
    execute_write_op_visitor(&write_op_exec, write_op, (void*)vargs);
}

void write_op_exec_begin(void* u)
{
    auto                 vargs    = static_cast<server_visitor_args_t>(u);
    yk_database_handle_t name_dbh = vargs->provider->name_dbh;
    yk_database_handle_t oid_dbh  = vargs->provider->oid_dbh;
    oid_t oid  = get_or_create_oid(vargs->provider, name_dbh, oid_dbh,
                                  vargs->object_name);
    vargs->oid = oid;
}

void write_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_create(void* u, int exclusive)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    oid_t             oid   = vargs->oid;
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
    }
    /* nothing to do, the object is actually created in write_op_exec_begin
       if it did not exist before */
    LEAVING;
}

void write_op_exec_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    oid_t oid = vargs->oid;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    struct mobject_provider* provider        = vargs->provider;
    unsigned                 bake_target_idx = oid % provider->num_bake_targets;
    bake_provider_handle_t bake_ph = provider->bake_targets[bake_target_idx].ph;
    region_descriptor_t    region
        = {provider->bake_targets[bake_target_idx].tid, 0};
    hg_bulk_t   remote_bulk     = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    double      wr_start, wr_end;

    int ret;

    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->stats_mutex));
    wr_start = ABT_get_wtime();
    if ((provider->last_wr_start > 0)
        && (provider->last_wr_start >= provider->last_wr_end)) {
        provider->total_seg_wr_duration += (wr_start - provider->last_wr_start);
    }
    provider->last_wr_start = wr_start;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->stats_mutex));

    if (len > SMALL_REGION_THRESHOLD) {
        ret = bake_create_write_persist_proxy(bake_ph, region.tid, remote_bulk, buf.as_offset,
                remote_addr_str, len, &region.rid);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: bake_create_write_persist_proxy returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        insert_region_log_entry(provider, oid, offset, len, &region);
    } else {
        margo_instance_id mid = vargs->provider->mid;
        char              data[SMALL_REGION_THRESHOLD];
        void*             buf_ptrs[1]  = {(void*)(&data[0])};
        hg_size_t         buf_sizes[1] = {len};
        hg_bulk_t         handle;
        ret = margo_bulk_create(mid, 1, buf_ptrs, buf_sizes, HG_BULK_WRITE_ONLY,
                                &handle);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_create returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        ret = margo_bulk_transfer(mid, HG_BULK_PULL, remote_addr, remote_bulk,
                                  buf.as_offset, handle, 0, len);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_transfer returned %d",
                        __func__, __LINE__, ret);
            margo_bulk_free(handle);
            LEAVING;
            return;
        }
        margo_bulk_free(handle);

        insert_small_region_log_entry(provider, oid, offset, len, data);
    }

    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->stats_mutex));
    wr_end = ABT_get_wtime();
    provider->segs++;
    provider->total_seg_size += len;
    if (provider->last_wr_start > provider->last_wr_end) {
        provider->total_seg_wr_duration += (wr_end - provider->last_wr_start);
    } else {
        provider->total_seg_wr_duration += (wr_end - provider->last_wr_end);
    }
    provider->last_wr_end = wr_end;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->stats_mutex));
    LEAVING;
}

void write_op_exec_write_full(void* u, buffer_u buf, size_t len)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    // truncate to 0 then write
    write_op_exec_truncate(u, 0);
    write_op_exec_write(u, buf, len, 0);
    LEAVING;
}

void write_op_exec_writesame(
    void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    oid_t oid = vargs->oid;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    hg_bulk_t   remote_bulk     = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int         ret;

    if (data_len > SMALL_REGION_THRESHOLD) {

        unsigned bake_target_idx = oid % vargs->provider->num_bake_targets;
        bake_provider_handle_t bake_ph
            = vargs->provider->bake_targets[bake_target_idx].ph;
        region_descriptor_t region
            = {vargs->provider->bake_targets[bake_target_idx].tid, 0};

        ret = bake_create(bake_ph, region.tid, data_len, &region.rid);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: bake_create returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        ret = bake_proxy_write(bake_ph, region.tid, region.rid, 0, remote_bulk,
                               buf.as_offset, remote_addr_str, data_len);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: bake_proxy_write returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        ret = bake_persist(bake_ph, region.tid, region.rid, 0, data_len);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: bake_persist returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }

        size_t i;

        // time_t ts = time(NULL);
        for (i = 0; i < write_len; i += data_len) {
            // TODO normally we should have the same timestamps but right now it
            // bugs...
            insert_region_log_entry(vargs->provider, oid, offset + i,
                                    std::min(data_len, write_len - i),
                                    &region); //, ts);
        }

    } else {

        margo_instance_id mid = vargs->provider->mid;
        char              data[SMALL_REGION_THRESHOLD];
        void*             buf_ptrs[1]  = {(void*)(&data[0])};
        hg_size_t         buf_sizes[1] = {data_len};
        hg_bulk_t         handle;
        ret = margo_bulk_create(mid, 1, buf_ptrs, buf_sizes, HG_BULK_WRITE_ONLY,
                                &handle);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_create returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        ret = margo_bulk_transfer(mid, HG_BULK_PULL, remote_addr, remote_bulk,
                                  buf.as_offset, handle, 0, data_len);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_transfer returned %d",
                        __func__, __LINE__, ret);
            margo_bulk_free(handle);
            LEAVING;
            return;
        }
        margo_bulk_free(handle);

        size_t i;
        for (i = 0; i < write_len; i += data_len) {
            insert_small_region_log_entry(vargs->provider, oid, offset + i,
                                          std::min(data_len, write_len - i),
                                          data);
        }
    }
    LEAVING;
}

void write_op_exec_append(void* u, buffer_u buf, size_t len)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    oid_t oid = vargs->oid;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    hg_bulk_t   remote_bulk     = vargs->bulk_handle;
    const char* remote_addr_str = vargs->client_addr_str;
    hg_addr_t   remote_addr     = vargs->client_addr;
    int         ret;

    // find out the current length of the object
    time_t   ts     = time(NULL);
    uint64_t offset = mobject_compute_object_size(
        vargs->provider, vargs->provider->segment_dbh, oid, ts);

    if (len > SMALL_REGION_THRESHOLD) {

        unsigned bake_target_idx = oid % vargs->provider->num_bake_targets;
        bake_provider_handle_t bake_ph
            = vargs->provider->bake_targets[bake_target_idx].ph;
        region_descriptor_t region
            = {vargs->provider->bake_targets[bake_target_idx].tid, 0};

        ret = bake_create_write_persist_proxy(bake_ph, region.tid, remote_bulk, buf.as_offset,
                remote_addr_str, len, &region.rid);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: bake_create_write_persisti_proxy returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }

        insert_region_log_entry(vargs->provider, oid, offset, len, &region, ts);

    } else {

        margo_instance_id mid = vargs->provider->mid;
        char              data[SMALL_REGION_THRESHOLD];
        void*             buf_ptrs[1]  = {(void*)(&data[0])};
        hg_size_t         buf_sizes[1] = {len};
        hg_bulk_t         handle;
        ret = margo_bulk_create(mid, 1, buf_ptrs, buf_sizes, HG_BULK_WRITE_ONLY,
                                &handle);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_create returned %d",
                        __func__, __LINE__, ret);
            LEAVING;
            return;
        }
        ret = margo_bulk_transfer(mid, HG_BULK_PULL, remote_addr, remote_bulk,
                                  buf.as_offset, handle, 0, len);
        if (ret != 0) {
            margo_error(mid, "[mobject] %s:%d: margo_bulk_transfer returned %d",
                        __func__, __LINE__, ret);
            margo_bulk_free(handle);
            LEAVING;
            return;
        }
        margo_bulk_free(handle);

        insert_small_region_log_entry(vargs->provider, oid, offset, len, data);
    }
    LEAVING;
}

void write_op_exec_remove(void* u)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    const char* object_name     = vargs->object_name;
    oid_t       oid             = vargs->oid;
    yk_database_handle_t name_dbh = vargs->provider->name_dbh;
    yk_database_handle_t oid_dbh  = vargs->provider->oid_dbh;
    yk_database_handle_t seg_dbh  = vargs->provider->segment_dbh;
    yk_return_t          yret;
    int                  bret;

    /* remove name->OID entry to make object no longer visible to clients */
    yret = yk_erase(name_dbh, YOKAN_MODE_DEFAULT, (const void*)object_name,
                    strlen(object_name) + 1);
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: sdskv_erase returned %d", __func__,
                    __LINE__, yret);
        LEAVING;
        return;
    }

    /* TODO bg thread for everything beyond this point */

    yret = yk_erase(oid_dbh, YOKAN_MODE_DEFAULT, &oid, sizeof(oid));
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: sdskv_erase returned %d", __func__,
                    __LINE__, yret);
        LEAVING;
        return;
    }

    segment_key_t lb;
    lb.oid       = oid;
    lb.timestamp = time(NULL);
    lb.seq_id    = MOBJECT_SEQ_ID_MAX;

    size_t              max_segments = 128; // XXX this is a pretty arbitrary number
    segment_key_t       segment_keys[max_segments];
    size_t              segment_keys_sizes[max_segments];
    region_descriptor_t segment_data[max_segments];
    size_t              segment_data_sizes[max_segments];

    /* iterate over and remove all segments for this oid */
    bool done = false;
    while (!done) {

        yret = yk_list_keyvals_packed(
            seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&lb,
            sizeof(lb),                              /* strict lower bound */
            (const void*)&oid, sizeof(oid),          /* prefix */
            max_segments,                            /* max key/val pairs */
            segment_keys,                            /* keys buffer */
            max_segments * sizeof(segment_key_t),    /* keys_buf_size */
            segment_keys_sizes,                      /* key sizes */
            segment_data,                            /* vals buffer */
            max_segments * sizeof(region_descriptor_t), /* vals_buf_size */
            segment_data_sizes);                     /* vals sizes */

        if (yret != YOKAN_SUCCESS) {
            margo_error(mid,
                        "[mobject] %s:%d: yk_list_keyvals_packed returned %d",
                        __func__, __LINE__, yret);
            LEAVING;
            return;
        }

        size_t i;
        for (i = 0; i < max_segments; ++i) {
            const segment_key_t&       seg    = segment_keys[i];
            const region_descriptor_t& region = segment_data[i];

            if (segment_keys_sizes[i] == YOKAN_NO_MORE_KEYS) {
                done = true;
                break;
            }

            if (seg.type == seg_type_t::BAKE_REGION) {
                // find the provider handle associated with the target
                bake_provider_handle_t bake_ph = BAKE_PROVIDER_HANDLE_NULL;
                for (unsigned j = 0; j < vargs->provider->num_bake_targets; j++) {
                    if (memcmp(&region.tid,
                                &vargs->provider->bake_targets[j].tid,
                                sizeof(bake_target_id_t))
                            == 0) {
                        bake_ph = vargs->provider->bake_targets[j].ph;
                    }
                }
                if (!bake_ph) {
                    margo_error(mid,
                                "[mobject] %s:%d: could not find bake provider "
                                "handle associated with stored target id",
                                __func__, __LINE__);
                    LEAVING;
                    return;
                }
                bret = bake_remove(bake_ph, region.tid, region.rid);
                if (yret != BAKE_SUCCESS) {
                    margo_error(mid, "[mobject] %s:%d: bake_remove returned %d",
                                __func__, __LINE__, bret);
                    /* XXX should save the error and keep removing */
                    LEAVING;
                    return;
                }
            }

            yret = yk_erase(seg_dbh, YOKAN_MODE_DEFAULT, &seg, sizeof(seg));
            if (yret != YOKAN_SUCCESS) {
                margo_error(mid, "[mobject] %s:%d: yk_erase returned %d",
                            __func__, __LINE__, yret);
                LEAVING;
                return;
            }
        }
    }

    LEAVING;
}

void write_op_exec_truncate(void* u, uint64_t offset)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    oid_t oid = vargs->oid;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    insert_punch_log_entry(vargs->provider, oid, offset);
    LEAVING;
}

void write_op_exec_zero(void* u, uint64_t offset, uint64_t len)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    oid_t oid = vargs->oid;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    insert_zero_log_entry(vargs->provider, oid, offset, len);
    LEAVING;
}

void write_op_exec_omap_set(void*              u,
                            char const* const* keys,
                            char const* const* vals,
                            const size_t*      lens,
                            size_t             num)
{
    yk_return_t          yret;
    auto                 vargs    = static_cast<server_visitor_args_t>(u);
    margo_instance_id    mid      = vargs->provider->mid;
    yk_database_handle_t omap_dbh = vargs->provider->omap_dbh;
    oid_t                oid      = vargs->oid;
    ENTERING;

    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    /* find out the max key length */
    size_t max_k_len = 0;
    for (auto i = 0; i < num; i++) {
        size_t s  = strlen(keys[i]);
        max_k_len = max_k_len < s ? s : max_k_len;
    }

    /* create an omap key of the right size */
    omap_key_t* k = (omap_key_t*)calloc(1, max_k_len + sizeof(omap_key_t));

    // TODO maybe use yk_put_multi/packed instead

    for (auto i = 0; i < num; i++) {
        size_t k_len = strlen(keys[i]) + sizeof(omap_key_t);
        memset(k, 0, max_k_len + sizeof(omap_key_t));
        k->oid = oid;
        strcpy(k->key, keys[i]);
        yret = yk_put(omap_dbh, YOKAN_MODE_DEFAULT, (const void*)k, k_len,
                      (const void*)vals[i], lens[i]);
        if (yret != YOKAN_SUCCESS) {
            margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                        __LINE__, yret);
        }
    }
    free(k);
    LEAVING;
}

void write_op_exec_omap_rm_keys(void*              u,
                                char const* const* keys,
                                size_t             num_keys)
{
    yk_return_t          yret;
    auto                 vargs    = static_cast<server_visitor_args_t>(u);
    margo_instance_id    mid      = vargs->provider->mid;
    yk_database_handle_t omap_dbh = vargs->provider->omap_dbh;
    oid_t                oid      = vargs->oid;
    ENTERING;
    if (oid == 0) {
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    size_t* key_sizes = (size_t*)calloc(num_keys, sizeof(size_t));
    for (size_t i = 0; i < num_keys; i++) {
        key_sizes[i] = strlen(keys[i]) + 1;
    }

    yret = yk_erase_multi(omap_dbh, YOKAN_MODE_DEFAULT, num_keys,
                          (const void* const*)keys, key_sizes);

    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_erase_multi returned %d",
                    __func__, __LINE__, yret);
    }
    LEAVING;
}

static oid_t get_or_create_oid(struct mobject_provider* provider,
                               yk_database_handle_t     name_dbh,
                               yk_database_handle_t     oid_dbh,
                               const char*              object_name)
{
    margo_instance_id mid = provider->mid;
    oid_t             oid = 0;
    size_t            s;
    yk_return_t       yret;
    ENTERING;

    s                       = sizeof(oid);
    size_t object_name_size = strlen(object_name) + 1;

    yret = yk_get(name_dbh, YOKAN_MODE_DEFAULT, (const void*)object_name,
                  object_name_size, &oid, &s);

    // oid found
    if (yret == YOKAN_SUCCESS) {
        LEAVING;
        return oid;
    }

    // error
    if (yret != YOKAN_ERR_KEY_NOT_FOUND) {
        margo_error(mid, "[mobject] %s:%d: yk_get returned %d", __func__,
                    __LINE__, yret);
        return 0;
    }

    // oid not found (yret == YOKAN_ERR_KEY_NOT_FOUND)

    std::hash<std::string> hash_fn;
    oid              = hash_fn(std::string(object_name));
    char* name_check = (char*)malloc(object_name_size);
    while (1) {
        /* avoid hash collisions by checking this oid mapping */
        s    = object_name_size;
        yret = yk_get(oid_dbh, YOKAN_MODE_DEFAULT, (const void*)&oid,
                      sizeof(oid), (void*)name_check, &s);

        if (yret == YOKAN_SUCCESS) {
            if (strncmp(object_name, name_check, s) == 0) {
                /* the object has been created by someone else in the meantime
                 */
                free(name_check);
                LEAVING;
                return oid;
            }
            oid++;
            continue;
        }
        break;
    }

    free(name_check);
    // we make sure we stopped at an unknown key (not another Yokan error)
    if (yret != YOKAN_ERR_KEY_NOT_FOUND) {
        margo_error(mid, "[mobject] %s:%d: yk_get returned %d", __func__,
                    __LINE__, yret);
        return 0;
    }
    // set name => oid
    yret = yk_put(name_dbh, YOKAN_MODE_DEFAULT, (const void*)object_name,
                  object_name_size, &oid, sizeof(oid));
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
        LEAVING;
        return 0;
    }
    // set oid => name
    yret = yk_put(oid_dbh, YOKAN_MODE_DEFAULT, &oid, sizeof(oid),
                  (const void*)object_name, object_name_size);
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
        LEAVING;
        return 0;
    }

    LEAVING;
    return oid;
}

static void insert_region_log_entry(struct mobject_provider*   provider,
                                    oid_t                      oid,
                                    uint64_t                   offset,
                                    uint64_t                   len,
                                    const region_descriptor_t* region,
                                    time_t                     ts)
{
    margo_instance_id mid = provider->mid;
    ENTERING;
    yk_database_handle_t seg_dbh = provider->segment_dbh;
    segment_key_t        seg;

    seg.oid         = oid;
    seg.timestamp   = ts == 0 ? time(NULL) : ts;
    seg.start_index = offset;
    seg.end_index   = offset + len;
    seg.type        = seg_type_t::BAKE_REGION;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));
    seg.seq_id = provider->seq_id++;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));

    yk_return_t yret
        = yk_put(seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&seg, sizeof(seg),
                 (const void*)region, sizeof(*region));
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
    }
    LEAVING;
}

static void insert_small_region_log_entry(struct mobject_provider* provider,
                                          oid_t                    oid,
                                          uint64_t                 offset,
                                          uint64_t                 len,
                                          const char*              data,
                                          time_t                   ts)
{
    margo_instance_id mid = provider->mid;
    ENTERING;
    yk_database_handle_t seg_dbh = provider->segment_dbh;
    segment_key_t        seg;

    seg.oid         = oid;
    seg.timestamp   = ts == 0 ? time(NULL) : ts;
    seg.start_index = offset;
    seg.end_index   = offset + len;
    seg.type        = seg_type_t::SMALL_REGION;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));
    seg.seq_id = provider->seq_id++;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));
    yk_return_t yret = yk_put(seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&seg,
                              sizeof(seg), (const void*)data, len);
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
    }
    LEAVING;
}

static void insert_zero_log_entry(struct mobject_provider* provider,
                                  oid_t                    oid,
                                  uint64_t                 offset,
                                  uint64_t                 len,
                                  time_t                   ts)
{
    margo_instance_id mid = provider->mid;
    ENTERING;
    yk_database_handle_t seg_dbh = provider->segment_dbh;
    segment_key_t        seg;

    seg.oid         = oid;
    seg.timestamp   = ts == 0 ? time(NULL) : ts;
    seg.start_index = offset;
    seg.end_index   = offset + len;
    seg.type        = seg_type_t::ZERO;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));
    seg.seq_id = provider->seq_id++;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));

    yk_return_t yret = yk_put(seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&seg,
                              sizeof(seg), (const void*)nullptr, 0);
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
    }
    LEAVING;
}

static void insert_punch_log_entry(struct mobject_provider* provider,
                                   oid_t                    oid,
                                   uint64_t                 offset,
                                   time_t                   ts)
{
    margo_instance_id mid = provider->mid;
    ENTERING;
    yk_database_handle_t seg_dbh = provider->segment_dbh;
    segment_key_t        seg;

    seg.oid         = oid;
    seg.timestamp   = ts == 0 ? time(NULL) : ts;
    seg.start_index = offset;
    seg.end_index   = std::numeric_limits<uint64_t>::max();
    seg.type        = seg_type_t::TOMBSTONE;
    ABT_mutex_lock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));
    seg.seq_id = provider->seq_id++;
    ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&provider->mutex));

    yk_return_t yret = yk_put(seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&seg,
                              sizeof(seg), (const void*)nullptr, 0);
    if (yret != YOKAN_SUCCESS) {
        margo_error(mid, "[mobject] %s:%d: yk_put returned %d", __func__,
                    __LINE__, yret);
    }
    LEAVING;
}

uint64_t mobject_compute_object_size(struct mobject_provider* provider,
                                     yk_database_handle_t     seg_dbh,
                                     oid_t                    oid,
                                     time_t                   ts)
{
    margo_instance_id mid = provider->mid;
    ENTERING;
    segment_key_t lb;
    lb.oid       = oid;
    lb.timestamp = ts;
    lb.seq_id    = MOBJECT_SEQ_ID_MAX;

    uint64_t size     = 0; // current assumed size
    uint64_t max_size = std::numeric_limits<uint64_t>::max();

    size_t         max_segments = 128;
    segment_key_t* segment_keys
        = (segment_key_t*)calloc(max_segments, sizeof(segment_key_t));
    size_t* segment_keys_size = (size_t*)calloc(max_segments, sizeof(size_t));

    bool done          = false;
    int  seg_start_ndx = 0;
    while (!done) {
        yk_return_t yret = yk_list_keys_packed(
            seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&lb,
            sizeof(lb),                           /* strict lower bound */
            (const void*)&oid, sizeof(oid),       /* prefix */
            max_segments,                         /* count */
            segment_keys,                         /* keys */
            max_segments * sizeof(segment_key_t), /* buffer size */
            segment_keys_size);                   /* key sizes */

        if (yret != YOKAN_SUCCESS) {
            margo_error(mid, "[mobject] %s:%d: yk_list_keys_packed returned %d",
                        __func__, __LINE__, yret);
            free(segment_keys);
            free(segment_keys_size);
            LEAVING;
            return 0;
        }

        for (size_t i = 0; i < max_segments; i++) {
            if (segment_keys_size[i] == YOKAN_NO_MORE_KEYS) {
                done = true;
                break;
            }
            auto& seg = segment_keys[i];
            if (seg.type < seg_type_t::TOMBSTONE) {
                if (size < seg.end_index) {
                    size = std::min(seg.end_index, max_size);
                }
            } else if (seg.type == seg_type_t::TOMBSTONE) {
                if (max_size > seg.start_index) { max_size = seg.start_index; }
                if (size < seg.start_index) { size = seg.start_index; }
                done = true;
                break;
            }
            lb.timestamp = seg.timestamp;
            lb.seq_id    = seg.seq_id;
        }
    }

    free(segment_keys);
    free(segment_keys_size);
    LEAVING;
    return size;
}
