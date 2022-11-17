/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <map>
#include <string>
#include <iostream>
#include <algorithm>
#include <vector>
#include <list>
#include <cinttypes>
#include <bake-client.h>
#include "src/server/core/core-read-op.h"
#include "src/server/visitor-args.h"
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/server/core/key-types.h"
#include "src/server/core/covermap.hpp"

#define ENTERING margo_trace(mid, "[mobject] Entering function %s", __func__);
#define LEAVING  margo_trace(mid, "[mobject] Leaving function %s", __func__);

static void read_op_exec_begin(void*);
static void read_op_exec_stat(void*, uint64_t*, time_t*, int*);
static void read_op_exec_read(void*, uint64_t, size_t, buffer_u, size_t*, int*);
static void read_op_exec_omap_get_keys(
    void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_omap_get_vals(void*,
                                       const char*,
                                       const char*,
                                       uint64_t,
                                       mobject_store_omap_iter_t*,
                                       int*);
static void read_op_exec_omap_get_vals_by_keys(
    void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
static void read_op_exec_end(void*);

/* defined in core-write-op.cpp */
extern uint64_t mobject_compute_object_size(struct mobject_provider* provider,
                                            yk_database_handle_t     seg_dbh,
                                            oid_t                    oid,
                                            time_t                   ts);

static oid_t get_oid_from_name(margo_instance_id    mid,
                               yk_database_handle_t name_dbh,
                               const char*          name);

static struct read_op_visitor read_op_exec
    = {.visit_begin                 = read_op_exec_begin,
       .visit_stat                  = read_op_exec_stat,
       .visit_read                  = read_op_exec_read,
       .visit_omap_get_keys         = read_op_exec_omap_get_keys,
       .visit_omap_get_vals         = read_op_exec_omap_get_vals,
       .visit_omap_get_vals_by_keys = read_op_exec_omap_get_vals_by_keys,
       .visit_end                   = read_op_exec_end};

extern "C" void core_read_op(mobject_store_read_op_t read_op,
                             server_visitor_args_t   vargs)
{
    execute_read_op_visitor(&read_op_exec, read_op, (void*)vargs);
}

void read_op_exec_begin(void* u)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    // find oid
    const char* object_name = vargs->object_name;
    oid_t       oid         = vargs->oid;
    if (oid == 0) {
        yk_database_handle_t name_dbh = vargs->provider->name_dbh;
        oid        = get_oid_from_name(mid, name_dbh, object_name);
        vargs->oid = oid;
    }
    LEAVING
}

void read_op_exec_stat(void* u, uint64_t* psize, time_t* pmtime, int* prval)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    yk_database_handle_t seg_dbh = vargs->provider->segment_dbh;
    // find oid
    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        LEAVING;
        return;
    }

    time_t ts = time(NULL);
    *psize    = mobject_compute_object_size(vargs->provider, seg_dbh, oid, ts);

    LEAVING;
}

void read_op_exec_read(void*    u,
                       uint64_t offset,
                       size_t   len,
                       buffer_u buf,
                       size_t*  bytes_read,
                       int*     prval)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    bake_provider_handle_t bph = vargs->provider->bake_ph;
    bake_target_id_t       bti = vargs->provider->bake_tid;
    bake_region_id_t       rid;
    hg_bulk_t              remote_bulk     = vargs->bulk_handle;
    const char*            remote_addr_str = vargs->client_addr_str;
    hg_addr_t              remote_addr     = vargs->client_addr;
    yk_database_handle_t   seg_dbh         = vargs->provider->segment_dbh;
    yk_return_t            yret;

    uint64_t client_start_index = offset;
    uint64_t client_end_index   = offset + len;

    *prval = 0;

    // find oid
    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    segment_key_t lb;
    lb.oid       = oid;
    lb.timestamp = time(NULL);
    lb.seq_id    = MOBJECT_SEQ_ID_MAX;

    covermap<uint64_t> coverage(offset, offset + len);

    size_t        max_segments = 128; // XXX this is a pretty arbitrary number
    segment_key_t segment_keys[max_segments];
    hg_size_t     segment_keys_size[max_segments];
    bake_region_id_t segment_data[max_segments];
    hg_size_t        segment_data_size[max_segments];

    bool done          = false;
    int  seg_start_ndx = 0;

    while (!coverage.full() && !done) {

        yret = yk_list_keyvals_packed(
            seg_dbh, YOKAN_MODE_DEFAULT, (const void*)&lb,
            sizeof(lb),                              /* strict lower bound */
            (const void*)&oid, sizeof(oid),          /* prefix */
            max_segments,                            /* count */
            segment_keys,                            /* keys buffer */
            max_segments * sizeof(segment_key_t),    /* keys buffer size */
            segment_keys_size,                       /* key sizes */
            segment_data,                            /* data buffer */
            max_segments * sizeof(bake_region_id_t), /* data buffer size */
            segment_data_size);                      /* data sizes */

        if (yret != YOKAN_SUCCESS) {
            margo_error(mid,
                        "[mobject] %s:%d: yk_list_keyvals_packed returned %d",
                        __func__, __LINE__, yret);
            *prval = -1;
            LEAVING;
            return;
        }

        size_t i;
        for (i = seg_start_ndx; i < max_segments; i++) {

            const segment_key_t&    seg    = segment_keys[i];
            const bake_region_id_t& region = segment_data[i];

            if (segment_keys_size[i] == YOKAN_NO_MORE_KEYS || seg.oid != oid
                || coverage.full()) {
                done = true;
                break;
            }

            switch (seg.type) {

            case seg_type_t::ZERO:
                coverage.set(seg.start_index, seg.end_index);
                break;

            case seg_type_t::TOMBSTONE:
                coverage.set(seg.start_index, seg.end_index);
                break;

            case seg_type_t::BAKE_REGION: {
                auto ranges = coverage.set(seg.start_index, seg.end_index);
                for (auto r : ranges) {
                    uint64_t segment_size  = r.end - r.start;
                    uint64_t region_offset = r.start - seg.start_index;
                    uint64_t remote_offset = r.start - offset;
                    uint64_t bytes_read    = 0;
                    int bret = bake_proxy_read(bph, bti, region, region_offset,
                                               remote_bulk, remote_offset,
                                               remote_addr_str, segment_size,
                                               &bytes_read);
                    if (bret != 0) {
                        *prval = -1;
                        margo_error(
                            mid, "[mobject] %s:%d: bake_proxy_read returned %d",
                            __func__, __LINE__, bret);
                        LEAVING;
                        return;
                    } else if (bytes_read != segment_size) {
                        *prval = -1;
                        margo_error(mid,
                                    "bake_proxy_read invalid read of %" PRIu64
                                    " (requested=%" PRIu64 ")",
                                    bytes_read, segment_size);
                        margo_error(
                            mid,
                            "[mobject] %s:%d: bake_proxy_read invalid read of "
                            "%" PRIu64 " (requested %" PRIu64 ")",
                            __func__, __LINE__, bytes_read, segment_size);
                        LEAVING;
                        return;
                    }
                }
                break;
            } // end case seg_type_t::BAKE_REGION

            case seg_type_t::SMALL_REGION: {
                auto ranges      = coverage.set(seg.start_index, seg.end_index);
                const char* base = static_cast<const char*>((void*)(&region));
                margo_instance_id mid = vargs->provider->mid;
                for (auto r : ranges) {
                    uint64_t segment_size  = r.end - r.start;
                    uint64_t region_offset = r.start - seg.start_index;
                    uint64_t remote_offset = r.start - offset;
                    void*    buf_ptrs[1]
                        = {const_cast<char*>(base + region_offset)};
                    hg_size_t buf_sizes[1] = {segment_size};
                    hg_bulk_t handle;
                    int ret = margo_bulk_create(mid, 1, buf_ptrs, buf_sizes,
                                                HG_BULK_READ_ONLY, &handle);
                    if (ret != HG_SUCCESS) {
                        margo_error(
                            mid,
                            "[mobject] %s:%d: margo_bulk_create returned %d",
                            __func__, __LINE__, ret);
                        LEAVING;
                        *prval = -1;
                        return;
                    } // end if
                    ret = margo_bulk_transfer(
                        mid, HG_BULK_PUSH, remote_addr, remote_bulk,
                        buf.as_offset + remote_offset, handle, 0, segment_size);
                    if (ret != HG_SUCCESS) {
                        margo_error(
                            mid,
                            "[mobject] %s:%d: margo_bulk_transfer returned %d",
                            __func__, __LINE__, ret);
                        *prval = -1;
                        LEAVING;
                        return;
                    } // end if
                    ret = margo_bulk_free(handle);
                    if (ret != HG_SUCCESS) {
                        margo_error(
                            mid, "[mobject] %s:%d: margo_bulk_free returned %d",
                            __func__, __LINE__, ret);
                        *prval = -1;
                        LEAVING;
                        return;
                    } // end if
                }     // end for
            }         // end case seg_type_t::SMALL_REGION

            } // end switch
            // update the start key timestamp to that of the last processed
            // segment
            lb.timestamp = seg.timestamp;
            lb.seq_id    = seg.seq_id;
        } // end for
    }
    *bytes_read = coverage.bytes_read();
    LEAVING;
}

void read_op_exec_omap_get_keys(void*                      u,
                                const char*                start_after,
                                uint64_t                   max_return,
                                mobject_store_omap_iter_t* iter,
                                int*                       prval)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    const char*          object_name = vargs->object_name;
    yk_database_handle_t omap_dbh    = vargs->provider->omap_dbh;
    yk_return_t          yret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    omap_iter_create(iter);
    size_t      lb_size = sizeof(omap_key_t) + MAX_OMAP_KEY_SIZE;
    omap_key_t* lb      = (omap_key_t*)calloc(1, lb_size);
    lb->oid             = oid;
    strcpy(lb->key, start_after);

    hg_size_t max_keys = 10;
    hg_size_t key_len  = MAX_OMAP_KEY_SIZE + sizeof(omap_key_t);
    // std::vector<void*>     keys(max_keys);
    std::vector<hg_size_t> ksizes(max_keys, key_len);
    // std::vector<std::vector<char>> buffers(max_keys,
    //                                       std::vector<char>(key_len));
    // for (auto i = 0; i < max_keys; i++) keys[i] = (void*)buffers[i].data();
    std::vector<char> keys(max_keys * key_len);

    hg_size_t keys_retrieved = max_keys;
    hg_size_t count          = 0;
    do {
        yret
            = yk_list_keys_packed(omap_dbh, YOKAN_MODE_DEFAULT, (const void*)lb,
                                  lb_size, /* strict lower bound */
                                  (const void*)&oid, sizeof(oid), /* prefix */
                                  max_keys,                       /* count */
                                  keys.data(),    /* keys buffer */
                                  keys.size(),    /* buffer size */
                                  ksizes.data()); /* key sizes */
        if (yret != YOKAN_SUCCESS) {
            *prval = -1;
            margo_error(mid, "[mobject] %s:%d: yk_list_keys_packed returned %d",
                        __func__, __LINE__, yret);
            break;
        }
        const char* k          = NULL;
        keys_retrieved         = 0;
        size_t keys_buf_offset = 0;
        for (auto i = 0; i < max_keys && count < max_return;
             i++, count++, keys_retrieved++) {
            if (ksizes[i] == YOKAN_NO_MORE_KEYS) break;
            // extract the actual key part, without the oid
            k = ((omap_key_t*)(keys.data() + keys_buf_offset))->key;
            omap_iter_append(*iter, k, nullptr, 0);
            keys_buf_offset += ksizes[i];
        }
        if (k != NULL) {
            strcpy(lb->key, k);
            lb_size = strlen(k) + sizeof(omap_key_t);
        }
    } while (keys_retrieved == max_keys && count < max_return);

out:
    free(lb);
    LEAVING;
}

void read_op_exec_omap_get_vals(void*                      u,
                                const char*                start_after,
                                const char*                filter_prefix,
                                uint64_t                   max_return,
                                mobject_store_omap_iter_t* iter,
                                int*                       prval)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    const char*          object_name = vargs->object_name;
    yk_database_handle_t omap_dbh    = vargs->provider->omap_dbh;
    yk_return_t          yret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    hg_size_t max_items = std::min(max_return, (decltype(max_return))10);
    hg_size_t key_len   = MAX_OMAP_KEY_SIZE + sizeof(omap_key_t);
    hg_size_t val_len   = MAX_OMAP_VAL_SIZE;

    omap_iter_create(iter);

    /* omap_key_t equivalent of start_key */
    hg_size_t   lb_size = key_len;
    omap_key_t* lb      = (omap_key_t*)calloc(1, lb_size);
    lb->oid             = oid;
    strcpy(lb->key, start_after);

    /* omap_key_t equivalent of the filter_prefix */
    hg_size_t   prefix_size = sizeof(omap_key_t) + strlen(filter_prefix);
    omap_key_t* prefix      = (omap_key_t*)calloc(1, prefix_size);
    prefix->oid             = oid;
    strcpy(prefix->key, filter_prefix);
    hg_size_t prefix_actual_size
        = offsetof(omap_key_t, key) + strlen(filter_prefix);
    /* we need the above because the prefix in sdskv is not considered a string
     */

    /* initialize structures to pass to SDSKV functions */
    std::vector<void*>             keys(max_items);
    std::vector<void*>             vals(max_items);
    std::vector<hg_size_t>         ksizes(max_items, key_len);
    std::vector<hg_size_t>         vsizes(max_items, val_len);
    std::vector<std::vector<char>> key_buffers(max_items,
                                               std::vector<char>(key_len));
    std::vector<std::vector<char>> val_buffers(max_items,
                                               std::vector<char>(val_len));
    for (auto i = 0; i < max_items; i++) {
        keys[i] = (void*)key_buffers[i].data();
        vals[i] = (void*)val_buffers[i].data();
    }

    size_t items_retrieved = 0;
    size_t count           = 0;
    do {
        yret = yk_list_keyvals(omap_dbh, YOKAN_MODE_DEFAULT, (const void*)lb,
                               lb_size, /* strict lower bound */
                               (const void*)prefix,
                               prefix_actual_size,          /* prefix */
                               max_items,                   /* count */
                               keys.data(), ksizes.data(),  /* keys */
                               vals.data(), vsizes.data()); /* values */

        if (yret != YOKAN_SUCCESS) {
            *prval = -1;
            margo_error(mid, "[mobject] %s:%d: yk_list_keyvals returned %d",
                        __func__, __LINE__, yret);
            break;
        }

        for (items_retrieved = 0; items_retrieved < max_items;
             items_retrieved++) {
            if (ksizes[items_retrieved] == YOKAN_NO_MORE_KEYS) break;
        }

        const char* k;
        for (auto i = 0; i < items_retrieved && count < max_return;
             i++, count++) {
            // extract the actual key part, without the oid
            k = ((omap_key_t*)keys[i])->key;
            /* this key is not part of the same object, we should leave the loop
             */
            if (((omap_key_t*)keys[i])->oid != oid)
                goto out; /* ugly way of leaving the loop, I know ... */

            omap_iter_append(*iter, k, (const char*)vals[i], vsizes[i]);
        }
        memset(lb, 0, lb_size);
        lb->oid = oid;
        strcpy(lb->key, k);

    } while (items_retrieved == max_items && count < max_return);

out:
    free(lb);
    LEAVING;
}

void read_op_exec_omap_get_vals_by_keys(void*                      u,
                                        char const* const*         keys,
                                        size_t                     num_keys,
                                        mobject_store_omap_iter_t* iter,
                                        int*                       prval)
{
    auto              vargs = static_cast<server_visitor_args_t>(u);
    margo_instance_id mid   = vargs->provider->mid;
    ENTERING;
    const char*          object_name = vargs->object_name;
    yk_database_handle_t omap_dbh    = vargs->provider->omap_dbh;
    yk_return_t          yret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        margo_error(mid, "[mobject] %s:%d: oid == 0", __func__, __LINE__);
        LEAVING;
        return;
    }

    omap_iter_create(iter);

    // figure out key sizes
    std::vector<size_t> ksizes(num_keys);
    size_t              max_ksize = 0;
    for (auto i = 0; i < num_keys; i++) {
        size_t s = offsetof(omap_key_t, key) + strlen(keys[i]) + 1;
        if (s > max_ksize) max_ksize = s;
        ksizes[i] = s;
    }
    max_ksize += sizeof(omap_key_t);

    // TODO use length_mutli and get_multi or even get_packed
    // with a large enough buffer

    omap_key_t* key = (omap_key_t*)calloc(1, max_ksize);
    for (size_t i = 0; i < num_keys; i++) {
        memset(key, 0, max_ksize);
        key->oid = oid;
        strcpy(key->key, keys[i]);
        // get length of the value
        hg_size_t vsize;
        yret = yk_length(omap_dbh, YOKAN_MODE_DEFAULT, (const void*)key,
                         ksizes[i], &vsize);
        if (yret != YOKAN_SUCCESS) {
            *prval = -1;
            margo_error(mid, "[mobject] %s:%d: yk_length returned %d", __func__,
                        __LINE__, yret);
            break;
        }
        std::vector<char> value(vsize);
        yret = yk_get(omap_dbh, YOKAN_MODE_DEFAULT, (const void*)key, ksizes[i],
                      (void*)value.data(), &vsize);
        if (yret != YOKAN_SUCCESS) {
            *prval = -1;
            margo_error(mid, "[mobject] %s:%d: sdskv_get returned %d", __func__,
                        __LINE__, yret);
            break;
        }
        omap_iter_append(*iter, keys[i], value.data(), vsize);
    }
    LEAVING;
}

void read_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

static oid_t get_oid_from_name(margo_instance_id    mid,
                               yk_database_handle_t name_dbh,
                               const char*          name)
{
    ENTERING;
    oid_t       result   = 0;
    size_t      oid_size = sizeof(result);
    yk_return_t yret = yk_get(name_dbh, YOKAN_MODE_DEFAULT, (const void*)name,
                              strlen(name) + 1, (void*)&result, &oid_size);
    if (yret != YOKAN_SUCCESS) result = 0;
    LEAVING;
    return result;
}
