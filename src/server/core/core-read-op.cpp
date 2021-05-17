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

static int tabs = 0;
/*
#define ENTERING {for(int i=0; i<tabs; i++) fprintf(stderr," ");
fprintf(stderr,"[ENTERING]>> %s\n",__FUNCTION__); tabs += 1;} #define LEAVING
{tabs -= 1; for(int i=0; i<tabs; i++) fprintf(stderr," ");
fprintf(stderr,"[LEAVING]<<< %s\n",__FUNCTION__); } #define ERROR    {for(int
i=0; i<(tabs+1); i++) fprintf(stderr, " "); fprintf(stderr,"[ERROR] "); }
*/
#define ENTERING
#define LEAVING
#define ERROR

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
                                            sdskv_provider_handle_t  ph,
                                            sdskv_database_id_t      seg_db_id,
                                            oid_t                    oid,
                                            time_t                   ts);

static oid_t get_oid_from_name(sdskv_provider_handle_t ph,
                               sdskv_database_id_t     name_db_id,
                               const char*             name);

#if 0
struct read_request_t {
    double timestamp;              // timestamp at which the segment was created
    uint64_t absolute_start_index; // start index within the object
    uint64_t absolute_end_index;   // end index within the object
    uint64_t region_start_index;   // where to start within the region
    uint64_t region_end_index;     // where to end within the region
    uint64_t client_offset;        // offset within the client's buffer
    bake_region_id_t region;  // region id
};
#endif

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
    ENTERING;
    auto vargs = static_cast<server_visitor_args_t>(u);
    // find oid
    const char* object_name = vargs->object_name;
    oid_t       oid         = vargs->oid;
    if (oid == 0) {
        sdskv_provider_handle_t sdskv_ph   = vargs->provider->sdskv_ph;
        sdskv_database_id_t     name_db_id = vargs->provider->name_db_id;
        oid        = get_oid_from_name(sdskv_ph, name_db_id, object_name);
        vargs->oid = oid;
    }
    LEAVING
}

void read_op_exec_stat(void* u, uint64_t* psize, time_t* pmtime, int* prval)
{
    ENTERING;
    auto                    vargs     = static_cast<server_visitor_args_t>(u);
    sdskv_provider_handle_t sdskv_ph  = vargs->provider->sdskv_ph;
    sdskv_database_id_t     seg_db_id = vargs->provider->segment_db_id;
    // find oid
    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        LEAVING;
        return;
    }

    time_t ts = time(NULL);
    *psize = mobject_compute_object_size(vargs->provider, sdskv_ph, seg_db_id,
                                         oid, ts);

    LEAVING;
}

void read_op_exec_read(void*    u,
                       uint64_t offset,
                       size_t   len,
                       buffer_u buf,
                       size_t*  bytes_read,
                       int*     prval)
{
    ENTERING;
    auto                    vargs = static_cast<server_visitor_args_t>(u);
    bake_provider_handle_t  bph   = vargs->provider->bake_ph;
    bake_target_id_t        bti   = vargs->provider->bake_tid;
    bake_region_id_t        rid;
    hg_bulk_t               remote_bulk     = vargs->bulk_handle;
    const char*             remote_addr_str = vargs->client_addr_str;
    hg_addr_t               remote_addr     = vargs->client_addr;
    sdskv_provider_handle_t sdskv_ph        = vargs->provider->sdskv_ph;
    sdskv_database_id_t     seg_db_id       = vargs->provider->segment_db_id;
    int                     ret;

    uint64_t client_start_index = offset;
    uint64_t client_end_index   = offset + len;

    *prval = 0;

    // find oid
    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        ERROR fprintf(stderr, "oid == 0\n");
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
    void*         segment_keys_addrs[max_segments];
    hg_size_t     segment_keys_size[max_segments];
    bake_region_id_t segment_data[max_segments];
    void*            segment_data_addrs[max_segments];
    hg_size_t        segment_data_size[max_segments];
    for (auto i = 0; i < max_segments; i++) {
        segment_keys_addrs[i] = (void*)(&segment_keys[i]);
        segment_keys_size[i]  = sizeof(segment_key_t);
        segment_data_addrs[i] = (void*)(&segment_data[i]);
        segment_data_size[i]  = sizeof(bake_region_id_t);
    }

    bool done          = false;
    int  seg_start_ndx = 0;
    while (!coverage.full() && !done) {

        // get the next max_segments segments
        size_t num_segments = max_segments;
        ret = sdskv_list_keyvals(sdskv_ph, seg_db_id, (const void*)&lb,
                                 sizeof(lb), segment_keys_addrs,
                                 segment_keys_size, segment_data_addrs,
                                 segment_data_size, &num_segments);

        if (ret != SDSKV_SUCCESS) {
            ERROR fprintf(stderr, "sdskv_list_keyvals returned %d\n", ret);
            *prval = -1;
            LEAVING;
            return;
        }

        size_t i;
        for (i = seg_start_ndx; i < num_segments; i++) {

            const segment_key_t&    seg    = segment_keys[i];
            const bake_region_id_t& region = segment_data[i];

            if (seg.oid != oid || coverage.full()) {
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
                    ret = bake_proxy_read(bph, bti, region, region_offset,
                                          remote_bulk, remote_offset,
                                          remote_addr_str, segment_size,
                                          &bytes_read);
                    if (ret != 0) {
                        *prval = -1;
                        ERROR fprintf(stderr, "bake_proxy_read returned %d\n",
                                      ret);
                        LEAVING;
                        return;
                    } else if (bytes_read != segment_size) {
                        *prval = -1;
                        ERROR fprintf(stderr,
                                      "bake_proxy_read invalid read of %" PRIu64
                                      " (requested=%" PRIu64 ")\n",
                                      bytes_read, segment_size);
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
                    ret = margo_bulk_create(mid, 1, buf_ptrs, buf_sizes,
                                            HG_BULK_READ_ONLY, &handle);
                    if (ret != HG_SUCCESS) {
                        ERROR fprintf(stderr, "margo_bulk_create returned %d\n",
                                      ret);
                        LEAVING;
                        *prval = -1;
                        return;
                    } // end if
                    ret = margo_bulk_transfer(
                        mid, HG_BULK_PUSH, remote_addr, remote_bulk,
                        buf.as_offset + remote_offset, handle, 0, segment_size);
                    if (ret != HG_SUCCESS) {
                        ERROR fprintf(stderr,
                                      "margo_bulk_transfer returned %d\n", ret);
                        *prval = -1;
                        LEAVING;
                        return;
                    } // end if
                    ret = margo_bulk_free(handle);
                    if (ret != HG_SUCCESS) {
                        ERROR fprintf(stderr, "margo_bulk_free returned %d\n",
                                      ret);
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

        seg_start_ndx = 1;
        if (num_segments != max_segments) done = true;
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
    ENTERING;
    auto                    vargs       = static_cast<server_visitor_args_t>(u);
    const char*             object_name = vargs->object_name;
    sdskv_provider_handle_t sdskv_ph    = vargs->provider->sdskv_ph;
    sdskv_database_id_t     omap_db_id  = vargs->provider->omap_db_id;
    int                     ret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        ERROR fprintf(stderr, "oid == 0\n");
        LEAVING;
        return;
    }

    omap_iter_create(iter);
    size_t      lb_size = sizeof(omap_key_t) + MAX_OMAP_KEY_SIZE;
    omap_key_t* lb      = (omap_key_t*)calloc(1, lb_size);
    lb->oid             = oid;
    strcpy(lb->key, start_after);

    hg_size_t              max_keys = 10;
    hg_size_t              key_len  = MAX_OMAP_KEY_SIZE + sizeof(omap_key_t);
    std::vector<void*>     keys(max_keys);
    std::vector<hg_size_t> ksizes(max_keys, key_len);
    std::vector<std::vector<char>> buffers(max_keys,
                                           std::vector<char>(key_len));
    for (auto i = 0; i < max_keys; i++) keys[i] = (void*)buffers[i].data();

    hg_size_t keys_retrieved = max_keys;
    hg_size_t count          = 0;
    do {
        ret = sdskv_list_keys(sdskv_ph, omap_db_id, (const void*)lb, lb_size,
                              keys.data(), ksizes.data(), &keys_retrieved);
        if (ret != SDSKV_SUCCESS) {
            *prval = -1;
            ERROR fprintf(stderr, "sdskv_list_keys returned %d\n", ret);
            break;
        }
        const char* k = NULL;
        for (auto i = 0; i < keys_retrieved && count < max_return;
             i++, count++) {
            // extract the actual key part, without the oid
            k = ((omap_key_t*)keys[i])->key;
            /* this key is not part of the same object, we should leave the loop
             */
            if (((omap_key_t*)keys[i])->oid != oid)
                goto out; /* ugly way of leaving the loop, I know ... */
            omap_iter_append(*iter, k, nullptr, 0);
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
    ENTERING;
    auto                    vargs       = static_cast<server_visitor_args_t>(u);
    const char*             object_name = vargs->object_name;
    sdskv_provider_handle_t sdskv_ph    = vargs->provider->sdskv_ph;
    sdskv_database_id_t     omap_db_id  = vargs->provider->omap_db_id;
    int                     ret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        ERROR fprintf(stderr, "oid == 0\n");
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

    hg_size_t items_retrieved = max_items;
    hg_size_t count           = 0;
    do {
        ret = sdskv_list_keyvals_with_prefix(
            sdskv_ph, omap_db_id, (const void*)lb, lb_size, (const void*)prefix,
            prefix_actual_size, keys.data(), ksizes.data(), vals.data(),
            vsizes.data(), &items_retrieved);
        if (ret != SDSKV_SUCCESS) {
            *prval = -1;
            ERROR fprintf(stderr,
                          "sdskv_list_keyvals_with_prefix returned %d\n", ret);
            break;
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
    ENTERING;
    auto                    vargs       = static_cast<server_visitor_args_t>(u);
    const char*             object_name = vargs->object_name;
    sdskv_provider_handle_t sdskv_ph    = vargs->provider->sdskv_ph;
    sdskv_database_id_t     omap_db_id  = vargs->provider->omap_db_id;
    int                     ret;
    *prval = 0;

    oid_t oid = vargs->oid;
    if (oid == 0) {
        *prval = -1;
        ERROR fprintf(stderr, "oid == 0\n");
        LEAVING;
        return;
    }

    omap_iter_create(iter);

    // figure out key sizes
    std::vector<hg_size_t> ksizes(num_keys);
    hg_size_t              max_ksize = 0;
    for (auto i = 0; i < num_keys; i++) {
        hg_size_t s = offsetof(omap_key_t, key) + strlen(keys[i]) + 1;
        if (s > max_ksize) max_ksize = s;
        ksizes[i] = s;
    }
    max_ksize += sizeof(omap_key_t);

    omap_key_t* key = (omap_key_t*)calloc(1, max_ksize);
    for (size_t i = 0; i < num_keys; i++) {
        memset(key, 0, max_ksize);
        key->oid = oid;
        strcpy(key->key, keys[i]);
        // get length of the value
        hg_size_t vsize;
        ret = sdskv_length(sdskv_ph, omap_db_id, (const void*)key, ksizes[i],
                           &vsize);
        if (ret != SDSKV_SUCCESS) {
            *prval = -1;
            ERROR fprintf(stderr, "sdskv_length returned %d\n", ret);
            break;
        }
        std::vector<char> value(vsize);
        ret = sdskv_get(sdskv_ph, omap_db_id, (const void*)key, ksizes[i],
                        (void*)value.data(), &vsize);
        if (ret != SDSKV_SUCCESS) {
            *prval = -1;
            ERROR fprintf(stderr, "sdskv_get returned %d\n", ret);
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

static oid_t get_oid_from_name(sdskv_provider_handle_t ph,
                               sdskv_database_id_t     name_db_id,
                               const char*             name)
{
    ENTERING;
    oid_t     result   = 0;
    hg_size_t oid_size = sizeof(result);
    int ret = sdskv_get(ph, name_db_id, (const void*)name, strlen(name) + 1,
                        (void*)&result, &oid_size);
    if (ret != SDSKV_SUCCESS) result = 0;
    LEAVING;
    return result;
}
