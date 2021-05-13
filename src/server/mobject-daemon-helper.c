/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <sdskv-server.h>

#include "mobject-server.h"
#include "src/server/core/key-types.h"

#define ASSERT(__cond, __msg, ...)                                \
    {                                                             \
        if (!(__cond)) {                                          \
            fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, \
                    __VA_ARGS__);                                 \
            exit(-1);                                             \
        }                                                         \
    }

/* comparison functions for SDSKV */
static int oid_map_compare(const void*, size_t, const void*, size_t);
static int name_map_compare(const void*, size_t, const void*, size_t);
static int seg_map_compare(const void*, size_t, const void*, size_t);
static int omap_map_compare(const void*, size_t, const void*, size_t);

int mobject_sdskv_provider_setup(sdskv_provider_t sdskv_prov,
                                 const char*      sdskv_path,
                                 sdskv_db_type_t  sdskv_backend)
{
    int ret;
    /* SDSKV provider initialization */
    sdskv_provider_add_comparison_function(
        sdskv_prov, "mobject_oid_map_compare", oid_map_compare);
    sdskv_provider_add_comparison_function(
        sdskv_prov, "mobject_name_map_compare", name_map_compare);
    sdskv_provider_add_comparison_function(
        sdskv_prov, "mobject_seg_map_compare", seg_map_compare);
    sdskv_provider_add_comparison_function(
        sdskv_prov, "mobject_omap_map_compare", omap_map_compare);

    sdskv_database_id_t oid_map_id, name_map_id, seg_map_id, omap_map_id;
    sdskv_config_t      config;
    memset(&config, 0, sizeof(config));

    config.db_name         = "oid_map";
    config.db_path         = sdskv_path;
    config.db_type         = sdskv_backend;
    config.db_comp_fn_name = "mobject_oid_map_compare";
    ret = sdskv_provider_attach_database(sdskv_prov, &config, &oid_map_id);
    ASSERT(ret == 0,
           "sdskv_provider_attach_database() failed to add database "
           "\"oid_map\" (ret = %d)\n",
           ret);

    config.db_name         = "name_map";
    config.db_path         = sdskv_path;
    config.db_type         = sdskv_backend;
    config.db_comp_fn_name = "mobject_name_map_compare";
    ret = sdskv_provider_attach_database(sdskv_prov, &config, &name_map_id);
    ASSERT(ret == 0,
           "sdskv_provider_attach_database() failed to add database "
           "\"name_map\" (ret = %d)\n",
           ret);

    config.db_name         = "seg_map";
    config.db_path         = sdskv_path;
    config.db_type         = sdskv_backend;
    config.db_comp_fn_name = "mobject_seg_map_compare";
    ret = sdskv_provider_attach_database(sdskv_prov, &config, &seg_map_id);
    ASSERT(ret == 0,
           "sdskv_provider_attach_database() failed to add database "
           "\"seg_map\" (ret = %d)\n",
           ret);

    config.db_name         = "omap_map";
    config.db_path         = sdskv_path;
    config.db_type         = sdskv_backend;
    config.db_comp_fn_name = "mobject_omap_map_compare";
    ret = sdskv_provider_attach_database(sdskv_prov, &config, &omap_map_id);
    ASSERT(ret == 0,
           "sdskv_provider_attach_database() failed to add database "
           "\"omap_map\" (ret = %d)\n",
           ret);

    return (0);
}

static int
oid_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    oid_t x = *((oid_t*)k1);
    oid_t y = *((oid_t*)k2);
    if (x == y) return 0;
    if (x < y) return -1;
    return 1;
}

static int
name_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const char* n1 = (const char*)k1;
    const char* n2 = (const char*)k2;
    return strcmp(n1, n2);
}

static int
seg_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const segment_key_t* seg1 = (const segment_key_t*)k1;
    const segment_key_t* seg2 = (const segment_key_t*)k2;
    if (seg1->oid < seg2->oid) return -1;
    if (seg1->oid > seg2->oid) return 1;
    if (seg1->timestamp > seg2->timestamp) return -1;
    if (seg1->timestamp < seg2->timestamp) return 1;
    if (seg1->seq_id > seg2->seq_id) return -1;
    if (seg1->seq_id < seg2->seq_id) return 1;
    return 0;
}

static int
omap_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const omap_key_t* ok1 = (const omap_key_t*)k1;
    const omap_key_t* ok2 = (const omap_key_t*)k2;
    if (ok1->oid < ok2->oid) return -1;
    if (ok1->oid > ok2->oid) return 1;
    return strcmp(ok1->key, ok2->key);
}
