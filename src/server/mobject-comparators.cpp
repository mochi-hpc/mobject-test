/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include "src/server/core/key-types.h"

extern "C" bool
mobject_oid_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    oid_t x = *((oid_t*)k1);
    oid_t y = *((oid_t*)k2);
    return x < y;
}

extern "C" bool
mobject_name_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const char* n1 = (const char*)k1;
    const char* n2 = (const char*)k2;
    return strcmp(n1, n2) < 0;
}

extern "C" bool
mobject_seg_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const segment_key_t* seg1 = (const segment_key_t*)k1;
    const segment_key_t* seg2 = (const segment_key_t*)k2;
    if (seg1->oid < seg2->oid) return true;
    if (seg1->oid > seg2->oid) return false;
    if (seg1->timestamp > seg2->timestamp) return true;
    if (seg1->timestamp < seg2->timestamp) return false;
    if (seg1->seq_id > seg2->seq_id) return true;
    if (seg1->seq_id < seg2->seq_id) return false;
    return false;
}

extern "C" bool
mobject_omap_map_compare(const void* k1, size_t sk1, const void* k2, size_t sk2)
{
    const omap_key_t* ok1 = (const omap_key_t*)k1;
    const omap_key_t* ok2 = (const omap_key_t*)k2;
    if (ok1->oid < ok2->oid) return true;
    if (ok1->oid > ok2->oid) return false;
    return strcmp(ok1->key, ok2->key) < 0;
}
