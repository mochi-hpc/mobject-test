/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __CORE_KEY_TYPES_H
#define __CORE_KEY_TYPES_H

#include <stdint.h>
#include <bake-client.h>

typedef uint64_t oid_t;

typedef enum seg_type_t
{
    ZERO         = 0,
    BAKE_REGION  = 1,
    SMALL_REGION = 2,
    TOMBSTONE    = 3
} seg_type_t;

/* a ZERO segment has no data attached in the kv database,
   it indicates that the segment is filled with 0s */
/* a BAKE_REGION segment has a bake_region_id_t as value
   in a kv database. It indicates that the data for the
   [start_index, end_index[ segment of the object can be
   found in this bulk region. */
/* a SMALL_REGION segment is the same as a BAKE_REGION
   but the content of the value in the database is the
   data itself, not a bake_region_id_t. Note that the
   threshold to use small region optimizations
   (SMALL_REGION_THRESHOLD) should not exceed the value
   of sizeof(bake_region_id_t). */
/* a TOMSTONE segment is used to invalidate a portion
   of an object. This portion if [start_index, +infinity[. */

typedef struct segment_key_t {
    oid_t  oid;
    time_t timestamp;
    // double timestamp;
    uint32_t seq_id;
    uint32_t type;        /* seg_type */
    uint64_t start_index; // first index, included
    uint64_t end_index;   // end index is not included
} segment_key_t;

typedef struct omap_key_t {
    oid_t oid;
    char  key[1];
} omap_key_t;

#define MAX_OMAP_KEY_SIZE 128
#define MAX_OMAP_VAL_SIZE 256

#define SMALL_REGION_THRESHOLD (sizeof(bake_region_id_t))

#endif
