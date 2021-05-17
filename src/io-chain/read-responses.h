/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_RESPONSE_H
#define __MOBJECT_READ_RESPONSE_H

#include <stdint.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"

typedef enum
{
    READ_RESPCODE_BASE = 0,
    READ_RESPCODE_STAT,
    READ_RESPCODE_READ,
    READ_RESPCODE_OMAP,
    _READ_RESPCODE_END_ENUM_
} read_resp_code_t;

typedef struct rd_response_BASE {
    read_resp_code_t         type;
    struct rd_response_BASE* prev;
    struct rd_response_BASE* next;
} * rd_response_base_t;

/**
 * stat response
 */
typedef struct rd_response_STAT {
    struct rd_response_BASE base;
    uint64_t                psize;
    time_t                  pmtime;
    int                     prval;
} * rd_response_stat_t;

/**
 * read response
 */
typedef struct rd_response_READ {
    struct rd_response_BASE base;
    size_t                  bytes_read;
    int                     prval;
} * rd_response_read_t;

/**
 * omap_* responses
 */
typedef struct rd_response_OMAP {
    struct rd_response_BASE   base;
    int                       prval;
    mobject_store_omap_iter_t iter;
} * rd_response_omap_t;

#endif
