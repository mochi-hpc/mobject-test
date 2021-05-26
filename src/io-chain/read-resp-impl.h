/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_RESP_IMPL_H
#define __MOBJECT_READ_RESP_IMPL_H

#include <stdint.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"

typedef struct rd_response_BASE* rd_response_base_t;

typedef struct read_response {
    size_t             num_responses;
    rd_response_base_t responses;
} * read_response_t;

read_response_t build_matching_read_responses(mobject_store_read_op_t actions);
void            free_read_responses(read_response_t response);
void feed_read_op_pointers_from_response(mobject_store_read_op_t actions,
                                         read_response_t         response);

#endif
