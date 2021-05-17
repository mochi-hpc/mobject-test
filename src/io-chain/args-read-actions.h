/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_ARGS_READ_ACTION_H
#define __MOBJECT_ARGS_READ_ACTION_H

#include "mobject-store-config.h"
#include "src/io-chain/read-actions.h"

/**
 * This file contains a set of structures meant
 * to store arguments for read_op operations in
 * a buffer. Some of these structures are self-sufficient.
 * Some other are meant to be used as headers, and the
 * the serialized buffer actually contains additional
 * data after them.
 */

/**
 * stat operation
 * no header (so no defined structure)
 * no extra data
 */
// typedef struct args_rd_action_STAT {
// } args_rd_action_stat;

/**
 * read operation
 * no extra data
 */
typedef struct args_rd_action_READ {
    uint64_t offset;
    size_t   len;
    uint64_t bulk_offset;
} args_rd_action_read;

/**
 * omap_get_keys operation
 * extra data contains the start_after string
 * (see read-actions.h)
 */
typedef struct args_rd_action_OMAP_GET_KEYS {
    uint64_t max_return;
    size_t   data_size;
} args_rd_action_omap_get_keys;

/**
 * omap_get_vals operation
 * extra data contains the start_after and
 * filter_prefix strings
 * (see read-actions.h)
 */
typedef struct args_rd_action_OMAP_GET_VALS {
    uint64_t max_return;
    size_t   data_size;
} args_rd_action_omap_get_vals;

/**
 * omap_get_vals_by_keys operation
 * extra data contains the list of null-terminated
 * keys; the total size of the extra data in bytes is
 * in data_size
 */
typedef struct args_rd_action_OMAP_GET_VALS_BY_KEYS {
    size_t num_keys;
    size_t data_size;
} args_rd_action_omap_get_vals_by_keys;

#endif
