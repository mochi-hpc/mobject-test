/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PI_WRITE_ACTION_H
#define __MOBJECT_PI_WRITE_ACTION_H

#include "mobject-store-config.h"
#include "src/io-chain/write-actions.h"

/**
 * This file contains a set of structures meant
 * to store arguments for write_op operations in
 * a buffer. Some of these structures are self-sufficient.
 * Some other are meant to be used as headers, and the
 * the serialized buffer actually contains additional
 * data after them.
 */

/**
 * create operation
 * no extra data
 */
typedef struct args_wr_action_CREATE {
    int exclusive;
} args_wr_action_create;

/**
 * write operation
 * no extra data
 */
typedef struct args_wr_action_WRITE {
    uint64_t buffer_position; // position in the received bulk handle
    size_t   len;             // length in the received bulk handle
    uint64_t offset; // offset at which to position the data in the object
} args_wr_action_write;

/**
 * write_full operation
 * no extra data
 */
typedef struct args_wr_action_WRITE_FULL {
    uint64_t buffer_position; // position in the received bulk handle
    size_t   len;             // length in the received bulk handle
} args_wr_action_write_full;

/**
 * writesame operation
 * no extra data
 */
typedef struct args_wr_action_WRITE_SAME {
    uint64_t buffer_position; // position in the received bulk handle
    size_t   data_len;        // length to take from received data
    size_t   write_len;       // length to write in the object
    uint64_t offset;          // at which offset to write in the object
} args_wr_action_write_same;

/**
 * append operation
 * no extra data
 */
typedef struct args_wr_action_APPEND {
    uint64_t buffer_position; // position in the received bulk handle
    size_t   len;             // length to take from received data
} args_wr_action_append;

/**
 * remove operation
 * no header (so no definition needed)
 * no extra data
 */
// typedef struct args_wr_action_REMOVE {
// } args_wr_action_remove;

/**
 * truncate operation
 * no extra data
 */
typedef struct args_wr_action_TRUNCATE {
    uint64_t offset; // offset at which to truncate the object
} args_wr_action_truncate;

/**
 * zero operation
 * no extra data
 */
typedef struct args_wr_action_ZERO {
    uint64_t offset; // offset at which to start zero-ing
    uint64_t len;    // length to set to zero
} args_wr_action_zero;

/**
 * omap_set operation
 * data_size represents the size of the extra data
 * to be read after this header.
 * extra data contains serialized keys and values
 * (see write-op-impl.h for the format)
 */
typedef struct args_wr_action_OMAP_SET {
    size_t num;
    size_t data_size;
} args_wr_action_omap_set;

/**
 * rm_keys operation
 * data_size represents the size of the extra data
 * to be read after this header.
 * extra data conrains serialized key names
 * (see write-op-impl.h for the format)
 */
typedef struct args_wr_action_RM_KEYS {
    size_t num_keys;
    size_t data_size;
} args_wr_action_omap_rm_keys;

#endif
