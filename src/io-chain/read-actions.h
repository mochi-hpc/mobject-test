/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_OPCODES_H
#define __MOBJECT_READ_OPCODES_H

#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/util/buffer-union.h"

typedef enum
{
    READ_OPCODE_BASE = 0,
    READ_OPCODE_STAT,
    READ_OPCODE_READ,
    READ_OPCODE_OMAP_GET_KEYS,
    READ_OPCODE_OMAP_GET_VALS,
    READ_OPCODE_OMAP_GET_VALS_BY_KEYS,
    _READ_OPCODE_END_ENUM_
} read_op_code_t;

#define READ_ACTION_DOWNCAST(child_obj, base_obj, child_category)    \
    if (READ_OPCODE_##child_category != base_obj->type) {            \
        MOBJECT_LOG("Downcast error: " #base_obj                     \
                    " is not of type READ_ACTION_" #child_category); \
    }                                                                \
    struct rd_action_##child_category* child_obj                     \
        = (struct rd_action_##child_category*)base_obj;

#define READ_ACTION_UPCAST(base_obj, child_obj) \
    struct rd_action_BASE* base_obj = (struct rd_action_BASE*)child_obj;

typedef struct rd_action_BASE {
    read_op_code_t         type;
    struct rd_action_BASE* prev;
    struct rd_action_BASE* next;
} * rd_action_base_t;

typedef struct rd_action_STAT {
    struct rd_action_BASE base;
    uint64_t*             psize;
    time_t*               pmtime;
    int*                  prval;
} * rd_action_stat_t;

typedef struct rd_action_READ {
    struct rd_action_BASE base;
    uint64_t              offset;
    size_t                len;
    buffer_u              buffer;
    size_t*               bytes_read;
    int*                  prval;
} * rd_action_read_t;

typedef struct rd_action_OMAP_GET_KEYS {
    struct rd_action_BASE      base;
    const char*                start_after;
    uint64_t                   max_return;
    mobject_store_omap_iter_t* iter;
    int*                       prval;
    size_t                     data_size;
    char                       data[1];
} * rd_action_omap_get_keys_t;
// data field here to hold embedded data (start_after)

typedef struct rd_action_OMAP_GET_VALS {
    struct rd_action_BASE      base;
    const char*                start_after;
    const char*                filter_prefix;
    uint64_t                   max_return;
    mobject_store_omap_iter_t* iter;
    int*                       prval;
    size_t                     data_size;
    char                       data[1];
} * rd_action_omap_get_vals_t;
// data field here to hold embedded data (start_after
// and filter_prefix strings)

typedef struct rd_action_OMAP_GET_VALS_BY_KEYS {
    struct rd_action_BASE      base;
    size_t                     num_keys;
    mobject_store_omap_iter_t* iter;
    int*                       prval;
    size_t                     data_size;
    char                       data[1];
} * rd_action_omap_get_vals_by_keys_t;
// data is a contiguous buffer holding all
// the null-terminated keys
#endif
