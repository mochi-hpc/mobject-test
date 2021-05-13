/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_WRITE_OPCODES_H
#define __MOBJECT_WRITE_OPCODES_H

#include "mobject-store-config.h"
#include "src/util/buffer-union.h"

typedef enum
{
    WRITE_OPCODE_BASE = 0,
    WRITE_OPCODE_CREATE,
    WRITE_OPCODE_WRITE,
    WRITE_OPCODE_WRITE_FULL,
    WRITE_OPCODE_WRITE_SAME,
    WRITE_OPCODE_APPEND,
    WRITE_OPCODE_REMOVE,
    WRITE_OPCODE_TRUNCATE,
    WRITE_OPCODE_ZERO,
    WRITE_OPCODE_OMAP_SET,
    WRITE_OPCODE_OMAP_RM_KEYS,
    _WRITE_OPCODE_END_ENUM_
} write_op_code_t;

#define WRITE_ACTION_DOWNCAST(child_obj, base_obj, child_category)    \
    if (WRITE_OPCODE_##child_category != base_obj->type) {            \
        MOBJECT_LOG("Downcast error: " #base_obj                      \
                    " is not of type WRITE_ACTION_" #child_category); \
    }                                                                 \
    struct wr_action_##child_category* child_obj                      \
        = (struct wr_action_##child_category*)base_obj;

#define WRITE_ACTION_UPCAST(base_obj, child_obj) \
    struct wr_action_BASE* base_obj = (struct wr_action_BASE*)child_obj;

typedef struct wr_action_BASE {
    write_op_code_t        type;
    struct wr_action_BASE* prev;
    struct wr_action_BASE* next;
} * wr_action_base_t;

typedef struct wr_action_CREATE {
    struct wr_action_BASE base;
    int                   exclusive;
} * wr_action_create_t;

typedef struct wr_action_WRITE {
    struct wr_action_BASE base;
    buffer_u              buffer;
    size_t                len;
    uint64_t              offset;
} * wr_action_write_t;

typedef struct wr_action_WRITE_FULL {
    struct wr_action_BASE base;
    buffer_u              buffer;
    size_t                len;
} * wr_action_write_full_t;

typedef struct wr_action_WRITE_SAME {
    struct wr_action_BASE base;
    buffer_u              buffer;
    size_t                data_len;
    size_t                write_len;
    uint64_t              offset;
} * wr_action_write_same_t;

typedef struct wr_action_APPEND {
    struct wr_action_BASE base;
    buffer_u              buffer;
    size_t                len;
} * wr_action_append_t;

typedef struct wr_action_REMOVE {
    struct wr_action_BASE base;
} * wr_action_remove_t;

typedef struct wr_action_TRUNCATE {
    struct wr_action_BASE base;
    uint64_t              offset;
} * wr_action_truncate_t;

typedef struct wr_action_ZERO {
    struct wr_action_BASE base;
    uint64_t              offset;
    uint64_t              len;
} * wr_action_zero_t;

typedef struct wr_action_OMAP_SET {
    struct wr_action_BASE base;
    size_t                num;
    size_t                data_size;
    char                  data[1];
} * wr_action_omap_set_t;
// data above is meant to hold keys, lengths, and values,
// all put together in the same contiguous buffer.
// The buffer holds a series of [key,len,value] segments
// where key is a null-terminated string, len is a size_t,
// and value is a len-sized segment.

typedef struct wr_action_RM_KEYS {
    struct wr_action_BASE base;
    size_t                num_keys;
    size_t                data_size;
    char                  data[1];
} * wr_action_omap_rm_keys_t;
// keys above is meant to hold keys in a contiguous buffer.
// The keys are null-terminated strings.

#endif
