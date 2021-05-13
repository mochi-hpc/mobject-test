/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_WRITE_OP_VISITOR_H
#define __MOBJECT_WRITE_OP_VISITOR_H

#include "libmobject-store.h"
#include "src/util/buffer-union.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct write_op_visitor {
    void (*visit_begin)(void*);
    void (*visit_create)(void*, int);
    void (*visit_write)(void*, buffer_u, size_t, uint64_t);
    void (*visit_write_full)(void*, buffer_u, size_t);
    void (*visit_writesame)(void*, buffer_u, size_t, size_t, uint64_t);
    void (*visit_append)(void*, buffer_u, size_t);
    void (*visit_remove)(void*);
    void (*visit_truncate)(void*, uint64_t);
    void (*visit_zero)(void*, uint64_t, uint64_t);
    void (*visit_omap_set)(
        void*, char const* const*, char const* const*, const size_t*, size_t);
    void (*visit_omap_rm_keys)(void*, char const* const*, size_t);
    void (*visit_end)(void*);
} * write_op_visitor_t;

void execute_write_op_visitor(write_op_visitor_t       visitor,
                              mobject_store_write_op_t write_op,
                              void*                    uargs);

#ifdef __cplusplus
}
#endif

#endif
