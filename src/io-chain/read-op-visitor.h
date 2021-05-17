/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_OP_VISITOR_H
#define __MOBJECT_READ_OP_VISITOR_H

#include "libmobject-store.h"
#include "src/util/buffer-union.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct read_op_visitor {
    void (*visit_begin)(void*);
    void (*visit_stat)(void*, uint64_t*, time_t*, int*);
    void (*visit_read)(void*, uint64_t, size_t, buffer_u, size_t*, int*);
    void (*visit_omap_get_keys)(
        void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
    void (*visit_omap_get_vals)(void*,
                                const char*,
                                const char*,
                                uint64_t,
                                mobject_store_omap_iter_t*,
                                int*);
    void (*visit_omap_get_vals_by_keys)(
        void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
    void (*visit_end)(void*);
} * read_op_visitor_t;

void execute_read_op_visitor(read_op_visitor_t       visitor,
                             mobject_store_read_op_t read_op,
                             void*                   uarg);

#ifdef __cplusplus
}
#endif

#endif
