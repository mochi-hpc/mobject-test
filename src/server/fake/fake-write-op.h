/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FAKE_WRITE_OP_H
#define __FAKE_WRITE_OP_H

#include <margo.h>
#include "libmobject-store.h"
#include "src/server/visitor-args.h"

#ifdef __cplusplus
extern "C" {
#endif

void fake_write_op(mobject_store_write_op_t write_op,
                   server_visitor_args_t    vargs);

#ifdef __cplusplus
}
#endif

#endif
