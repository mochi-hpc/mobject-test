/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PRINT_WRITE_OP_H
#define __PRINT_WRITE_OP_H

#include <margo.h>
#include "libmobject-store.h"

void print_write_op(mobject_store_write_op_t write_op, const char* object_name);

#endif
