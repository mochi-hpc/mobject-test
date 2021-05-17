/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PRINT_READ_OP_H
#define __PRINT_READ_OP_H

#include <margo.h>
#include "libmobject-store.h"

void print_read_op(mobject_store_read_op_t read_op, const char* object_name);

#endif
