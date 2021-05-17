/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PREPARE_READ_OP_H
#define __MOBJECT_PREPARE_READ_OP_H

#include <margo.h>
#include "libmobject-store.h"

/**
 * This function takes a read_op that was created by the client
 * and prepares it to be sent to a server. This means creating a bulk
 * handle that stiches together all the buffers that the user wants to use
 * as a destination, and replacing all pointers in the chain of actions
 * by offsets within the resulting hg_bultk_t object.
 */
void prepare_read_op(margo_instance_id mid, mobject_store_read_op_t read_op);

#endif
