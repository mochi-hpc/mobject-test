/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PROC_WRITE_ACTION_H
#define __MOBJECT_PROC_WRITE_ACTION_H

#include <margo.h>
#include "libmobject-store.h"

/**
 * This function is the traditional hg_proc_* function meant to serialize
 * a mobject_store_write_op_t object to send it through RPC.
 */
hg_return_t
hg_proc_mobject_store_write_op_t(hg_proc_t                 proc,
                                 mobject_store_write_op_t* write_op);

#endif
