/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PROC_READ_ACTION_H
#define __MOBJECT_PROC_READ_ACTION_H

#include <margo.h>
#include "libmobject-store.h"

/**
 * This function is the traditional hg_proc_* function meant to serialize
 * a mobject_store_read_op_t object to send it through RPC.
 */
hg_return_t hg_proc_mobject_store_read_op_t(hg_proc_t                proc,
                                            mobject_store_read_op_t* read_op);

#endif
