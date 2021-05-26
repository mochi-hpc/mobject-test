/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_READ_OP_H
#define __MOBJECT_READ_OP_H

#include <margo.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/read-actions.h"

/**
 * This object represents a handler for a list of actions
 * to perform on a particular object.
 * "ready" indicates that the object is ready to be
 * sent to be used for bulk transfers: all pointers
 * have been converted into an offset in a bulk handle.
 * It can therefore be sent to a server and processed.
 */
struct mobject_store_read_op {
    int              ready;
    hg_bulk_t        bulk_handle;
    size_t           num_actions;
    rd_action_base_t actions;
};

mobject_store_read_op_t create_read_op(void);
void                    release_read_op(mobject_store_read_op_t read_op);

#endif
