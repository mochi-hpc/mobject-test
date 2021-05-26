/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_WRITE_OP_H
#define __MOBJECT_WRITE_OP_H

#include <margo.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/write-actions.h"

/**
 * The mobject_store_write_op structure is what a
 * mobject_store_write_op_t points to (see typedef in libmobject-store.h).
 * It mainly contains a list of actions ("child" structures of
 * wr_action_base_t).
 *
 * When created on the client side, ready is set to 0 and the
 * bulk_handle is set to HG_BULK_NULL. The actions in the list may have unions
 * giving the choice between a local pointer (const char*) or a position
 * in a bulk segment (uint64_t).
 *
 * A call to prepare_bulk_for_write_op will convert all the pointers to
 * positions in a bulk handle and make the object ready to be sent to a server.
 * It will also set ready to 1, at which point adding more actions
 * to the list becomes forbiden.
 *
 * The hg_proc_mobject_store_write_op_t function allows serializing and
 * deserializing the object when sending and receiving it. Serializing only
 * works for a write_op that has been prepared (prepare_bulk_for_write_op has
 * been called). When deserializing, ready is set 1 and the actions
 * refer to offsets in the bulk_handle.
 */
struct mobject_store_write_op {
    int ready; // whether the unions in the actions are
               // to be interpreted as offsets in bulk handles
    hg_bulk_t        bulk_handle; // bulk handle exposing the data
    size_t           num_actions; // number of action in the linked-list bellow
    wr_action_base_t actions;     // list of actions
};

mobject_store_write_op_t create_write_op(void);
void                     release_write_op(mobject_store_write_op_t write_op);

#endif
