/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <string.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/write-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

mobject_store_write_op_t create_write_op(void)
{
    mobject_store_write_op_t write_op = (mobject_store_write_op_t)calloc(
        1, sizeof(struct mobject_store_write_op));
    MOBJECT_ASSERT(write_op != MOBJECT_WRITE_OP_NULL,
                   "Could not allocate write_op");
    write_op->actions     = (wr_action_base_t)0;
    write_op->bulk_handle = HG_BULK_NULL;
    write_op->num_actions = 0;
    write_op->ready       = 0;
    return write_op;
}

void release_write_op(mobject_store_write_op_t write_op)
{
    if (write_op == MOBJECT_WRITE_OP_NULL) return;

    if (write_op->bulk_handle != HG_BULK_NULL)
        margo_bulk_free(write_op->bulk_handle);

    wr_action_base_t action, tmp;

    DL_FOREACH_SAFE(write_op->actions, action, tmp)
    {
        DL_DELETE(write_op->actions, action);
        free(action);
    }

    free(write_op);
}
