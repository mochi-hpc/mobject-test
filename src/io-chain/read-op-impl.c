/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <string.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/io-chain/read-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

mobject_store_read_op_t create_read_op(void)
{
    mobject_store_read_op_t read_op
        = (mobject_store_read_op_t)calloc(1, sizeof(*read_op));
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "Could not allocate read_op");
    read_op->actions     = (rd_action_base_t)0;
    read_op->bulk_handle = HG_BULK_NULL;
    read_op->ready       = 0;
    return read_op;
}

void release_read_op(mobject_store_read_op_t read_op)
{
    if(read_op == MOBJECT_READ_OP_NULL) return;

    if(read_op->bulk_handle != HG_BULK_NULL)
        margo_bulk_free(read_op->bulk_handle);

    rd_action_base_t action, tmp;

    DL_FOREACH_SAFE(read_op->actions, action, tmp)
    {
        DL_DELETE(read_op->actions, action);
        free(action);
    }

    free(read_op);
}
