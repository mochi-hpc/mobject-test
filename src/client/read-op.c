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

mobject_store_read_op_t mobject_create_read_op(void)
{
    return create_read_op();
}

void mobject_release_read_op(mobject_store_read_op_t read_op)
{
    release_read_op(read_op);
}

void mobject_read_op_stat(mobject_store_read_op_t read_op,
                          uint64_t*               psize,
                          time_t*                 pmtime,
                          int*                    prval)
{
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "invalid mobject_store_read_op_t obect");
    MOBJECT_ASSERT(!(read_op->ready),
                   "can't modify a read_op that is ready to be processed");

    rd_action_stat_t action = (rd_action_stat_t)calloc(1, sizeof(*action));
    action->base.type       = READ_OPCODE_STAT;
    action->psize           = psize;
    action->pmtime          = pmtime;
    action->prval           = prval;

    READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

    read_op->num_actions += 1;
}

void mobject_read_op_read(mobject_store_read_op_t read_op,
                          char*                   buffer,
                          uint64_t                offset,
                          size_t                  len,
                          size_t*                 bytes_read,
                          int*                    prval)
{
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "invalid mobject_store_read_op_t obect");
    MOBJECT_ASSERT(!(read_op->ready),
                   "can't modify a read_op that is ready to be processed");

    rd_action_read_t action   = (rd_action_read_t)calloc(1, sizeof(*action));
    action->base.type         = READ_OPCODE_READ;
    action->offset            = offset;
    action->len               = len;
    action->buffer.as_pointer = buffer;
    action->bytes_read        = bytes_read;
    action->prval             = prval;

    READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

    read_op->num_actions += 1;

    memset(buffer, 0, len);
}

void mobject_read_op_omap_get_keys(mobject_store_read_op_t    read_op,
                                   const char*                start_after,
                                   uint64_t                   max_return,
                                   mobject_store_omap_iter_t* iter,
                                   int*                       prval)
{
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "invalid mobject_store_read_op_t obect");
    MOBJECT_ASSERT(!(read_op->ready),
                   "can't modify a read_op that is ready to be processed");

    size_t strl = strlen(start_after);

    rd_action_omap_get_keys_t action
        = (rd_action_omap_get_keys_t)calloc(1, sizeof(*action) + strl);
    action->base.type   = READ_OPCODE_OMAP_GET_KEYS;
    action->start_after = action->data;
    action->max_return  = max_return;
    action->iter        = iter;
    action->prval       = prval;
    action->data_size   = strl + 1;
    strcpy(action->data, start_after);

    READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

    read_op->num_actions += 1;
}

void mobject_read_op_omap_get_vals(mobject_store_read_op_t    read_op,
                                   const char*                start_after,
                                   const char*                filter_prefix,
                                   uint64_t                   max_return,
                                   mobject_store_omap_iter_t* iter,
                                   int*                       prval)
{
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "invalid mobject_store_read_op_t obect");
    MOBJECT_ASSERT(!(read_op->ready),
                   "can't modify a read_op that is ready to be processed");

    // compute required size for embedded data
    size_t strl1     = strlen(start_after) + 1;
    size_t strl2     = strlen(filter_prefix) + 1;
    size_t extra_mem = strl1 + strl2;

    rd_action_omap_get_vals_t action
        = (rd_action_omap_get_vals_t)calloc(1, sizeof(*action) - 1 + extra_mem);
    action->base.type     = READ_OPCODE_OMAP_GET_VALS;
    action->start_after   = action->data;
    action->filter_prefix = action->data + strl1;
    action->max_return    = max_return;
    action->iter          = iter;
    action->prval         = prval;
    action->data_size     = extra_mem;
    strcpy(action->data, start_after);
    strcpy(action->data + strl1, filter_prefix);

    READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

    read_op->num_actions += 1;
}

void mobject_read_op_omap_get_vals_by_keys(mobject_store_read_op_t    read_op,
                                           char const* const*         keys,
                                           size_t                     keys_len,
                                           mobject_store_omap_iter_t* iter,
                                           int*                       prval)
{
    MOBJECT_ASSERT(read_op != MOBJECT_READ_OP_NULL,
                   "invalid mobject_store_read_op_t obect");
    MOBJECT_ASSERT(!(read_op->ready),
                   "can't modify a read_op that is ready to be processed");

    // computing extra memory required to hold keys
    size_t extra_mem = 0;
    size_t i;
    for (i = 0; i < keys_len; i++) { extra_mem += strlen(keys[i]) + 1; }

    rd_action_omap_get_vals_by_keys_t action
        = (rd_action_omap_get_vals_by_keys_t)calloc(1, sizeof(*action) - 1
                                                           + extra_mem);
    action->base.type = READ_OPCODE_OMAP_GET_VALS_BY_KEYS;
    action->num_keys  = keys_len;
    action->iter      = iter;
    action->prval     = prval;
    action->data_size = extra_mem;
    char* s           = action->data;
    for (i = 0; i < keys_len; i++) {
        strcpy(s, keys[i]);
        s += strlen(keys[i]) + 1;
    }

    READ_ACTION_UPCAST(base, action);
    DL_APPEND(read_op->actions, base);

    read_op->num_actions += 1;
}
