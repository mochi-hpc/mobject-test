/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "src/io-chain/proc-read-actions.h"
#include "src/io-chain/args-read-actions.h"
#include "src/io-chain/read-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

/**
 * This file contains the main hg_proc_mobject_store_read_op_t
 * serialization function. It relies on helper functions to encode
 * and decode each possible write action. Encoding/decoding works
 * by creating an args_rd_action_* object and passing it the required
 * parameters, then serializing the structure along with potential
 * additional data.
 */

typedef hg_return_t (*encode_fn)(hg_proc_t, uint64_t*, void*);
typedef hg_return_t (*decode_fn)(hg_proc_t, uint64_t*, void*);

static hg_return_t
encode_read_action_stat(hg_proc_t proc, uint64_t* pos, rd_action_stat_t action);

static hg_return_t decode_read_action_stat(hg_proc_t         proc,
                                           uint64_t*         pos,
                                           rd_action_stat_t* action);

static hg_return_t
encode_read_action_read(hg_proc_t proc, uint64_t* pos, rd_action_read_t action);

static hg_return_t decode_read_action_read(hg_proc_t         proc,
                                           uint64_t*         pos,
                                           rd_action_read_t* action);

static hg_return_t encode_read_action_omap_get_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_keys_t action);

static hg_return_t decode_read_action_omap_get_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_keys_t* action);

static hg_return_t encode_read_action_omap_get_vals(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_t action);

static hg_return_t decode_read_action_omap_get_vals(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_t* action);

static hg_return_t encode_read_action_omap_get_vals_by_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_by_keys_t action);

static hg_return_t decode_read_action_omap_get_vals_by_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_by_keys_t* action);

/**
 * The following two arrays are here to avoid a big switch.
 */

/* encoding functions */
static encode_fn encode_read_action[_READ_OPCODE_END_ENUM_]
    = {NULL,
       (encode_fn)encode_read_action_stat,
       (encode_fn)encode_read_action_read,
       (encode_fn)encode_read_action_omap_get_keys,
       (encode_fn)encode_read_action_omap_get_vals,
       (encode_fn)encode_read_action_omap_get_vals_by_keys};

/* decoding functions */
static decode_fn decode_read_action[_READ_OPCODE_END_ENUM_]
    = {NULL,
       (decode_fn)decode_read_action_stat,
       (decode_fn)decode_read_action_read,
       (decode_fn)decode_read_action_omap_get_keys,
       (decode_fn)decode_read_action_omap_get_vals,
       (decode_fn)decode_read_action_omap_get_vals_by_keys};

/**
 * Serialization function for mobject_store_read_op_t objects.
 * For encoding, the object should be prepared first (that is, the union fields
 * pointing to either a buffer or an offset in a bulk should be an offset in a
 * bulk).
 */
hg_return_t hg_proc_mobject_store_read_op_t(hg_proc_t                proc,
                                            mobject_store_read_op_t* read_op)
{
    rd_action_base_t elem;
    hg_return_t      ret      = HG_SUCCESS;
    uintptr_t        position = 0;

    switch (hg_proc_get_op(proc)) {

    case HG_ENCODE:

        MOBJECT_ASSERT((*read_op)->ready,
                       "Cannot encode a read_op before it has been prepared");
        // encode the bulk handle associated with the series of operations
        ret = hg_proc_hg_bulk_t(proc, &((*read_op)->bulk_handle));
        if (ret != HG_SUCCESS) return ret;
        // encode the number of actions
        ret = hg_proc_memcpy(proc, &((*read_op)->num_actions),
                             sizeof((*read_op)->num_actions));
        if (ret != HG_SUCCESS) return ret;

        // for each action ...
        DL_FOREACH((*read_op)->actions, elem)
        {
            read_op_code_t opcode = elem->type;
            MOBJECT_ASSERT((opcode > 0 || opcode < _READ_OPCODE_END_ENUM_),
                           "Invalid read_op opcode");
            // encode the type of action
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            // encode the action's arguments
            ret = encode_read_action[opcode](proc, &position, elem);
            if (ret != HG_SUCCESS) return ret;
        }
        break;

    case HG_DECODE:

        *read_op          = create_read_op();
        (*read_op)->ready = 1;
        // decode the bulk handle
        ret = hg_proc_hg_bulk_t(proc, &((*read_op)->bulk_handle));
        if (ret != HG_SUCCESS) return ret;
        // decode the number of actions
        ret = hg_proc_memcpy(proc, &((*read_op)->num_actions),
                             sizeof((*read_op)->num_actions));
        if (ret != HG_SUCCESS) return ret;

        rd_action_base_t next_action;
        size_t           i;
        for (i = 0; i < (*read_op)->num_actions; i++) {
            // decode the current action's type
            read_op_code_t opcode;
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            MOBJECT_ASSERT((opcode > 0 || opcode < _READ_OPCODE_END_ENUM_),
                           "Invalid write_op opcode");
            // decode the action's arguments
            ret = decode_read_action[opcode](proc, &position, &next_action);
            if (ret != HG_SUCCESS) return ret;
            next_action->type = opcode;
            // append to the list
            DL_APPEND((*read_op)->actions, next_action);
        }
        break;

    case HG_FREE:

        release_read_op(*read_op);
        return HG_SUCCESS;
    }

    return ret;
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static hg_return_t
encode_read_action_stat(hg_proc_t proc, uint64_t* pos, rd_action_stat_t action)
{
    return HG_SUCCESS;
}

static hg_return_t
decode_read_action_stat(hg_proc_t proc, uint64_t* pos, rd_action_stat_t* action)
{
    hg_return_t ret = HG_SUCCESS;
    *action         = (rd_action_stat_t)calloc(1, sizeof(**action));
    return ret;
}

static hg_return_t
encode_read_action_read(hg_proc_t proc, uint64_t* pos, rd_action_read_t action)
{
    args_rd_action_read a;
    a.offset      = action->offset;
    a.len         = action->len;
    a.bulk_offset = action->buffer.as_offset;
    *pos += a.len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t
decode_read_action_read(hg_proc_t proc, uint64_t* pos, rd_action_read_t* action)
{
    hg_return_t         ret = HG_SUCCESS;
    args_rd_action_read a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action                     = (rd_action_read_t)calloc(1, sizeof(**action));
    (*action)->offset           = a.offset;
    (*action)->len              = a.len;
    (*action)->buffer.as_offset = a.bulk_offset;
    *pos += a.len;

    return ret;
}

static hg_return_t encode_read_action_omap_get_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_keys_t action)
{
    args_rd_action_omap_get_keys a;
    a.max_return = action->max_return;
    a.data_size  = action->data_size;

    hg_return_t ret;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    ret = hg_proc_memcpy(proc, action->data, action->data_size);
    return ret;
}

static hg_return_t decode_read_action_omap_get_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_keys_t* action)
{
    hg_return_t                  ret = HG_SUCCESS;
    args_rd_action_omap_get_keys a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (rd_action_omap_get_keys_t)calloc(1, sizeof(**action) - 1
                                                       + a.data_size);
    (*action)->max_return  = a.max_return;
    (*action)->data_size   = a.data_size;
    (*action)->start_after = (*action)->data;

    ret = hg_proc_memcpy(proc, (*action)->data, (*action)->data_size);
    return ret;
}

static hg_return_t encode_read_action_omap_get_vals(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_t action)
{
    args_rd_action_omap_get_vals a;
    a.max_return = action->max_return;
    a.data_size  = action->data_size;

    hg_return_t ret;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    ret = hg_proc_memcpy(proc, action->data, action->data_size);

    return ret;
}

static hg_return_t decode_read_action_omap_get_vals(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_t* action)
{
    hg_return_t                  ret = HG_SUCCESS;
    args_rd_action_omap_get_vals a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (rd_action_omap_get_vals_t)calloc(1, sizeof(**action) - 1
                                                       + a.data_size);
    (*action)->max_return  = a.max_return;
    (*action)->data_size   = a.data_size;
    (*action)->start_after = (*action)->data;

    ret      = hg_proc_memcpy(proc, (*action)->data, (*action)->data_size);
    size_t s = strlen((*action)->start_after);
    (*action)->filter_prefix = (*action)->data + s + 1;

    return ret;
}

static hg_return_t encode_read_action_omap_get_vals_by_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_by_keys_t action)
{
    args_rd_action_omap_get_vals_by_keys a;
    a.num_keys  = action->num_keys;
    a.data_size = action->data_size;

    hg_return_t ret;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    ret = hg_proc_memcpy(proc, action->data, action->data_size);

    return ret;
}

static hg_return_t decode_read_action_omap_get_vals_by_keys(
    hg_proc_t proc, uint64_t* pos, rd_action_omap_get_vals_by_keys_t* action)
{
    hg_return_t                          ret = HG_SUCCESS;
    args_rd_action_omap_get_vals_by_keys a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (rd_action_omap_get_vals_by_keys_t)calloc(1, sizeof(**action) - 1
                                                               + a.data_size);
    (*action)->num_keys  = a.num_keys;
    (*action)->data_size = a.data_size;

    ret = hg_proc_memcpy(proc, (*action)->data, (*action)->data_size);

    return ret;
}
