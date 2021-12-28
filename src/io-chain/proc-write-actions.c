/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "src/io-chain/proc-write-actions.h"
#include "src/io-chain/args-write-actions.h"
#include "src/io-chain/write-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"
#include <stdlib.h>

/**
 * This file contains the main hg_proc_mobject_store_write_op_t
 * serialization function. It relies on helper functions to encode
 * and decode each possible write action. Encoding/decoding works
 * by creating an args_wr_action_* object and passing it the required
 * parameters, then serializing the structure along with potential
 * additional data.
 */

typedef hg_return_t (*encode_fn)(hg_proc_t, uint64_t*, void*);
typedef hg_return_t (*decode_fn)(hg_proc_t, uint64_t*, void*);

static hg_return_t encode_write_action_create(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_create_t action);

static hg_return_t decode_write_action_create(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_create_t* action);

static hg_return_t encode_write_action_write(hg_proc_t         proc,
                                             uint64_t*         pos,
                                             wr_action_write_t action);

static hg_return_t decode_write_action_write(hg_proc_t          proc,
                                             uint64_t*          pos,
                                             wr_action_write_t* action);

static hg_return_t encode_write_action_write_full(
    hg_proc_t proc, uint64_t* pos, wr_action_write_full_t action);

static hg_return_t decode_write_action_write_full(
    hg_proc_t proc, uint64_t* pos, wr_action_write_full_t* action);

static hg_return_t encode_write_action_write_same(
    hg_proc_t proc, uint64_t* pos, wr_action_write_same_t action);

static hg_return_t decode_write_action_write_same(
    hg_proc_t proc, uint64_t* pos, wr_action_write_same_t* action);

static hg_return_t encode_write_action_append(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_append_t action);

static hg_return_t decode_write_action_append(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_append_t* action);

static hg_return_t encode_write_action_remove(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_remove_t action);

static hg_return_t decode_write_action_remove(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_remove_t* action);

static hg_return_t encode_write_action_truncate(hg_proc_t            proc,
                                                uint64_t*            pos,
                                                wr_action_truncate_t action);

static hg_return_t decode_write_action_truncate(hg_proc_t             proc,
                                                uint64_t*             pos,
                                                wr_action_truncate_t* action);

static hg_return_t encode_write_action_zero(hg_proc_t        proc,
                                            uint64_t*        pos,
                                            wr_action_zero_t action);

static hg_return_t decode_write_action_zero(hg_proc_t         proc,
                                            uint64_t*         pos,
                                            wr_action_zero_t* action);

static hg_return_t encode_write_action_omap_set(hg_proc_t            proc,
                                                uint64_t*            pos,
                                                wr_action_omap_set_t action);

static hg_return_t decode_write_action_omap_set(hg_proc_t             proc,
                                                uint64_t*             pos,
                                                wr_action_omap_set_t* action);

static hg_return_t encode_write_action_omap_rm_keys(
    hg_proc_t proc, uint64_t* pos, wr_action_omap_rm_keys_t action);

static hg_return_t decode_write_action_omap_rm_keys(
    hg_proc_t proc, uint64_t* pos, wr_action_omap_rm_keys_t* action);

/**
 * The following two arrays are here to avoid a big switch.
 */

/* encoding functions */
static encode_fn encode_write_action[_WRITE_OPCODE_END_ENUM_]
    = {NULL,
       (encode_fn)encode_write_action_create,
       (encode_fn)encode_write_action_write,
       (encode_fn)encode_write_action_write_full,
       (encode_fn)encode_write_action_write_same,
       (encode_fn)encode_write_action_append,
       (encode_fn)encode_write_action_remove,
       (encode_fn)encode_write_action_truncate,
       (encode_fn)encode_write_action_zero,
       (encode_fn)encode_write_action_omap_set,
       (encode_fn)encode_write_action_omap_rm_keys};

/* decoding functions */
static decode_fn decode_write_action[_WRITE_OPCODE_END_ENUM_]
    = {NULL,
       (decode_fn)decode_write_action_create,
       (decode_fn)decode_write_action_write,
       (decode_fn)decode_write_action_write_full,
       (decode_fn)decode_write_action_write_same,
       (decode_fn)decode_write_action_append,
       (decode_fn)decode_write_action_remove,
       (decode_fn)decode_write_action_truncate,
       (decode_fn)decode_write_action_zero,
       (decode_fn)decode_write_action_omap_set,
       (decode_fn)decode_write_action_omap_rm_keys};

/**
 * Serialization function for mobject_store_write_op_t objects.
 * For encoding, the object should be prepared first (that is, the union fields
 * pointing to either a buffer or an offset in a bulk should be an offset in a
 * bulk).
 */
hg_return_t hg_proc_mobject_store_write_op_t(hg_proc_t                 proc,
                                             mobject_store_write_op_t* write_op)
{
    wr_action_base_t elem;
    hg_return_t      ret      = HG_SUCCESS;
    uintptr_t        position = 0;

    switch (hg_proc_get_op(proc)) {

    case HG_ENCODE:

        MOBJECT_ASSERT((*write_op)->ready,
                       "Cannot encode a write_op before it has been prepared");
        // encode the bulk handle associated with the series of operations
        ret = hg_proc_hg_bulk_t(proc, &((*write_op)->bulk_handle));
        if (ret != HG_SUCCESS) return ret;
        // encode the number of actions
        ret = hg_proc_memcpy(proc, &((*write_op)->num_actions),
                             sizeof((*write_op)->num_actions));
        if (ret != HG_SUCCESS) return ret;

        // for each action ...
        DL_FOREACH((*write_op)->actions, elem)
        {
            write_op_code_t opcode = elem->type;
            MOBJECT_ASSERT((opcode > 0 && opcode < _WRITE_OPCODE_END_ENUM_),
                           "Invalid write_op opcode");
            // encode the type of action
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            // encode the action's arguments
            ret = encode_write_action[opcode](proc, &position, elem);
            if (ret != HG_SUCCESS) return ret;
        }
        break;

    case HG_DECODE:

        *write_op          = create_write_op();
        (*write_op)->ready = 1;

        // decode the bulk handle
        ret = hg_proc_hg_bulk_t(proc, &((*write_op)->bulk_handle));
        if (ret != HG_SUCCESS) return ret;
        // decode the number of actions
        ret = hg_proc_memcpy(proc, &((*write_op)->num_actions),
                             sizeof((*write_op)->num_actions));
        if (ret != HG_SUCCESS) return ret;

        wr_action_base_t next_action;
        size_t           i;
        for (i = 0; i < (*write_op)->num_actions; i++) {
            // decode the current action's type
            write_op_code_t opcode;
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            MOBJECT_ASSERT((opcode > 0 && opcode < _WRITE_OPCODE_END_ENUM_),
                           "Invalid write_op opcode");
            // decode the action's arguments
            ret = decode_write_action[opcode](proc, &position, &next_action);
            if (ret != HG_SUCCESS) return ret;
            next_action->type = opcode;
            // append to the list
            DL_APPEND((*write_op)->actions, next_action);
        }
        break;

    case HG_FREE:
        release_write_op(*write_op);
        return HG_SUCCESS;
    }

    return ret;
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static hg_return_t encode_write_action_create(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_create_t action)
{
    args_wr_action_create a;
    a.exclusive = action->exclusive;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_create(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_create_t* action)
{
    hg_return_t           ret = HG_SUCCESS;
    args_wr_action_create a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action              = (wr_action_create_t)calloc(1, sizeof(**action));
    (*action)->exclusive = a.exclusive;

    return ret;
}

static hg_return_t encode_write_action_write(hg_proc_t         proc,
                                             uint64_t*         pos,
                                             wr_action_write_t action)
{
    args_wr_action_write a;
    a.buffer_position = *pos;
    a.len             = action->len;
    a.offset          = action->offset;
    *pos += action->len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_write(hg_proc_t          proc,
                                             uint64_t*          pos,
                                             wr_action_write_t* action)
{
    hg_return_t          ret = HG_SUCCESS;
    args_wr_action_write a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (wr_action_write_t)calloc(1, sizeof(**action));
    (*action)->buffer.as_offset = *pos;
    (*action)->len              = a.len;
    (*action)->offset           = a.offset;
    *pos += a.len;

    return ret;
}

static hg_return_t encode_write_action_write_full(hg_proc_t              proc,
                                                  uint64_t*              pos,
                                                  wr_action_write_full_t action)
{
    args_wr_action_write_full a;
    a.buffer_position = *pos;
    a.len             = action->len;
    *pos += action->len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_write_full(
    hg_proc_t proc, uint64_t* pos, wr_action_write_full_t* action)
{
    hg_return_t               ret = HG_SUCCESS;
    args_wr_action_write_full a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (wr_action_write_full_t)calloc(1, sizeof(**action));
    (*action)->buffer.as_offset = *pos;
    (*action)->len              = a.len;
    *pos += a.len;

    return ret;
}

static hg_return_t encode_write_action_write_same(hg_proc_t              proc,
                                                  uint64_t*              pos,
                                                  wr_action_write_same_t action)
{
    args_wr_action_write_same a;
    a.buffer_position = *pos;
    a.data_len        = action->data_len;
    a.write_len       = action->write_len;
    a.offset          = action->offset;
    *pos += action->data_len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_write_same(
    hg_proc_t proc, uint64_t* pos, wr_action_write_same_t* action)
{
    hg_return_t               ret = HG_SUCCESS;
    args_wr_action_write_same a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (wr_action_write_same_t)calloc(1, sizeof(**action));
    (*action)->buffer.as_offset = *pos;
    (*action)->data_len         = a.data_len;
    (*action)->write_len        = a.write_len;
    (*action)->offset           = a.offset;
    *pos += a.data_len;

    return ret;
}

static hg_return_t encode_write_action_append(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_append_t action)
{
    args_wr_action_append a;
    a.buffer_position = *pos;
    a.len             = action->len;
    *pos += action->len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_append(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_append_t* action)
{
    hg_return_t           ret = HG_SUCCESS;
    args_wr_action_append a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (wr_action_append_t)calloc(1, sizeof(**action));
    (*action)->buffer.as_offset = *pos;
    (*action)->len              = a.len;
    *pos += a.len;

    return ret;
}

static hg_return_t encode_write_action_remove(hg_proc_t          proc,
                                              uint64_t*          pos,
                                              wr_action_remove_t action)
{
    return HG_SUCCESS;
}

static hg_return_t decode_write_action_remove(hg_proc_t           proc,
                                              uint64_t*           pos,
                                              wr_action_remove_t* action)
{
    hg_return_t ret = HG_SUCCESS;
    *action         = (wr_action_remove_t)calloc(1, sizeof(**action));

    return ret;
}

static hg_return_t encode_write_action_truncate(hg_proc_t            proc,
                                                uint64_t*            pos,
                                                wr_action_truncate_t action)
{
    args_wr_action_truncate a;
    a.offset = action->offset;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_truncate(hg_proc_t             proc,
                                                uint64_t*             pos,
                                                wr_action_truncate_t* action)
{
    hg_return_t             ret = HG_SUCCESS;
    args_wr_action_truncate a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action           = (wr_action_truncate_t)calloc(1, sizeof(**action));
    (*action)->offset = a.offset;

    return ret;
}

static hg_return_t
encode_write_action_zero(hg_proc_t proc, uint64_t* pos, wr_action_zero_t action)
{
    args_wr_action_zero a;
    a.offset = action->offset;
    a.len    = action->len;
    return hg_proc_memcpy(proc, &a, sizeof(a));
}

static hg_return_t decode_write_action_zero(hg_proc_t         proc,
                                            uint64_t*         pos,
                                            wr_action_zero_t* action)
{
    hg_return_t         ret = HG_SUCCESS;
    args_wr_action_zero a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action           = (wr_action_zero_t)calloc(1, sizeof(**action));
    (*action)->offset = a.offset;
    (*action)->len    = a.len;

    return ret;
}

static hg_return_t encode_write_action_omap_set(hg_proc_t            proc,
                                                uint64_t*            pos,
                                                wr_action_omap_set_t action)
{
    args_wr_action_omap_set a;
    a.num           = action->num;
    a.data_size     = action->data_size;
    hg_return_t ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;
    return hg_proc_memcpy(proc, action->data, action->data_size);
}

static hg_return_t decode_write_action_omap_set(hg_proc_t             proc,
                                                uint64_t*             pos,
                                                wr_action_omap_set_t* action)
{
    hg_return_t             ret = HG_SUCCESS;
    args_wr_action_omap_set a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action
        = (wr_action_omap_set_t)calloc(1, sizeof(**action) - 1 + a.data_size);
    (*action)->num       = a.num;
    (*action)->data_size = a.data_size;

    ret = hg_proc_memcpy(proc, (*action)->data, a.data_size);

    return ret;
}

static hg_return_t encode_write_action_omap_rm_keys(
    hg_proc_t proc, uint64_t* pos, wr_action_omap_rm_keys_t action)
{
    args_wr_action_omap_rm_keys a;
    a.num_keys      = action->num_keys;
    a.data_size     = action->data_size;
    hg_return_t ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;
    return hg_proc_memcpy(proc, action->data, action->data_size);
}

static hg_return_t decode_write_action_omap_rm_keys(
    hg_proc_t proc, uint64_t* pos, wr_action_omap_rm_keys_t* action)
{
    hg_return_t                 ret = HG_SUCCESS;
    args_wr_action_omap_rm_keys a;
    ret = hg_proc_memcpy(proc, &a, sizeof(a));
    if (ret != HG_SUCCESS) return ret;

    *action = (wr_action_omap_rm_keys_t)calloc(1, sizeof(**action) - 1
                                                      + a.data_size);
    (*action)->num_keys  = a.num_keys;
    (*action)->data_size = a.data_size;

    ret = hg_proc_memcpy(proc, (*action)->data, a.data_size);

    return ret;
}
