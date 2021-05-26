/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "src/io-chain/prepare-write-op.h"
#include "src/io-chain/write-op-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

static void convert_write(uint64_t*         cur_offset,
                          wr_action_write_t action,
                          void**            ptr,
                          size_t*           len);

static void convert_write_full(uint64_t*              cur_offset,
                               wr_action_write_full_t action,
                               void**                 ptr,
                               size_t*                len);

static void convert_write_same(uint64_t*              cur_offset,
                               wr_action_write_same_t action,
                               void**                 ptr,
                               size_t*                len);

static void convert_append(uint64_t*          cur_offset,
                           wr_action_append_t action,
                           void**             ptr,
                           size_t*            len);

void prepare_write_op(margo_instance_id mid, mobject_store_write_op_t write_op)
{
    if (write_op->ready == 1) return;
    if (write_op->num_actions == 0) {
        write_op->ready = 1;
        return;
    }

    wr_action_base_t action;

    void**   pointers = (void**)calloc(write_op->num_actions, sizeof(void*));
    size_t*  lengths  = (size_t*)calloc(write_op->num_actions, sizeof(size_t));
    uint64_t current_offset = 0;
    size_t   i              = 0;

    DL_FOREACH(write_op->actions, action)
    {

        switch (action->type) {
        case WRITE_OPCODE_WRITE:
            convert_write(&current_offset, (wr_action_write_t)action,
                          pointers + i, lengths + i);
            i += 1;
            break;
        case WRITE_OPCODE_WRITE_FULL:
            convert_write_full(&current_offset, (wr_action_write_full_t)action,
                               pointers + i, lengths + i);
            i += 1;
            break;
        case WRITE_OPCODE_WRITE_SAME:
            convert_write_same(&current_offset, (wr_action_write_same_t)action,
                               pointers + i, lengths + i);
            i += 1;
            break;
        case WRITE_OPCODE_APPEND:
            convert_append(&current_offset, (wr_action_append_t)action,
                           pointers + i, lengths + i);
            i += 1;
            break;
        default:
            /* nothing to do for other op types */
            break;
        }
    }

    uint32_t count = i;
    if (count != 0) {
        hg_return_t ret
            = margo_bulk_create(mid, count, pointers, lengths,
                                HG_BULK_READ_ONLY, &(write_op->bulk_handle));
        // TODO handle error
        assert(ret == HG_SUCCESS);
    }

    write_op->ready = 1;
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static void convert_write(uint64_t*         cur_offset,
                          wr_action_write_t action,
                          void**            ptr,
                          size_t*           len)
{
    uint64_t pos = *cur_offset;
    *cur_offset += action->len;
    *ptr                     = (void*)action->buffer.as_pointer;
    *len                     = action->len;
    action->buffer.as_offset = pos;
}

static void convert_write_full(uint64_t*              cur_offset,
                               wr_action_write_full_t action,
                               void**                 ptr,
                               size_t*                len)
{
    uint64_t pos = *cur_offset;
    *cur_offset += action->len;
    *ptr                     = (void*)action->buffer.as_pointer;
    *len                     = action->len;
    action->buffer.as_offset = pos;
}

static void convert_write_same(uint64_t*              cur_offset,
                               wr_action_write_same_t action,
                               void**                 ptr,
                               size_t*                len)
{
    uint64_t pos = *cur_offset;
    *cur_offset += action->data_len;
    *ptr                     = (void*)action->buffer.as_pointer;
    *len                     = action->data_len;
    action->buffer.as_offset = pos;
}

static void convert_append(uint64_t*          cur_offset,
                           wr_action_append_t action,
                           void**             ptr,
                           size_t*            len)
{
    uint64_t pos = *cur_offset;
    *cur_offset += action->len;
    *ptr                     = (void*)action->buffer.as_pointer;
    *len                     = action->len;
    action->buffer.as_offset = pos;
}
