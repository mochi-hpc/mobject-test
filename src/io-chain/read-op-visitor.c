/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "libmobject-store.h"
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-op-impl.h"
#include "src/util/utlist.h"

static void execute_read_op_visitor_on_stat(read_op_visitor_t visitor,
                                            rd_action_stat_t  a,
                                            void*             uargs);
static void execute_read_op_visitor_on_read(read_op_visitor_t visitor,
                                            rd_action_read_t  a,
                                            void*             uargs);
static void execute_read_op_visitor_on_omap_get_keys(
    read_op_visitor_t visitor, rd_action_omap_get_keys_t a, void* uargs);
static void execute_read_op_visitor_on_omap_get_vals(
    read_op_visitor_t visitor, rd_action_omap_get_vals_t a, void* uargs);
static void execute_read_op_visitor_on_omap_get_vals_by_keys(
    read_op_visitor_t                 visitor,
    rd_action_omap_get_vals_by_keys_t a,
    void*                             uargs);

typedef void (*dispatch_fn)(read_op_visitor_t, rd_action_base_t, void*);

static dispatch_fn visitor_dispatch[_READ_OPCODE_END_ENUM_] = {
    NULL,
    (dispatch_fn)execute_read_op_visitor_on_stat,
    (dispatch_fn)execute_read_op_visitor_on_read,
    (dispatch_fn)execute_read_op_visitor_on_omap_get_keys,
    (dispatch_fn)execute_read_op_visitor_on_omap_get_vals,
    (dispatch_fn)execute_read_op_visitor_on_omap_get_vals_by_keys,
};

void execute_read_op_visitor(read_op_visitor_t       visitor,
                             mobject_store_read_op_t read_op,
                             void*                   uargs)
{
    rd_action_base_t a;

    visitor->visit_begin(uargs);

    DL_FOREACH((read_op->actions), a)
    {
        visitor_dispatch[a->type](visitor, a, uargs);
    }

    visitor->visit_end(uargs);
}

////////////////////////////////////////////////////////////////////////////////
//                          STATIC FUNCTIONS BELOW                            //
////////////////////////////////////////////////////////////////////////////////

static void execute_read_op_visitor_on_stat(read_op_visitor_t visitor,
                                            rd_action_stat_t  a,
                                            void*             uargs)
{
    if (visitor->visit_stat)
        visitor->visit_stat(uargs, a->psize, a->pmtime, a->prval);
}

static void execute_read_op_visitor_on_read(read_op_visitor_t visitor,
                                            rd_action_read_t  a,
                                            void*             uargs)
{
    if (visitor->visit_read)
        visitor->visit_read(uargs, a->offset, a->len, a->buffer, a->bytes_read,
                            a->prval);
}

static void execute_read_op_visitor_on_omap_get_keys(
    read_op_visitor_t visitor, rd_action_omap_get_keys_t a, void* uargs)
{
    if (visitor->visit_omap_get_keys)
        visitor->visit_omap_get_keys(uargs, a->start_after, a->max_return,
                                     a->iter, a->prval);
}

static void execute_read_op_visitor_on_omap_get_vals(
    read_op_visitor_t visitor, rd_action_omap_get_vals_t a, void* uargs)
{
    if (visitor->visit_omap_get_vals)
        visitor->visit_omap_get_vals(uargs, a->start_after, a->filter_prefix,
                                     a->max_return, a->iter, a->prval);
}

static void execute_read_op_visitor_on_omap_get_vals_by_keys(
    read_op_visitor_t visitor, rd_action_omap_get_vals_by_keys_t a, void* uargs)
{
    if (visitor->visit_omap_get_vals_by_keys == NULL) return;

    const char* keys[a->num_keys];
    unsigned    i;
    const char* ptr = a->data;
    for (i = 0; i < a->num_keys; i++) {
        keys[i] = ptr;
        ptr += strlen(keys[i]) + 1;
    }
    visitor->visit_omap_get_vals_by_keys(uargs, keys, a->num_keys, a->iter,
                                         a->prval);
}
