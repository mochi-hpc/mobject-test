/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include "src/io-chain/read-op-impl.h"
#include "src/io-chain/read-responses.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/io-chain/read-actions.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

/**
 * "build" functions
 * Taking an action and building a response object for it.
 */
typedef rd_response_base_t (*build_matching_fn)(rd_action_base_t);

static rd_response_base_t build_matching_stat(rd_action_stat_t a);
static rd_response_base_t build_matching_read(rd_action_read_t a);
static rd_response_base_t
build_matching_omap_get_keys(rd_action_omap_get_keys_t a);
static rd_response_base_t
build_matching_omap_get_vals(rd_action_omap_get_vals_t a);
static rd_response_base_t
build_matching_omap_get_vals_by_keys(rd_action_omap_get_vals_by_keys_t a);

/**
 * "feed" functions
 * Taking an action and a response, feeding the response's fields
 * into the actions's pointers.
 */
typedef void (*feed_action_fn)(rd_action_base_t, rd_response_base_t);

static void feed_stat_action(rd_action_stat_t a, rd_response_stat_t r);
static void feed_read_action(rd_action_read_t a, rd_response_read_t r);
static void feed_omap_get_keys_action(rd_action_omap_get_keys_t a,
                                      rd_response_omap_t        r);
static void feed_omap_get_vals_action(rd_action_omap_get_vals_t a,
                                      rd_response_omap_t        r);
static void
feed_omap_get_vals_by_keys_action(rd_action_omap_get_vals_by_keys_t a,
                                  rd_response_omap_t                r);

/**
 * "free" functions
 * Frees a response object.
 */
typedef void (*free_response_fn)(rd_response_base_t);

static void free_resp_omap(rd_response_omap_t a)
{
    omap_iter_free(a->iter);
    free(a);
};

static build_matching_fn match_fn[]
    = {NULL,
       (build_matching_fn)build_matching_stat,
       (build_matching_fn)build_matching_read,
       (build_matching_fn)build_matching_omap_get_keys,
       (build_matching_fn)build_matching_omap_get_vals,
       (build_matching_fn)build_matching_omap_get_vals_by_keys};

static feed_action_fn feed_fn[]
    = {NULL,
       (feed_action_fn)feed_stat_action,
       (feed_action_fn)feed_read_action,
       (feed_action_fn)feed_omap_get_keys_action,
       (feed_action_fn)feed_omap_get_vals_action,
       (feed_action_fn)feed_omap_get_vals_by_keys_action};

static free_response_fn free_fn[]
    = {NULL, (free_response_fn)free, (free_response_fn)free,
       (free_response_fn)free_resp_omap};

read_response_t build_matching_read_responses(mobject_store_read_op_t read_op)
{
    rd_action_base_t   a;
    rd_response_base_t r = NULL;

    read_response_t result = (read_response_t)calloc(1, sizeof(*result));

    DL_FOREACH(read_op->actions, a)
    {
        r = match_fn[a->type](a);
        DL_APPEND(result->responses, r);
        result->num_responses += 1;
    }

    return result;
}

void free_read_responses(read_response_t resp)
{
    rd_response_base_t r, tmp;
    DL_FOREACH_SAFE(resp->responses, r, tmp)
    {
        DL_DELETE(resp->responses, r);
        free_fn[r->type](r);
    }
    free(resp);
}

void feed_read_op_pointers_from_response(mobject_store_read_op_t read_op,
                                         read_response_t         response)
{
    MOBJECT_ASSERT((read_op->num_actions == response->num_responses),
                   "Number of responses received doesn't match number of "
                   "operations in read_op");

    rd_action_base_t   a;
    rd_response_base_t r = response->responses;

    DL_FOREACH(read_op->actions, a)
    {
        feed_fn[a->type](a, r);
        r = r->next;
    }
}

rd_response_base_t build_matching_stat(rd_action_stat_t a)
{
    rd_response_stat_t resp = (rd_response_stat_t)calloc(1, sizeof(*resp));
    resp->base.type         = READ_RESPCODE_STAT;
    a->psize                = &(resp->psize);
    a->pmtime               = &(resp->pmtime);
    a->prval                = &(resp->prval);
    return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_read(rd_action_read_t a)
{
    rd_response_read_t resp = (rd_response_read_t)calloc(1, sizeof(*resp));
    resp->base.type         = READ_RESPCODE_READ;
    a->bytes_read           = &(resp->bytes_read);
    a->prval                = &(resp->prval);
    return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_omap_get_keys(rd_action_omap_get_keys_t a)
{
    rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
    resp->base.type         = READ_RESPCODE_OMAP;
    a->prval                = &(resp->prval);
    a->iter                 = &(resp->iter);
    return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_omap_get_vals(rd_action_omap_get_vals_t a)
{
    rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
    resp->base.type         = READ_RESPCODE_OMAP;
    a->prval                = &(resp->prval);
    a->iter                 = &(resp->iter);
    return (rd_response_base_t)resp;
}

rd_response_base_t
build_matching_omap_get_vals_by_keys(rd_action_omap_get_vals_by_keys_t a)
{
    rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
    resp->base.type         = READ_RESPCODE_OMAP;
    a->prval                = &(resp->prval);
    a->iter                 = &(resp->iter);
    return (rd_response_base_t)resp;
}

void feed_stat_action(rd_action_stat_t a, rd_response_stat_t r)
{
    MOBJECT_ASSERT(r->base.type == READ_RESPCODE_STAT,
                   "Response type does not match the input action");
    if (a->psize) *(a->psize) = r->psize;
    if (a->pmtime) *(a->pmtime) = r->pmtime;
    if (a->prval) *(a->prval) = r->prval;
}

void feed_read_action(rd_action_read_t a, rd_response_read_t r)
{
    MOBJECT_ASSERT(r->base.type == READ_RESPCODE_READ,
                   "Response type does not match the input action");
    if (a->bytes_read) *(a->bytes_read) = r->bytes_read;
    if (a->prval) *(a->prval) = r->prval;
}

void feed_omap_get_keys_action(rd_action_omap_get_keys_t a,
                               rd_response_omap_t        r)
{
    MOBJECT_ASSERT(r->base.type == READ_RESPCODE_OMAP,
                   "Response type does not match the input action");
    if (a->prval) *(a->prval) = r->prval;
    if (a->iter) {
        *(a->iter) = r->iter;
        omap_iter_incr_ref(r->iter);
    }
}

void feed_omap_get_vals_action(rd_action_omap_get_vals_t a,
                               rd_response_omap_t        r)
{
    MOBJECT_ASSERT(r->base.type == READ_RESPCODE_OMAP,
                   "Response type does not match the input action");
    if (a->prval) *(a->prval) = r->prval;
    if (a->iter) {
        *(a->iter) = r->iter;
        omap_iter_incr_ref(r->iter);
    }
}

void feed_omap_get_vals_by_keys_action(rd_action_omap_get_vals_by_keys_t a,
                                       rd_response_omap_t                r)
{
    MOBJECT_ASSERT(r->base.type == READ_RESPCODE_OMAP,
                   "Response type does not match the input action");
    if (a->prval) *(a->prval) = r->prval;
    if (a->iter) {
        *(a->iter) = r->iter;
        omap_iter_incr_ref(r->iter);
    }
}
