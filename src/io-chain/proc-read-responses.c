/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <mercury_proc.h>
#include "src/io-chain/proc-read-responses.h"
#include "src/io-chain/read-responses.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/proc-omap-iter.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

typedef hg_return_t (*encode_fn)(hg_proc_t, void*);
typedef hg_return_t (*decode_fn)(hg_proc_t, void*);

static hg_return_t encode_stat_response(hg_proc_t proc, rd_response_stat_t r);
static hg_return_t decode_stat_response(hg_proc_t proc, rd_response_stat_t* r);
static hg_return_t encode_read_response(hg_proc_t proc, rd_response_read_t r);
static hg_return_t decode_read_response(hg_proc_t proc, rd_response_read_t* r);
static hg_return_t encode_omap_response(hg_proc_t proc, rd_response_omap_t r);
static hg_return_t decode_omap_response(hg_proc_t proc, rd_response_omap_t* r);

static encode_fn encode[]
    = {NULL, (encode_fn)encode_stat_response, (encode_fn)encode_read_response,
       (encode_fn)encode_omap_response};

static decode_fn decode[]
    = {NULL, (decode_fn)decode_stat_response, (decode_fn)decode_read_response,
       (decode_fn)decode_omap_response};

hg_return_t hg_proc_read_response_t(hg_proc_t proc, read_response_t* response)
{
    hg_return_t        ret = HG_SUCCESS;
    rd_response_base_t elem;

    switch (hg_proc_get_op(proc)) {

    case HG_ENCODE:
        // encode the number of actions
        ret = hg_proc_memcpy(proc, &((*response)->num_responses),
                             sizeof((*response)->num_responses));
        if (ret != HG_SUCCESS) return ret;

        DL_FOREACH((*response)->responses, elem)
        {
            read_resp_code_t opcode = elem->type;
            MOBJECT_ASSERT((opcode > 0 || opcode < _READ_RESPCODE_END_ENUM_),
                           "Invalid response code");
            // encode the type of response
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            // encode the response's arguments
            ret = encode[opcode](proc, elem);
            if (ret != HG_SUCCESS) return ret;
        }

        break;
    case HG_DECODE:
        // allocate the response chain
        *response = (read_response_t)calloc(1, sizeof(**response));
        // decode the number of responses in the chain
        ret = hg_proc_memcpy(proc, &((*response)->num_responses),
                             sizeof((*response)->num_responses));
        if (ret != HG_SUCCESS) return ret;

        size_t             i;
        rd_response_base_t next_resp;
        for (i = 0; i < (*response)->num_responses; i++) {
            // decode the current response's type
            read_resp_code_t opcode;
            ret = hg_proc_memcpy(proc, &opcode, sizeof(opcode));
            if (ret != HG_SUCCESS) return ret;
            MOBJECT_ASSERT((opcode > 0 || opcode < _READ_RESPCODE_END_ENUM_),
                           "Invalid write_op opcode");
            // decode the response's arguments
            ret = decode[opcode](proc, &next_resp);
            if (ret != HG_SUCCESS) return ret;
            next_resp->type = opcode;
            // append to the list
            DL_APPEND((*response)->responses, next_resp);
        }

        break;
    case HG_FREE:

        free_read_responses(*response);
        break;
    }

    return ret;
}

hg_return_t encode_stat_response(hg_proc_t proc, rd_response_stat_t r)
{
    hg_return_t ret;
    ret = hg_proc_uint64_t(proc, &(r->psize));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &(r->pmtime), sizeof(r->pmtime));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &(r->prval), sizeof(r->prval));
    return ret;
}

hg_return_t decode_stat_response(hg_proc_t proc, rd_response_stat_t* r)
{
    *r = (rd_response_stat_t)calloc(1, sizeof(**r));

    hg_return_t ret;
    ret = hg_proc_uint64_t(proc, &((*r)->psize));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &((*r)->pmtime), sizeof((*r)->pmtime));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &((*r)->prval), sizeof((*r)->prval));
    return ret;
}

hg_return_t encode_read_response(hg_proc_t proc, rd_response_read_t r)
{
    hg_return_t ret;
    ret = hg_proc_hg_size_t(proc, &(r->bytes_read));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &(r->prval), sizeof(r->prval));
    return ret;
}

hg_return_t decode_read_response(hg_proc_t proc, rd_response_read_t* r)
{
    *r = (rd_response_read_t)calloc(1, sizeof(**r));

    hg_return_t ret;
    ret = hg_proc_hg_size_t(proc, &((*r)->bytes_read));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_memcpy(proc, &((*r)->prval), sizeof((*r)->prval));
    return ret;
}

hg_return_t encode_omap_response(hg_proc_t proc, rd_response_omap_t r)
{
    hg_return_t ret;
    ret = hg_proc_memcpy(proc, &(r->prval), sizeof(r->prval));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_mobject_store_omap_iter_t(proc, &(r->iter));
    return ret;
}

hg_return_t decode_omap_response(hg_proc_t proc, rd_response_omap_t* r)
{
    *r = (rd_response_omap_t)calloc(1, sizeof(**r));

    hg_return_t ret = HG_SUCCESS;
    ret             = hg_proc_memcpy(proc, &((*r)->prval), sizeof((*r)->prval));
    if (ret != HG_SUCCESS) return ret;
    ret = hg_proc_mobject_store_omap_iter_t(proc, &((*r)->iter));
    return ret;
}
