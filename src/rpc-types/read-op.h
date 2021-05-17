/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RPC_TYPE_READ_OP_H
#define __RPC_TYPE_READ_OP_H

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>
#include <libmobject-store.h>
#include "src/io-chain/proc-write-actions.h"
#include "src/io-chain/proc-read-actions.h"
#include "src/io-chain/proc-read-responses.h"

MERCURY_GEN_PROC(
    read_op_in_t,
    ((hg_const_string_t)(client_addr))((hg_const_string_t)(pool_name))(
        (hg_const_string_t)(object_name))((mobject_store_read_op_t)(read_op)))

MERCURY_GEN_PROC(read_op_out_t, ((read_response_t)(responses)))

#endif
