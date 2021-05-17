/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RPC_TYPE_WRITE_OP_H
#define __RPC_TYPE_WRITE_OP_H

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>
#include <libmobject-store.h>
#include "src/io-chain/proc-write-actions.h"
#include "src/io-chain/proc-read-actions.h"
#include "src/io-chain/proc-read-responses.h"

MERCURY_GEN_PROC(
    write_op_in_t,
    ((hg_const_string_t)(client_addr))((hg_const_string_t)(pool_name))(
        (hg_const_string_t)(object_name))((mobject_store_write_op_t)(write_op)))

MERCURY_GEN_PROC(write_op_out_t, ((int32_t)(ret)))

#endif
