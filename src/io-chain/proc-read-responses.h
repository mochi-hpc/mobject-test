/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_PROC_READ_RESPONSE_H
#define __MOBJECT_PROC_READ_RESPONSE_H

#include <margo.h>
#include "src/io-chain/read-resp-impl.h"

hg_return_t hg_proc_read_response_t(hg_proc_t proc, read_response_t* response);

#endif
