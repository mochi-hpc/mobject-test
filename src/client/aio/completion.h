/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_COMPLETION_H
#define __MOBJECT_COMPLETION_H

#include <margo.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"

/**
 * The mobject_store_completion object is used for asynchronous
 * functions. It contains the callbacks to call when the data is
 * safe and when the operation has completed, as well as potential
 * user data and required mechanism to be able to block on the
 * completion object.
 * mobject_store_completion* is typedef-ed as mobject_store_completion_t
 * in libmobject-store.h.
 */
struct mobject_store_completion {
    mobject_store_callback_t cb_complete; // completion callback
    mobject_store_callback_t cb_safe;     // safe callback
    void*                    cb_arg;      // arguments for callbacks
    mobject_request_t        request;     // margo request to wait on
    int                      ret_value;   // return value of the operation
};

#endif
