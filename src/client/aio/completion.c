/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/client/aio/completion.h"
#include "src/rpc-types/read-op.h"
#include "src/rpc-types/write-op.h"
#include "src/util/log.h"

int mobject_store_aio_create_completion(void*                       cb_arg,
                                        mobject_store_callback_t    cb_complete,
                                        mobject_store_callback_t    cb_safe,
                                        mobject_store_completion_t* pc)
{
    mobject_store_completion_t completion
        = (mobject_store_completion_t)calloc(1, sizeof(*completion));
    MOBJECT_ASSERT(completion != 0,
                   "Could not allocate mobject_store_completion_t object");
    completion->request     = MOBJECT_REQUEST_NULL;
    completion->cb_complete = cb_complete;
    completion->cb_safe     = cb_safe;
    completion->cb_arg      = cb_arg;
    *pc                     = completion;
    return 0;
}

int mobject_store_aio_wait_for_complete(mobject_store_completion_t c)
{
    if (c == MOBJECT_COMPLETION_NULL) { return -1; }

    MOBJECT_ASSERT(c->request != MOBJECT_REQUEST_NULL,
                   "Invalid completion handle");
    int ret;
    int r = mobject_aio_wait(c->request, &ret);
    if (r != 0) return (r);

    c->ret_value = ret;
    c->request   = MOBJECT_REQUEST_NULL;

    if (c->cb_safe) (c->cb_safe)(c, c->cb_arg);

    if (c->cb_complete) (c->cb_complete)(c, c->cb_arg);

    return 0;
}

int mobject_store_aio_is_complete(mobject_store_completion_t c)
{
    if (c == MOBJECT_COMPLETION_NULL) { return 1; }

    if (c->request == MOBJECT_REQUEST_NULL) { return 1; }

    int flag;
    mobject_aio_test(c->request, &flag);

    return flag;
}

int mobject_store_aio_get_return_value(mobject_store_completion_t c)
{
    if (c == MOBJECT_COMPLETION_NULL) {
        MOBJECT_LOG(
            "Warning: passing NULL to mobject_store_aio_get_return_value");
        return -1;
    }
    return c->ret_value;
}

void mobject_store_aio_release(mobject_store_completion_t c)
{
    if (c == MOBJECT_COMPLETION_NULL) return;
    MOBJECT_ASSERT(c->request == MOBJECT_REQUEST_NULL,
                   "Trying to release a completion handle before operation "
                   "completed (will lead to memory leaks)");
    free(c);
}
