/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <stdio.h>
#include "libmobject-store.h"
#include "src/omap-iter/omap-iter-impl.h"

int mobject_store_omap_get_next(mobject_store_omap_iter_t iter,
                                char**                    key,
                                char**                    val,
                                size_t*                   len)
{
    if (iter->current == NULL) {
        *key = NULL;
        *val = NULL;
        *len = 0;
        return -1;
    }

    *key          = iter->current->key;
    *val          = iter->current->value;
    *len          = iter->current->value_size;
    iter->current = iter->current->next;
    if (iter->current == iter->head) iter->current = NULL;

    return 0;
}

void mobject_store_omap_get_end(mobject_store_omap_iter_t iter)
{
    omap_iter_free(iter);
}
