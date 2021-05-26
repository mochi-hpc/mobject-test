/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <margo.h>
#include "src/omap-iter/proc-omap-iter.h"
#include "src/util/utlist.h"

hg_return_t hg_proc_mobject_store_omap_iter_t(hg_proc_t                  proc,
                                              mobject_store_omap_iter_t* iter)
{
    omap_iter_node_t node;
    hg_return_t      ret = HG_SUCCESS;

    switch (hg_proc_get_op(proc)) {

    case HG_ENCODE:

        if (*iter == NULL) {
            hg_size_t s = 0;
            ret         = hg_proc_hg_size_t(proc, &s);
            if (ret != HG_SUCCESS) return ret;
            break;
        }

        ret = hg_proc_hg_size_t(proc, &((*iter)->num_items));
        if (ret != HG_SUCCESS) return ret;

        DL_FOREACH((*iter)->head, node)
        {
            ret = hg_proc_hg_size_t(proc, &(node->key_size));
            if (ret != HG_SUCCESS) return ret;
            if (node->key_size) {
                ret = hg_proc_memcpy(proc, node->key, node->key_size);
                if (ret != HG_SUCCESS) return ret;
            }
            ret = hg_proc_hg_size_t(proc, &(node->value_size));
            if (ret != HG_SUCCESS) return ret;
            if (node->value_size) {
                ret = hg_proc_memcpy(proc, node->value, node->value_size);
                if (ret != HG_SUCCESS) return ret;
            }
        }

        break;

    case HG_DECODE:

        omap_iter_create(iter);
        size_t num_items = 0;
        ret              = hg_proc_hg_size_t(proc, &num_items);
        if (ret != HG_SUCCESS) return ret;

        unsigned i;
        size_t   key_size, val_size;
        char*    key;
        char*    val;
        for (i = 0; i < num_items; i++) {
            ret = hg_proc_hg_size_t(proc, &key_size);
            if (key_size != 0) {
                key = (char*)calloc(1, key_size);
                ret = hg_proc_memcpy(proc, key, key_size);
                if (ret != HG_SUCCESS) return ret;
            } else {
                key = NULL;
            }
            ret = hg_proc_hg_size_t(proc, &val_size);
            if (ret != HG_SUCCESS) return ret;
            if (val_size != 0) {
                val = (char*)calloc(1, val_size);
                ret = hg_proc_memcpy(proc, val, val_size);
            } else {
                val = NULL;
            }
            omap_iter_append(*iter, key, val, val_size);
            if (key) free(key);
            if (val) free(val);
        }

        break;

    case HG_FREE:

        omap_iter_free(*iter);

        break;
    }
    return HG_SUCCESS;
}
