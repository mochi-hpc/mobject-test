/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MOBJECT_OMAP_ITER_H
#define __MOBJECT_OMAP_ITER_H

#include "libmobject-store.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct omap_iter_node* omap_iter_node_t;

struct omap_iter_node {
    char*            key;
    char*            value;
    size_t           key_size;
    size_t           value_size;
    omap_iter_node_t prev, next;
};

struct mobject_store_omap_iter {
    size_t           num_items;
    size_t           ref_count;
    omap_iter_node_t head;
    omap_iter_node_t current;
};

void omap_iter_create(mobject_store_omap_iter_t* iter);

void omap_iter_incr_ref(mobject_store_omap_iter_t iter);

void omap_iter_free(mobject_store_omap_iter_t iter);

void omap_iter_append(mobject_store_omap_iter_t iter,
                      const char*               key,
                      const char*               val,
                      size_t                    val_size);

#ifdef __cplusplus
}
#endif

#endif
