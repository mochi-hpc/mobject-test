/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include "mobject-store-config.h"

#include <stdio.h>
#include <assert.h>

#ifdef USE_LIBRADOS
#include <librados.h>
#else
#include <libmobject-store.h>
#endif

int main(int argc, char *argv[])
{
    rados_t cluster;
    int ret;

    ret = rados_create(&cluster, "admin");
    assert(ret == 0);

    rados_shutdown(cluster);

    return(0);
}
