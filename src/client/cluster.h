/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __CLUSTER_H
#define __CLUSTER_H

#include <margo.h>
#include <ssg.h>
#include <ch-placement.h>

#include "libmobject-store.h"
#include "mobject-client.h"

#define MOBJECT_CLUSTER_FILE_ENV          "MOBJECT_CLUSTER_FILE"
#define MOBJECT_CLUSTER_SHUTDOWN_KILL_ENV "MOBJECT_SHUTDOWN_KILL_SERVERS"

typedef struct ch_placement_instance* chi_t;

struct mobject_store_handle {
    margo_instance_id mid;
    mobject_client_t  mobject_clt;
    ssg_group_id_t    gid;
    chi_t             ch_instance;
    int               connected;
};

struct mobject_store_ioctx {
    mobject_store_t cluster;
    char*           pool_name;
};

#endif
