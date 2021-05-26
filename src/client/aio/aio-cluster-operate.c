/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include <ssg.h>

#include "mobject-store-config.h"
#include "libmobject-store.h"
#include "src/client/cluster.h"
#include "src/client/aio/completion.h"
#include "src/util/log.h"

int mobject_store_aio_write_op_operate(mobject_store_write_op_t   write_op,
                                       mobject_store_ioctx_t      io,
                                       mobject_store_completion_t completion,
                                       const char*                oid,
                                       time_t*                    mtime,
                                       int                        flags)
{
    // XXX pick other servers using ch-placement
    ssg_member_id_t svr_id;
    ssg_get_group_member_id_from_rank(io->cluster->gid, 0, &svr_id);
    hg_addr_t svr_addr;
    ssg_get_group_member_addr(io->cluster->gid, svr_id, &svr_addr);

    mobject_provider_handle_t mph;
    mobject_provider_handle_create(io->cluster->mobject_clt, svr_addr, 1, &mph);
    mobject_request_t req;
    mobject_aio_write_op_operate(mph, write_op, io->pool_name, oid, mtime,
                                 flags, &req);
    mobject_provider_handle_release(mph);

    completion->request = req;

    return 0;
}

int mobject_store_aio_read_op_operate(mobject_store_read_op_t    read_op,
                                      mobject_store_ioctx_t      io,
                                      mobject_store_completion_t completion,
                                      const char*                oid,
                                      int                        flags)
{
    // XXX pick other servers using ch-placement
    ssg_member_id_t svr_id;
    ssg_get_group_member_id_from_rank(io->cluster->gid, 0, &svr_id);
    hg_addr_t svr_addr;
    ssg_get_group_member_addr(io->cluster->gid, svr_id, &svr_addr);

    mobject_provider_handle_t mph;
    mobject_provider_handle_create(io->cluster->mobject_clt, svr_addr, 1, &mph);
    mobject_request_t req;
    mobject_aio_read_op_operate(mph, read_op, io->pool_name, oid, flags, &req);
    mobject_provider_handle_release(mph);

    completion->request = req;

    return 0;
}
