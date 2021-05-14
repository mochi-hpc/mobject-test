/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef MOBJECT_SERVER_H
#define MOBJECT_SERVER_H

#include <margo.h>
#include <ssg.h>
#include <bake-client.h>
#include <sdskv-client.h>
#include <sdskv-server.h>
/* server-side utilities and routines.  Clients are looking for either
 * libmobject-store.h or librados-mobject-store.h */
#ifdef __cplusplus
extern "C" {
#endif

#define MOBJECT_SERVER_GROUP_NAME "mobject-store-servers"
#define MOBJECT_ABT_POOL_DEFAULT  ABT_POOL_NULL

typedef struct mobject_provider* mobject_provider_t;

/**
 * Start a mobject server instance
 *
 * @param[in] mid           margo instance id
 * @param[in] provider_id   id of the provider
 * @param[in] pool          Argobots pool for the provider
 * @param[in] bake_ph       Bake provider handle to use to write/read data
 * @param[in] sdskv_ph      SDSKV provider handle to use to access metadata
 * @param[in] gid           SSG group id of the group gathering all mobject
 * providers
 * @param[in] cluster_file  file name to write cluster connect info to
 * @param[out] provider     resulting provider
 *
 * @returns 0 on success, negative error code on failure
 */
int mobject_provider_register(margo_instance_id       mid,
                              uint16_t                provider_id,
                              ABT_pool                pool,
                              bake_provider_handle_t  bake_ph,
                              sdskv_provider_handle_t sdskv_ph,
                              ssg_group_id_t          gid,
                              const char*             cluster_file,
                              mobject_provider_t*     provider);

#ifdef __cplusplus
}
#endif

#endif
