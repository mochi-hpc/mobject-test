/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef MOBJECT_SERVER_H
#define MOBJECT_SERVER_H

#include <margo.h>
#include <bake-client.h>
#include <yokan/provider-handle.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOBJECT_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct mobject_provider* mobject_provider_t;

struct mobject_provider_init_args {
    const char* json_config;
    ABT_pool    pool;
};

/**
 * Start a mobject server instance
 *
 * @param[in] mid           margo instance id
 * @param[in] provider_id   id of the provider
 * @param[in] bake_ph       Bake provider handle to use to write/read data
 * @param[in] yokan_ph      Yokan provider handle to use to access metadata
 *                          providers
 * @param[in] args          Optional arguments
 * @param[out] provider     resulting provider
 *
 * @returns 0 on success, negative error code on failure
 */
int mobject_provider_register(margo_instance_id                  mid,
                              uint16_t                           provider_id,
                              bake_provider_handle_t             bake_ph,
                              yk_provider_handle_t               yokan_ph,
                              struct mobject_provider_init_args* args,
                              mobject_provider_t*                provider);

#ifdef __cplusplus
}
#endif

#endif
