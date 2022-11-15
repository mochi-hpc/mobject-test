#include <bedrock/module.h>
#include "mobject-server.h"

static int mobject_register_provider(bedrock_args_t             args,
                                     bedrock_module_provider_t* provider)
{
    margo_instance_id mid         = bedrock_args_get_margo_instance(args);
    uint16_t          provider_id = bedrock_args_get_provider_id(args);
    ABT_pool          pool        = bedrock_args_get_pool(args);
    const char*       config      = bedrock_args_get_config(args);
    const char*       name        = bedrock_args_get_name(args);

    struct mobject_provider_init_args mobject_args = {0};
    mobject_args.pool                              = pool;
    mobject_args.json_config                       = config;

    size_t num_sdskv_ph
        = bedrock_args_get_num_dependencies(args, "yokan_provider_handle");
    size_t num_bake_ph
        = bedrock_args_get_num_dependencies(args, "bake_provider_handle");

    if (num_sdskv_ph != 1) {
        margo_error(
            mid,
            "mobject_register_provider: expected 1 yokan provider handle,"
            " %ld provided",
            num_sdskv_ph);
    }
    if (num_bake_ph != 1) {
        margo_error(
            mid,
            "mobject_register_provider: expected 1 bake provider handle,"
            " %ld provided",
            num_bake_ph);
    }

    yk_provider_handle_t yokan_ph
        = bedrock_args_get_dependency(args, "yokan_provider_handle", 0);

    bake_provider_handle_t bake_ph
        = bedrock_args_get_dependency(args, "bake_provider_handle", 0);

    return mobject_provider_register(mid, provider_id, bake_ph, yokan_ph,
                                     &mobject_args,
                                     (mobject_provider_t*)provider);
}

static int mobject_deregister_provider(bedrock_module_provider_t provider)
{
    // TODO
    return BEDROCK_SUCCESS;
}

static char* mobject_get_provider_config(bedrock_module_provider_t provider)
{
    // TODO
    return strdup("{}");
}

static int mobject_init_client(bedrock_args_t           args,
                               bedrock_module_client_t* client)
{
    // TODO
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    margo_error(mid,
                "Bedrock cannot instantiate a mobject client at the moment");
    return -1;
}

static int mobject_finalize_client(bedrock_module_client_t client)
{
    // TODO
    return -1;
}

static int mobject_create_provider_handle(bedrock_module_client_t client,
                                          hg_addr_t               address,
                                          uint16_t                provider_id,
                                          bedrock_module_provider_handle_t* ph)
{
    // TODO
    return -1;
}

static int mobject_destroy_provider_handle(bedrock_module_provider_handle_t ph)
{
    // TODO
    return -1;
}

static struct bedrock_dependency mobject_provider_deps[3]
    = {{"yokan_provider_handle", "yokan", BEDROCK_REQUIRED},
       {"bake_provider_handle", "bake", BEDROCK_REQUIRED},
       BEDROCK_NO_MORE_DEPENDENCIES};

static struct bedrock_module mobject
    = {.register_provider       = mobject_register_provider,
       .deregister_provider     = mobject_deregister_provider,
       .get_provider_config     = mobject_get_provider_config,
       .init_client             = mobject_init_client,
       .finalize_client         = mobject_finalize_client,
       .create_provider_handle  = mobject_create_provider_handle,
       .destroy_provider_handle = mobject_destroy_provider_handle,
       .client_dependencies     = NULL,
       .provider_dependencies   = mobject_provider_deps};

BEDROCK_REGISTER_MODULE(mobject, mobject);
