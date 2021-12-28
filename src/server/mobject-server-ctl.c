/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <margo.h>
#include <mercury_proc_string.h>
#include <ssg.h>

static void usage(void)
{
    fprintf(stderr, "Usage: mobject-server-ctl <cluster_file> <operation>\n");
    fprintf(
        stderr,
        "  <cluster_file>   Mobject cluster ID file to issue commands to\n");
    fprintf(stderr,
            "  <operation>      Mobject server operation to perform (stat, "
            "clean, shutdown)\n");
    exit(-1);
}

static void
parse_args(int argc, char** argv, char** server_gid_file, char** server_op)
{
    if (argc != 3) usage();
    *server_gid_file = argv[1];
    *server_op       = argv[2];

    return;
}

#define send_mobject_server_shutdown margo_shutdown_remote_instance
int send_mobject_server_clean(margo_instance_id mid, hg_addr_t server_addr);
int send_mobject_server_stat(margo_instance_id mid, hg_addr_t server_addr);

hg_id_t mobject_server_clean_rpc_id;
hg_id_t mobject_server_stat_rpc_id;

int main(int argc, char* argv[])
{
    char*             server_gid_file;
    char*             server_op;
    char*             server_addr_str;
    ssg_group_id_t    server_gid;
    int               num_addrs;
    margo_instance_id mid;
    char              proto[24] = {0};
    int               group_size;
    int (*send_op_ptr)(margo_instance_id, hg_addr_t);
    int             i;
    ssg_member_id_t server_id;
    hg_addr_t       server_addr;
    int             ret;

    parse_args(argc, argv, &server_gid_file, &server_op);

    if (strcmp(server_op, "shutdown") == 0)
        send_op_ptr = send_mobject_server_shutdown;
    else if (strcmp(server_op, "clean") == 0)
        send_op_ptr = send_mobject_server_clean;
    else if (strcmp(server_op, "stat") == 0)
        send_op_ptr = send_mobject_server_stat;
    else {
        fprintf(stderr, "Error: Invalid server control operation: %s\n",
                server_op);
        return -1;
    }

    /* SSG initialization */
    /* NOTE: initialized before Margo so we can pull relevant SSG info out of
     * GID file */
    ret = ssg_init();
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "Error: Unable to initialize SSG\n");
        return (-1);
    }

    num_addrs = 1;
    ret       = ssg_group_id_load(server_gid_file, &num_addrs, &server_gid);
    if (ret != 0 || num_addrs < 1) {
        fprintf(stderr, "Error: Unable to load mobject server group ID\n");
        ssg_finalize();
        return -1;
    }

    ret = ssg_get_group_member_addr_str(server_gid, 0, &server_addr_str);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr,
                "Error: Unable to get mobject server group address string\n");
        ssg_finalize();
        return -1;
    }

    /* we only need to get the proto portion of the address to initialize margo
     */
    for (i = 0;
         i < 24 && server_addr_str[i] != '\0' && server_addr_str[i] != ':'; i++)
        proto[i] = server_addr_str[i];

    mid = margo_init(proto, MARGO_SERVER_MODE, 0, -1);
    if (mid == MARGO_INSTANCE_NULL) {
        fprintf(stderr, "Error: Unable to initialize margo\n");
        ssg_finalize();
        free(server_addr_str);
        return (-1);
    }

    /* register RPC handlers for controlling mobject server groups */
    mobject_server_clean_rpc_id
        = MARGO_REGISTER(mid, "mobject_server_clean", void, void, NULL);
    mobject_server_stat_rpc_id
        = MARGO_REGISTER(mid, "mobject_server_stat", void, void, NULL);

    /* observe server group to get all server addresses */
    ret = ssg_group_refresh(mid, server_gid);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "Error: Unable to observe SSG server group\n");
        margo_finalize(mid);
        ssg_finalize();
        free(server_addr_str);
        return (-1);
    }

    ret = ssg_get_group_size(server_gid, &group_size);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "Error: Unable to determine SSG server group size\n");
        ssg_group_destroy(server_gid);
        margo_finalize(mid);
        ssg_finalize();
        free(server_addr_str);
        return (-1);
    }

    for (i = 0; i < group_size; i++) {
        ssg_get_group_member_id_from_rank(server_gid, i, &server_id);
        ssg_get_group_member_addr(server_gid, server_id, &server_addr);
        if (server_addr == HG_ADDR_NULL) {
            fprintf(stderr, "Error: NULL address given for group member %d\n",
                    i);
            ssg_group_destroy(server_gid);
            margo_finalize(mid);
            ssg_finalize();
            free(server_addr_str);
            return (-1);
        }

        ret = send_op_ptr(mid, server_addr);
    }

    ssg_group_destroy(server_gid);
    margo_finalize(mid);
    ssg_finalize();
    free(server_addr_str);

    return 0;
}

int send_mobject_server_clean(margo_instance_id mid, hg_addr_t server_addr)
{
    hg_handle_t handle;
    hg_return_t hret;

    hret = margo_create(mid, server_addr, mobject_server_clean_rpc_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: Unable to create Mercury handle\n");
        return -1;
    }

    /* XXX hard-coded to mobject server provider id of 1 */
    hret = margo_provider_forward(1, handle, NULL);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        fprintf(stderr, "Error: Unable to forward server clean RPC\n");
        return -1;
    }

    margo_destroy(handle);
    return 0;
}

int send_mobject_server_stat(margo_instance_id mid, hg_addr_t server_addr)
{
    hg_handle_t handle;
    hg_return_t hret;

    hret = margo_create(mid, server_addr, mobject_server_stat_rpc_id, &handle);
    if (hret != HG_SUCCESS) {
        fprintf(stderr, "Error: Unable to create Mercury handle\n");
        return -1;
    }

    /* XXX hard-coded to mobject server provider id of 1 */
    hret = margo_provider_forward(1, handle, NULL);
    if (hret != HG_SUCCESS) {
        margo_destroy(handle);
        fprintf(stderr, "Error: Unable to forward server stat RPC\n");
        return -1;
    }

    margo_destroy(handle);
    return 0;
}
