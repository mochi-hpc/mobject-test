/*
 * (C) 2017 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef MOBJECT_SERVER_H
#define MOBJECT_SERVER_H

#include <margo.h>
/* server-side utilities and routines.  Clients are looking for either
 * libmobject-store.h or librados-mobject-store.h */

/**
 * Start a mobject server instance
 *
 * @param[in] mid
 * @param[in poolname
 * @returns 0 on success, negative error code on failure */
int mobject_server_register(margo_instance_id mid, const char *poolname);


#endif