/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include "src/io-chain/read-op-visitor.h"
#include "src/io-chain/read-resp-impl.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/server/printer/print-read-op.h"

static void read_op_printer_begin(void*);
static void read_op_printer_stat(void*, uint64_t*, time_t*, int*);
static void
read_op_printer_read(void*, uint64_t, size_t, buffer_u, size_t*, int*);
static void read_op_printer_omap_get_keys(
    void*, const char*, uint64_t, mobject_store_omap_iter_t*, int*);
static void read_op_printer_omap_get_vals(void*,
                                          const char*,
                                          const char*,
                                          uint64_t,
                                          mobject_store_omap_iter_t*,
                                          int*);
static void read_op_printer_omap_get_vals_by_keys(
    void*, char const* const*, size_t, mobject_store_omap_iter_t*, int*);
static void read_op_printer_end(void*);

struct read_op_visitor read_op_printer
    = {.visit_begin                 = read_op_printer_begin,
       .visit_end                   = read_op_printer_end,
       .visit_stat                  = read_op_printer_stat,
       .visit_read                  = read_op_printer_read,
       .visit_omap_get_keys         = read_op_printer_omap_get_keys,
       .visit_omap_get_vals         = read_op_printer_omap_get_vals,
       .visit_omap_get_vals_by_keys = read_op_printer_omap_get_vals_by_keys};

void print_read_op(mobject_store_read_op_t read_op, const char* object_name)
{
    execute_read_op_visitor(&read_op_printer, read_op, (void*)object_name);
}

void read_op_printer_begin(void* u)
{
    printf("<mobject_read_operation object_name=\"%s\">\n", (char*)u);
}

void read_op_printer_stat(void* u, uint64_t* psize, time_t* pmtime, int* prval)
{
    printf("\t<stat/>\n");
    *psize = 128;
    time(pmtime);
    *prval = 1234;
}

void read_op_printer_read(void*    u,
                          uint64_t offset,
                          size_t   len,
                          buffer_u buf,
                          size_t*  bytes_read,
                          int*     prval)
{
    printf("\t<read offset=%ld length=%ld to=%ld/>\n", offset, len,
           buf.as_offset);
    *bytes_read = len;
    *prval      = 1235;
}

void read_op_printer_omap_get_keys(void*                      u,
                                   const char*                start_after,
                                   uint64_t                   max_return,
                                   mobject_store_omap_iter_t* iter,
                                   int*                       prval)
{
    printf("\t<omap_get_keys start_after=\"%s\" max_return=%ld />\n",
           start_after, max_return);
    omap_iter_create(iter);
    // use omap_iter_append to add things to the iterator
    *prval = 1236;
}

void read_op_printer_omap_get_vals(void*                      u,
                                   const char*                start_after,
                                   const char*                filter_prefix,
                                   uint64_t                   max_return,
                                   mobject_store_omap_iter_t* iter,
                                   int*                       prval)
{
    printf(
        "\t<omap_get_vals start_after=\"%s\" filter_prefix=\"%s\" "
        "max_return=%ld />\n",
        start_after, filter_prefix, max_return);

    omap_iter_create(iter);
    *prval = 1237;
}

void read_op_printer_omap_get_vals_by_keys(void*                      u,
                                           char const* const*         keys,
                                           size_t                     num_keys,
                                           mobject_store_omap_iter_t* iter,
                                           int*                       prval)
{
    printf("\t<omap_get_vals_by_keys num=%ld>\n", num_keys);
    unsigned i;
    for (i = 0; i < num_keys; i++)
        printf("\t\t<record key=\"%s\" />\n", keys[i]);
    printf("\t<omap_get_vals_by_keys/>\n");

    omap_iter_create(iter);
    *prval = 1238;
}

void read_op_printer_end(void* u) { printf("</mobject_read_operation>\n"); }
