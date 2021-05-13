/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include "src/server/printer/print-write-op.h"
#include "src/io-chain/write-op-visitor.h"

static void write_op_printer_begin(void*);
static void write_op_printer_end(void*);
static void write_op_printer_create(void*, int);
static void write_op_printer_write(void*, buffer_u, size_t, uint64_t);
static void write_op_printer_write_full(void*, buffer_u, size_t);
static void
write_op_printer_writesame(void*, buffer_u, size_t, size_t, uint64_t);
static void write_op_printer_append(void*, buffer_u, size_t);
static void write_op_printer_remove(void*);
static void write_op_printer_truncate(void*, uint64_t);
static void write_op_printer_zero(void*, uint64_t, uint64_t);
static void write_op_printer_omap_set(
    void*, char const* const*, char const* const*, const size_t*, size_t);
static void write_op_printer_omap_rm_keys(void*, char const* const*, size_t);

struct write_op_visitor write_op_printer
    = {.visit_begin        = write_op_printer_begin,
       .visit_create       = write_op_printer_create,
       .visit_write        = write_op_printer_write,
       .visit_write_full   = write_op_printer_write_full,
       .visit_writesame    = write_op_printer_writesame,
       .visit_append       = write_op_printer_append,
       .visit_remove       = write_op_printer_remove,
       .visit_truncate     = write_op_printer_truncate,
       .visit_zero         = write_op_printer_zero,
       .visit_omap_set     = write_op_printer_omap_set,
       .visit_omap_rm_keys = write_op_printer_omap_rm_keys,
       .visit_end          = write_op_printer_end};

void print_write_op(mobject_store_write_op_t write_op, const char* object_name)
{
    /* Execute the operation chain */
    execute_write_op_visitor(&write_op_printer, write_op, (void*)object_name);
}

void write_op_printer_begin(void* object_name)
{
    printf("<mobject_write_operation on=\"%s\">\n", (char*)object_name);
}

void write_op_printer_end(void* unused)
{
    printf("</mobject_write_operation>\n");
}

void write_op_printer_create(void* unused, int exclusive)
{
    printf("\t<create exclusive=%d />\n", exclusive);
}

void write_op_printer_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
    printf("\t<write from=%ld length=%ld offset=%ld />\n", buf.as_offset, len,
           offset);
}

void write_op_printer_write_full(void* u, buffer_u buf, size_t len)
{
    printf("\t<write_full from=%ld length=%ld />\n", buf.as_offset, len);
}

void write_op_printer_writesame(
    void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
    printf("\t<writesame from=%ld data_len=%ld write_len=%ld offset=%ld />\n",
           buf.as_offset, data_len, write_len, offset);
}

void write_op_printer_append(void* u, buffer_u buf, size_t len)
{
    printf("\t<append from=%ld length=%ld />\n", buf.as_offset, len);
}

void write_op_printer_remove(void* u) { printf("\t<remove />\n"); }

void write_op_printer_truncate(void* u, uint64_t offset)
{
    printf("\t<truncate offset=%ld />\n", offset);
}

void write_op_printer_zero(void* u, uint64_t offset, uint64_t len)
{
    printf("\t<zero offset=%ld len=%ld />\n", offset, len);
}

void write_op_printer_omap_set(void*              u,
                               char const* const* keys,
                               char const* const* vals,
                               const size_t*      lens,
                               size_t             num)
{
    printf("\t<omap_set num=%ld>\n", num);
    unsigned i;
    for (i = 0; i < num; i++) {
        printf("\t\t<record key=\"%s\"\t lens=%ld>%s</record>\n", keys[i],
               lens[i], vals[i]);
    }
    printf("\t</omap_set>\n");
}

void write_op_printer_omap_rm_keys(void*              u,
                                   char const* const* keys,
                                   size_t             num_keys)
{
    printf("\t<omap_rm_keys num=%ld>\n", num_keys);
    unsigned i;
    for (i = 0; i < num_keys; i++) {
        printf("\t\t<record key=\"%s\" />\n", keys[i]);
    }
    printf("\t</omap_rm_keys>\n");
}
