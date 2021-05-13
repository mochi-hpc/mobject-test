/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <map>
#include <string>
#include <iostream>
#include "src/server/visitor-args.h"
#include "src/server/fake/fake-object.hpp"
#include "src/io-chain/write-op-visitor.h"

extern std::map<std::string, fake_object> fake_db;

static void write_op_exec_begin(void*);
static void write_op_exec_end(void*);
static void write_op_exec_create(void*, int);
static void write_op_exec_write(void*, buffer_u, size_t, uint64_t);
static void write_op_exec_write_full(void*, buffer_u, size_t);
static void write_op_exec_writesame(void*, buffer_u, size_t, size_t, uint64_t);
static void write_op_exec_append(void*, buffer_u, size_t);
static void write_op_exec_remove(void*);
static void write_op_exec_truncate(void*, uint64_t);
static void write_op_exec_zero(void*, uint64_t, uint64_t);
static void write_op_exec_omap_set(
    void*, char const* const*, char const* const*, const size_t*, size_t);
static void write_op_exec_omap_rm_keys(void*, char const* const*, size_t);

static struct write_op_visitor write_op_exec
    = {.visit_begin        = write_op_exec_begin,
       .visit_create       = write_op_exec_create,
       .visit_write        = write_op_exec_write,
       .visit_write_full   = write_op_exec_write_full,
       .visit_writesame    = write_op_exec_writesame,
       .visit_append       = write_op_exec_append,
       .visit_remove       = write_op_exec_remove,
       .visit_truncate     = write_op_exec_truncate,
       .visit_zero         = write_op_exec_zero,
       .visit_omap_set     = write_op_exec_omap_set,
       .visit_omap_rm_keys = write_op_exec_omap_rm_keys,
       .visit_end          = write_op_exec_end};

extern "C" void fake_write_op(mobject_store_write_op_t write_op,
                              server_visitor_args_t    vargs)
{
    /* Execute the operation chain */
    execute_write_op_visitor(&write_op_exec, write_op, (void*)vargs);
}

void write_op_exec_begin(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_end(void* u)
{
    auto vargs = static_cast<server_visitor_args_t>(u);
}

void write_op_exec_create(void* u, int exclusive)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) != 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (create) Object " << name
                  << " already exists" << std::endl;
        return;
    }
    fake_db[name] = fake_object();
}

void write_op_exec_write(void* u, buffer_u buf, size_t len, uint64_t offset)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (write) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    margo_instance_id mid = vargs->srv_ctx->mid;
    fake_db[name].write(mid, vargs->client_addr, vargs->bulk_handle,
                        buf.as_offset, offset, len);
}

void write_op_exec_write_full(void* u, buffer_u buf, size_t len)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (write_full) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    margo_instance_id mid = vargs->srv_ctx->mid;
    fake_db[name].write_full(mid, vargs->client_addr, vargs->bulk_handle,
                             buf.as_offset, len);
}

void write_op_exec_writesame(
    void* u, buffer_u buf, size_t data_len, size_t write_len, uint64_t offset)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (writesame) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    margo_instance_id mid = vargs->srv_ctx->mid;
    fake_db[name].writesame(mid, vargs->client_addr, vargs->bulk_handle,
                            buf.as_offset, offset, data_len, write_len);
}

void write_op_exec_append(void* u, buffer_u buf, size_t len)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (append) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    margo_instance_id mid = vargs->srv_ctx->mid;
    fake_db[name].append(mid, vargs->client_addr, vargs->bulk_handle,
                         buf.as_offset, len);
}

void write_op_exec_remove(void* u)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (remove) Object " << name
                  << " did not exist" << std::endl;
        return;
    }
    fake_db.erase(name);
}

void write_op_exec_truncate(void* u, uint64_t offset)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (truncate) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    fake_db[name].truncate(offset);
}

void write_op_exec_zero(void* u, uint64_t offset, uint64_t len)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (zero) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    fake_db[name].zero(offset, len);
}

void write_op_exec_omap_set(void*              u,
                            char const* const* keys,
                            char const* const* vals,
                            const size_t*      lens,
                            size_t             num)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (omap_set) Object " << name
                  << " does not exist, it will be created" << std::endl;
    }
    unsigned i;
    auto&    entry = fake_db[name];
    for (i = 0; i < num; i++) { entry.omap_set(keys[i], lens[i], vals[i]); }
}

void write_op_exec_omap_rm_keys(void*              u,
                                char const* const* keys,
                                size_t             num_keys)
{
    auto        vargs = static_cast<server_visitor_args_t>(u);
    std::string name(vargs->object_name);
    if (fake_db.count(name) == 0) {
        std::cerr << "[FAKE-BACKEND-WARNING] (omap_rm_keys) Object " << name
                  << " did not exist" << std::endl;
        return;
    }
    unsigned i;
    auto&    entry = fake_db[name];
    for (i = 0; i < num_keys; i++) { entry.omap_rm(keys[i]); }
}
