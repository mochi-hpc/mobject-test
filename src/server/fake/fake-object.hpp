/*
 * (C) 2018 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __FAKE_OBJECT_HPP
#define __FAKE_OBJECT_HPP

#include <iostream>
#include <cstring>
#include <vector>
#include <string>
#include <margo.h>
#include <libmobject-store.h>
#include "src/omap-iter/omap-iter-impl.h"

class fake_object {

  private:
    std::vector<char> m_data; // data associated with the object
    std::map<std::string, std::vector<char>> m_omap;          // omap
    time_t                                   m_creation_time; // creation time
    time_t m_modification_time; // modification time

  public:
    fake_object()
    {
        time(&m_creation_time);
        m_modification_time = m_creation_time;
    }

    void omap_set(const std::string& key, size_t len, const char* val)
    {
        m_omap[key] = std::vector<char>(val, val + len);
    }

    void omap_rm(const std::string& key) { m_omap.erase(key); }

    void omap_get_keys(const std::string&        start_after,
                       size_t                    max,
                       mobject_store_omap_iter_t iter) const
    {
        auto it = m_omap.lower_bound(start_after);
        if (it == m_omap.end()) return;
        if (it->first == start_after) it++;
        for (size_t i = 0; (it != m_omap.end()) && (i < max); it++, i++) {
            auto& key = it->first;
            omap_iter_append(iter, &key[0], (const char*)0, 0);
        }
    }

    void omap_get_vals(const std::string&        start_after,
                       const std::string&        filter_prefix,
                       size_t                    max,
                       mobject_store_omap_iter_t iter) const
    {
        auto it = m_omap.lower_bound(start_after);
        if (it == m_omap.end()) return;
        if (it->first == start_after) it++;
        for (size_t i = 0; (it != m_omap.end()) && (i < max); it++, i++) {
            auto& key = it->first;
            auto& val = it->second;
            if (key.substr(0, filter_prefix.size()) != filter_prefix) continue;
            omap_iter_append(iter, &key[0], &val[0], val.size());
        }
    }

    void omap_get_vals_by_keys(const std::vector<std::string>& keys,
                               mobject_store_omap_iter_t       iter) const
    {
        for (auto& k : keys) {
            if (m_omap.count(k) != 0) {
                auto        it  = m_omap.find(k);
                const auto& val = it->second;
                omap_iter_append(iter, &k[0], &val[0], val.size());
            }
        }
    }

    void write(margo_instance_id mid,
               hg_addr_t         client_addr,
               hg_bulk_t         bulk_handle,
               uint64_t          remote_offset,
               uint64_t          local_offset,
               size_t            len)
    {

        if (local_offset + len > m_data.size())
            m_data.resize(local_offset + len);

        std::vector<void*>     buf_ptrs(1);
        std::vector<hg_size_t> buf_sizes(1);
        buf_ptrs[0]  = (void*)(&m_data[local_offset]);
        buf_sizes[0] = len;

        hg_bulk_t local_bulk;

        hg_return_t ret
            = margo_bulk_create(mid, 1, buf_ptrs.data(), buf_sizes.data(),
                                HG_BULK_WRITE_ONLY, &local_bulk);
        // TODO error checking
        ret = margo_bulk_transfer(mid, HG_BULK_PULL, client_addr, bulk_handle,
                                  remote_offset, local_bulk, 0, len);
        ret = margo_bulk_free(local_bulk);

        time(&m_modification_time);
    }

    void write_full(margo_instance_id mid,
                    hg_addr_t         client_addr,
                    hg_bulk_t         bulk_handle,
                    uint64_t          remote_offset,
                    size_t            len)
    {

        m_data.resize(len);
        write(mid, client_addr, bulk_handle, remote_offset, 0, len);
    }

    void writesame(margo_instance_id mid,
                   hg_addr_t         client_addr,
                   hg_bulk_t         bulk_handle,
                   uint64_t          remote_offset,
                   uint64_t          local_offset,
                   size_t            data_len,
                   size_t            write_len)
    {

        if (write_len < data_len) { data_len = write_len; }
        uint64_t base_offset = local_offset;
        write(mid, client_addr, bulk_handle, remote_offset, local_offset,
              data_len);
        write_len -= data_len;
        local_offset += data_len;
        if (local_offset + write_len > m_data.size()) {
            m_data.resize(local_offset + write_len);
        }

        while (write_len > 0) {
            if (write_len < data_len) data_len = write_len;
            std::memcpy((void*)(&m_data[local_offset]),
                        (void*)(&m_data[base_offset]), data_len);
            local_offset += data_len;
            write_len -= data_len;
        }
    }

    void append(margo_instance_id mid,
                hg_addr_t         client_addr,
                hg_bulk_t         bulk_handle,
                uint64_t          remote_offset,
                size_t            len)
    {

        uint64_t local_offset = m_data.size();
        write(mid, client_addr, bulk_handle, remote_offset, local_offset, len);
    }

    void truncate(uint64_t offset)
    {
        m_data.resize(offset);
        time(&m_modification_time);
    }

    void zero(uint64_t offset, size_t size)
    {
        if (offset + size > m_data.size()) { m_data.resize(offset + size); }
        std::memset((void*)(&m_data[offset]), 0, size);
        time(&m_modification_time);
    }

    void stat(size_t* psize, time_t* pmtime) const
    {
        *psize  = m_data.size();
        *pmtime = m_modification_time;
    }

    void read(margo_instance_id mid,
              hg_addr_t         client_addr,
              hg_bulk_t         remote_handle,
              uint64_t          remote_offset,
              uint64_t          local_offset,
              size_t            len,
              size_t*           bytes_read) const
    {

        if (local_offset > m_data.size()) {
            *bytes_read = 0;
            return;
        }

        // Figure out what we can read
        if (local_offset + len > m_data.size())
            len = m_data.size() - local_offset;

        std::vector<void*>     buf_ptrs(1);
        std::vector<hg_size_t> buf_sizes(1);
        buf_ptrs[0]  = (void*)(&m_data[local_offset]);
        buf_sizes[0] = len;

        hg_bulk_t local_bulk;

        hg_return_t ret
            = margo_bulk_create(mid, 1, buf_ptrs.data(), buf_sizes.data(),
                                HG_BULK_READ_ONLY, &local_bulk);
        // TODO error checking
        ret = margo_bulk_transfer(mid, HG_BULK_PUSH, client_addr, remote_handle,
                                  remote_offset, local_bulk, 0, len);
        ret = margo_bulk_free(local_bulk);

        *bytes_read = len;
    }
};

#endif
