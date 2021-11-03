// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#if defined(ZENFS_BYTEDISK)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <libbytedisk.h>
#include <bytedisk.h>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/metrics_reporter.h"
#include "zbd_stat.h"

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;

class BytediskNamespace {
public:
    explicit BytediskNamespace(ZonedBlockDevice* zbd, 
                    bytedisk_ns_handle_t bytedisk_ns);

    IOStatus Append(char *data, uint32_t size);
    IOStatus AppendAsync(char* data, uint32_t size);

    IOStatus Sync();

    IOStatus Reset();

    struct bytedisk_io_cb_data {
        uint32_t type;
        uint32_t thread_index;
        uint64_t offset;
        size_t io_size;
        size_t *completed_size;
        size_t inflight;
    };

    static void bytedisk_io_cb(bytedisk_io_handle_t handler, bool success, void *cb_arg) {
        struct bytedisk_io_cb_data *data = (struct bytedisk_io_cb_data *)cb_arg;

        if (!success) {
            BYTEDISK_ERROR("%s error:%u, %u, expected %lu %llx",
                        data->type ? "write" : "read", data->thread_index,
                        write_num[data->thread_index], data->io_size, handler);
        } else {
            *data->completed_size += data->io_size;
        }
        // free(data); // Don't free cb_data_ here
    }

    void EncodeJson(std::ostream &json_stream) {
        json_stream << "{";
        json_stream << "\"start\":" << start_ << ",";
        json_stream << "\"capacity\":" << capacity_ << ",";
        json_stream << "\"max_capacity\":" << max_capacity_ << ",";
        json_stream << "\"wp\":" << wp_ << ",";
        json_stream << "\"lifetime\":" << lifetime_ << ",";
        json_stream << "\"used_capacity\":" << used_capacity_;
        json_stream << "}";
    }

public:
    ZonedBlockDevice* zbd_;
    bytedisk_ns_handle_t ns_;
    uint64_t start_;
    uint64_t capacity_;
    uint64_t max_capacity_;
    uint64_t used_capacity_;
    uint64_t wp_;
    Env::WriteLifeTimeHint lifetime_;
    struct bytedisk_io_cb_data* cb_data_;

private:
    ZonedBlockDevice *zbd_;
};

class ZonedBlockDevice {
public:
    explicit ZonedBlockDevice(std::string bdevname,
                              std::shared_ptr<Logger> logger);
    
    explicit ZonedBlockDevice(
        std::string bdevname, std::shared_ptr<Logger> logger,
        std::string bytedance_tags,
        std::shared_ptr<MetricReporterFactory> metrics_reporter_factory);
    )

    virtual ~ZonedBlockDevice();

    IOStatus Open(bool readonly = false);

    BytediskNamespace* AllocateNamespace();
    BytediskNamespace* AllocateMetaZone();
    BytediskNamespace* AllocateSnapshotZone();

    IOStatus InitNamespaces(size_t start, size_t size, size_t* num_namespace);

    uint64_t GetFreeSpace();
    uint64_t GetUsedSpace();

    uint32_t GetBlockSize() { return block_sz_; }
    uint64_t GetZoneSize() { return zone_sz_; }
    uint64_t GetNrZones() { return nr_zones_; }
    uint32_t GetMaxActiveZones() { return max_nr_active_io_zones_ + 1; }

    std::vector<BytediskNamespace *> GetOpNamespaces() { return op_namespaces_; }
    std::vector<BytediskNamespace *> GetIoNamespaces() { return io_namespaces_; }
    std::vector<BytediskNamespace *> GetSnapshotNamespaces() { return snapshot_namespaces_; }

    bytedisk_dev_handle_t GetDeviceHandle() { return dev_; }
    bytedisk_ns_handle_t GetNamespaceUseNsid(uint64_t nsid);

private:
    bytedisk_dev_handle_t dev_;

    std::string filename_;
    uint32_t block_sz_;
    uint64_t zone_sz_;
    uint64_t nr_zones_;
    uint64_t max_nr_active_zones_;
    std::vector<BytediskNamespace *> io_namespaces_;
    std::vector<BytediskNamespace *> op_namespaces_;
    std::vector<BytediskNamespace *> snapshot_namespaces_;
    time_t start_time_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // defined(ZENFS_BYTEDISK)

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
