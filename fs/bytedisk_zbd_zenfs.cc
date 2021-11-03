// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#if defined(ZENFS_BYTEDISK)

#include "bytedisk_zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <ctime>
#include <iostream>
#include <map>
#include <tuple>

#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "utilities/trace/bytedance_metrics_reporter.h"

#define KB (1024)
#define MB (1024 * KB)

#define ZENFS_DEBUG
#include "utils.h"

/* Number of reserved zones for op log
 * Two non-offline op log zones are needed to be able
 * to roll the log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_OP_LOG_ZONES (2)

/* Number of reserved zones for metadata snapshot
 */
#define ZENFS_SNAPSHOT_ZONES (2)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

/* Default capacity of namespace 512 MB */
#ifndef ZENFS_NAMESPACE_CAP
#define ZENFS_NAMESPACE_CAP (512 * 1024 * 1024)
#endif

#define ZENFS_TIMEOUT (1)

namespace ROCKSDB_NAMESPACE {

BytediskNamespace::BytediskNamespace(ZonedBlockDevice* zbd, 
                    bytedisk_ns_handle_t bytedisk_ns) :
                    zbd_(zbd), ns_(bytedisk_ns),
                    start_(bytedisk_get_ns_start_off(ns_)),
                    wp_(bytedisk_get_ns_writepointer(ns_)),
                    max_capacity_(ZENFS_NAMESPACE_CAP),
                    used_capacity_(0),
                    capacity_(max_capacity_ - (wp_ - start_)),
                    lifetime_(Env::WLTH_NOT_SET) {
    cb_data_ = (struct bytedisk_io_cb_data *) malloc(sizeof(* cb_data_));
    memset(cb_data_, 0, sizeof(*cb_data_));

    cb_data_->type = 1;
    cb_data_->thread_index = std::this_thread::get_id();
    cb_data_->offset = wp_;
    cb_data_->io_size = 0;
    cb_data_->inflight = 0;

    std::ostream json_stream;
    EncodeJson(json_stream);
    std::cout << json_stream;
}

IOStatus BytediskNamespace::Append(char *data, uint32_t size) {
    IOStatus s;
    ssize_t ret_size;
    
    if (capacity_ < size)
        return IOStatus::NoSpace("Not enough capacity for append");

    assert((size % zbd_->GetBlockSize()) == 0);

    ret_size = bytedisk_sync_write(ns_, wp_, data, size);

    if (ret_size < 0)
        return IOStatus::IOError("Write failed");
    
    wp_ += ret_size;
    capacity_ -= ret_size;

    return IOStatus::OK();
}

IOStatus BytediskNamespace::Sync() {
    int cnt = 0;

    if (cb_data->inflight == 0) return IOStatus::OK();
    
    while (cb_data->completed_size != cb_data->inflight && cnt < ZENFS_TIMEOUT) {
        // TODO:for now, we make sure only sleep for 1 second to wait io finish
        // in the future, we can set io priority to exceed the sync process
        cnt++;
        usleep(0.1);
    }

    if (cb_data_->completed_size != cb_data_->inflight) {
        fprintf(stderr, "Failed to complete io - timeout\n");
        return IOStatus::IOError("Failed to complete io - timeout?");
    }

    cb_data_->inflight = 0;
    return IOStatus::OK();
}

IOStatus BytediskNamespace::AppendAsync(char *data, uint32_t size) {
    bytedisk_io_handle_t io_handler;
    uint64_t completed_size = 0;
    IOStatus s;

    assert((size % zbd_->GetBlockSize()) == 0);

    s = Sync();
    if (! s.ok()) return s;

    if (size > capacity_) return IOStatus::NoSpace("Not enough capacity for append");

    cb_data_->thread_index = std::this_thread::get_id();
    cb_data_->offset = wp_;
    cb_data_->io_size = size;
    cb_data_->completed_size = &completed_size;
    cb_data_->inflight = size;

    io_handler = bytedisk_async_write(ns_, wp_, data, size, bytedisk_io_cb, cb_data_);
    if (io_handler == 0) {
        return IOStatus::IOError("Write failed");
    }

    wp_ += size;
    capacity_ -= size;

    return IOStatus::OK();
}

IOStatus BytediskNamespace::Reset() {
    bytedisk_ns_handle_t handler;

    handler = bytedisk_reset_namespace(ns_);
    if (handler == 0) return IOStatus::IOError("Namespace reset failed\n");

    ns_ = handler;
    capacity_ = bytedisk_get_ns_length(ns_);
    max_capacity_ = bytedisk_get_ns_length(ns_);
    wp_ = start_;
    lifetime_ = Env::WLTH_NOT_SET;

    return IOStatus::OK();
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger) :
                                   filename_("/dev/" + bdevname), logger_(logger) {
    Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
}

ZonedBlockDevice::~ZonedBlockDevice() {
    for (const auto& n : op_namespaces_) {
        delete n;
    }

    for (const auto& n : snapshot_namespaces_) {
        delete n;
    }

    for (const auto& n : io_namespaces_) {
        delete n;
    }

    bytedisk_close_dev(dev_);
}

IOStatus ZonedBlockDevice::Open() {
    dev_ = bytedisk_open_dev(filename_);

    block_sz_ = bytedisk_get_block_size(dev_);
    zone_sz_ = bytedisk_get_dev_zone_cap(dev_);
    nr_zones_ = bytedisk_get_dev_zone_cnt(dev_);
    max_nr_active_zones_ = bytedisk_get_dev_active_zone(dev_);

    // allocate io_namespaces, op_namespaces, and snapshot_namespaces
    InitNamespaces();

    return IOStatus::OK();
}

IOStatus ZonedBlockDevice::InitNamespaces() {
    size_t total_namespaces = 0;
    size_t offset = 0;
    bytedisk_ns_handle_t ns;

    total_namespaces = zone_sz_ * nr_zones_ / ZENFS_NAMESPACE_CAP;

    for (int i = 0; i < total_namespaces; i++) {
        ns = bytedisk_allocate_namespace(dev_, offset, ZENFS_NAMESPACE_CAP);
        if (ns == 0) {
            return IOStatus::IOError("Failed to allocate namespaces.");
        }
        if (i < ZENFS_OP_LOG_ZONES) {
            op_namespaces_.push_back(new BytediskNamespace(this, ns));
        } else if (i < ZENFS_OP_LOG_ZONES + ZENFS_SNAPSHOT_ZONES) {
            snapshot_namespaces_.push_back(new BytediskNamespace(this, ns));
        } else {
            io_namespaces_.push_back(new BytediskNamespace(this, ns));
        }
        offset += ZENFS_NAMESPACE_CAP;
    }

    start_time_ = time(NULL);
    return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
    uint64_t free = 0;
    for (const auto& n : io_namespaces_) {
        free += n->capacity_;
    }
    return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
    uint64_t used = 0;
    for (const auto& n : io_namespaces_) {
        used += n->used_capacity_;
    }
    return used;
}

bytedisk_ns_handle_t ZonedBlockDevice::GetNamespaceUseNsid(uint64_t nsid) {
    return bytedisk_get_dev_namespace(dev_, nsid);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // defined(ZENFS_BYTEDISK)

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
