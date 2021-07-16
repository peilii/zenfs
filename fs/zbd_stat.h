// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class ZoneFileStat {
 public:
  uint64_t file_id;
  uint64_t size_in_zone;
  std::string filename;
};

class ZoneStat {
 public:
  uint64_t total_capacity;
  uint64_t write_position;
  uint64_t start_position;
  std::vector<ZoneFileStat> files;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
