// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/options.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "fs/fs.h"
#include "storage/lake/location_provider.h"
#include "storage/olap_define.h"
#include "util/uid_util.h"

namespace starrocks {

class MemTracker;

struct StorePath {
    StorePath() = default;
    explicit StorePath(std::string path_) : path(std::move(path_)) {}
    std::string path;
    TStorageMedium::type storage_medium{TStorageMedium::HDD};
};

// parse a single root path of storage_root_path
Status parse_root_path(const std::string& root_path, StorePath* path);

Status parse_conf_store_paths(const std::string& config_path, std::vector<StorePath>* path,
                              std::string_view configvar_name = "config::storage_root_path");

Status parse_conf_datacache_paths(const std::string& config_path, std::vector<std::string>* paths);

struct EngineOptions {
    // list paths that tablet will be put into.
    std::vector<StorePath> store_paths;
    // BE's UUID. It will be reset every time BE restarts.
    UniqueId backend_uid{0, 0};
    MemTracker* compaction_mem_tracker = nullptr;
    MemTracker* update_mem_tracker = nullptr;
    // if start as cn, no need to write cluster id
    bool need_write_cluster_id = true;
};

// Options only applies to cloud-native table r/w IO
struct LakeIOOptions {
    // Cache remote file locally on read requests.
    // This options can be ignored if the underlying filesystem does not support local cache.
    bool fill_data_cache = false;

    bool skip_disk_cache = false;
    // Specify different buffer size for different read scenarios
    int64_t buffer_size = -1;
    bool fill_metadata_cache = false;
    bool use_page_cache = false;
    bool cache_file_only = false; // only used for CACHE SELECT
    std::shared_ptr<FileSystem> fs;
    std::shared_ptr<starrocks::lake::LocationProvider> location_provider;
};

} // namespace starrocks
