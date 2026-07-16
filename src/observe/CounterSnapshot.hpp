// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace chronon::observe {

/// COUNTER_SNAPSHOT record flag: payload is one metadata-indexed value batch.
inline constexpr uint8_t COUNTER_SNAPSHOT_BATCH_FLAG = 1u << 1;

/**
 * Fixed prefix for a batched counter snapshot. It is followed by @c count
 * uint64_t values whose names are registered once through
 * CounterSnapshotPlanMetadata.
 */
struct CounterSnapshotBatchHeader {
    uint64_t cycle = 0;
    uint32_t plan_id = 0;
    uint32_t count = 0;
};

static_assert(sizeof(CounterSnapshotBatchHeader) == 16);

struct CounterSnapshotEntryMetadata {
    std::string unit_name;
    std::string counter_name;
};

struct CounterSnapshotPlanMetadata {
    std::vector<CounterSnapshotEntryMetadata> entries;
};

}  // namespace chronon::observe
