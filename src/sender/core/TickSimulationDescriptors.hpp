// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

// Internal runtime descriptors; model configuration lives in TickSimulationConfig.hpp.
namespace chronon::sender {

/**
 * Per-cluster progress counter for dependency-driven sync.
 *
 * Each thread publishes its completed cycle count to a cache-line-aligned
 * atomic; peers read only their predecessors', avoiding centralized barrier
 * contention.
 */
struct alignas(64) ThreadProgress {
    std::atomic<uint64_t> completed_cycle{0};
};

/**
 * Cross-thread dependency descriptor: thread T at cycle C may proceed once
 * pred_thread has completed cycle (C - min_delay).
 */
struct ThreadCrossDep {
    size_t pred_thread;
    uint32_t min_delay;
};

/**
 * Pre-resolved dependency for hot-path access.
 *
 * Stores a direct pointer to the predecessor's progress atomic, removing
 * the vector<unique_ptr> indirection in the spin loop. Thread T at cycle C
 * may run when `*progress_ptr >= C + 1 - min_delay`.
 */
struct ResolvedDep {
    std::atomic<uint64_t>* progress_ptr;
    uint32_t min_delay;
    /// Stable predecessor cluster index, also used as the worker-local cache
    /// index. Real dependencies use [0, num_clusters); the synthetic
    /// lookahead-floor dependency uses the reserved num_clusters slot.
    size_t pred_id = 0;
};

struct BlockedClusterInfo {
    size_t cluster = SIZE_MAX;
    size_t pred_cluster = SIZE_MAX;
    uint64_t needed = 0;
    uint64_t observed = 0;
    uint32_t delay = 0;
    uint64_t deficit = 0;
};

}  // namespace chronon::sender
