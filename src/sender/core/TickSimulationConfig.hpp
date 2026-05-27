// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Configuration and per-thread descriptor structs for TickSimulation.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

#include "../schedule/SchedulerTimelineTrace.hpp"

namespace chronon::sender {

/**
 * Configuration for tick-based simulation.
 *
 * Placement (initialization-time) vs. scheduling (runtime) are orthogonal.
 * Placement groups units into clusters; scheduling decides how those clusters
 * advance in time. `cluster.size() == 1` is the fallback for un-clustered
 * topologies — the scheduler treats the two uniformly.
 */
struct TickSimulationConfig {
    size_t num_threads = std::thread::hardware_concurrency();

    /// Scheduler selection:
    /// - !enable_parallel                      → Sequential (single-thread)
    /// - enable_parallel && !enable_lookahead  → Barrier    (per-cycle sync)
    /// - enable_parallel &&  enable_lookahead  → Lookahead  (per-cluster atomics)
    bool enable_parallel = true;
    bool enable_lookahead = true;

    uint32_t max_lookahead_cycles = 100;
    uint64_t epoch_size = 64;  ///< Cycles per epoch before sync.

    uint64_t tick_frequency_hz = 1'000'000'000;  ///< 1 GHz default.

    bool trace_execution = false;
    SchedulerTimelineTraceConfig timeline_trace;

    bool enable_weighted_partitioning = true;

    bool enable_dynamic_rebalance = true;
    double rebalance_imbalance_threshold = 1.3;
    uint64_t rebalance_check_interval_cycles = 8192;
    double rebalance_min_gain = 0.05;  ///< Skip rebalance if predicted gain below this fraction.
    uint64_t rebalance_cooldown_cycles =
        0;  ///< Minimum cycles between rebalances (0 disables cooldown).

    enum class PartitionSolverType { Weighted, SA };
    PartitionSolverType partition_solver = PartitionSolverType::Weighted;

    /// SA objective critical-path term weight applied to max per-thread
    /// chain cost (ns). 0 disables the term (default).
    double sa_critical_path_weight = 0.0;
};

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
