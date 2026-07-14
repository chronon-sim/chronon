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
    /// Deprecated compatibility knob for the per-epoch lookahead fallback.
    /// Epoch-free lookahead ignores this value; it will be removed or ignored
    /// once the fallback scheduler is removed.
    uint64_t epoch_size = 64;

    /// Epoch-free lookahead: in the persistent-worker lookahead path, collapse
    /// the per-epoch std::barrier into a single
    /// run-spanning window. Run-ahead is then bounded by lookahead_floor_ +
    /// max_lookahead_cycles plus per-edge queue-headroom dependencies, with
    /// per-connection MPSC arbitration and a single MPSC flush at run end.
    /// Engaged by runParallel() only when the persistent path is eligible,
    /// max_lookahead_cycles is positive, and every MPSC port has fully resolved
    /// per-connection progress; otherwise it falls back to the barrier path.
    /// Results are identical to the barrier path — it trades per-epoch straggler
    /// idle for lookahead-window slack.
    ///
    /// Setting this to false selects the deprecated per-epoch lookahead
    /// fallback. That fallback is retained for compatibility and will be
    /// removed in a future release.
    bool enable_epoch_free_lookahead = true;

    uint64_t tick_frequency_hz = 1'000'000'000;  ///< 1 GHz default.

    bool trace_execution = false;
    SchedulerTimelineTraceConfig timeline_trace;

    /// Enables the cluster-aware partitioning path. When false, Chronon falls
    /// back to the legacy topology-only thread assignment.
    bool enable_weighted_partitioning = true;

    bool enable_dynamic_rebalance = true;
    double rebalance_imbalance_threshold = 1.03;
    uint64_t rebalance_check_interval_cycles = 2048;
    double rebalance_min_gain = 0.01;  ///< Skip rebalance if predicted gain below this fraction.
    uint64_t rebalance_cooldown_cycles =
        0;  ///< Minimum cycles between rebalances (0 disables cooldown).

    /// Initial partition solver used by the cluster-aware path. SA is the
    /// default; Weighted keeps the deterministic four-phase solver available.
    enum class PartitionSolverType { Weighted, SA };
    PartitionSolverType partition_solver = PartitionSolverType::SA;

    /// SA objective critical-path term weight applied to max per-thread
    /// chain cost (ns). 0 disables the term (default).
    double sa_critical_path_weight = 0.0;

    /// Fixed, wall-clock-free sync cost (ns) fed ONLY to the initial deterministic
    /// partition so the solver minimizes cross-thread edge cut, co-locating
    /// topologically-connected units (e.g. a CPU core's pipeline) on one thread.
    /// Scaled to dominate the uniform unit cost (1.0) — empirically ~8x is the
    /// sweet spot: large enough to pull connected components together, small
    /// enough that the compute-balance signal still steers the SA solver (much
    /// higher swamps it and convergence degrades). 0 restores the old
    /// pure-load-balance behavior.
    ///
    /// This knob is scoped to the initial partition: it is not written into
    /// platform_metrics_, so dynamic rebalance decides migrations on its own
    /// inputs (measured unit costs + platform_metrics_) and is unaffected by it.
    double initial_partition_sync_cost_ns = 8.0;
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
