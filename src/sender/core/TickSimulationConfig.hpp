// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Model configuration for TickSimulation; runtime descriptors are kept separately.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

#include "../schedule/SchedulerTimelineTrace.hpp"

namespace chronon::sender {

/// Auto selects epoch-free execution when safe and useful, otherwise sequential.
enum class ExecutionPolicy { Auto, Sequential };

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
    /// - !enable_parallel                         → Sequential
    /// - all epoch-free safety gates are satisfied → Epoch-free lookahead
    /// - otherwise                                → Sequential fallback
    /// @deprecated Prefer setExecutionPolicy(). Retained for source compatibility.
    bool enable_parallel = true;
    /// Compatibility switch. False now forces Sequential; the removed
    /// per-cycle barrier scheduler is no longer selectable.
    /// @deprecated Prefer setExecutionPolicy().
    bool enable_lookahead = true;

    /// In explicit clock mode, bounds the rolling physical-time batch window.
    uint32_t max_lookahead_cycles = 100;
    /// Host predicate and Sequential termination polling interval. Epoch-free
    /// runUntilTermination ignores this value because stop propagates directly.
    /// @deprecated Prefer setPollingIntervalCycles().
    uint64_t epoch_size = 64;

    /// Epoch-free lookahead uses one persistent-worker, run-spanning window.
    /// Run-ahead is bounded by lookahead_floor_ +
    /// max_lookahead_cycles plus per-edge queue-headroom dependencies. MPSC
    /// lanes require neither centralized scheduler arbitration nor a run-end
    /// flush. Bounded destinations perform aggregate admission on their owning
    /// Unit immediately before its tick.
    /// Engaged only when max_lookahead_cycles is positive and every MPSC port
    /// has fully resolved per-connection progress and queue headroom. A failed
    /// safety gate selects Sequential; no barrier-based fallback remains.
    /// Setting this compatibility switch to false also forces Sequential.
    /// @deprecated Prefer setExecutionPolicy().
    bool enable_epoch_free_lookahead = true;

    uint64_t tick_frequency_hz = 1'000'000'000;  ///< 1 GHz default.

    /// Sample multi-clock scheduler components every 64 sweeps (no speculative ticks).
    bool profile_clock_scheduler = false;

    bool trace_execution = false;
    SchedulerTimelineTraceConfig timeline_trace;

    /// Enables the cluster-aware partitioning path. When false, Chronon falls
    /// back to the legacy topology-only thread assignment.
    bool enable_weighted_partitioning = true;

    /// Reuses actor-local safe points for single-clock and explicit-clock graphs.
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

    /// Canonical configuration API. The booleans above are compatibility fields.
    /// This reports the requested policy, not the resolved execution backend.
    ExecutionPolicy executionPolicy() const noexcept {
        return enable_parallel && enable_lookahead && enable_epoch_free_lookahead
                   ? ExecutionPolicy::Auto
                   : ExecutionPolicy::Sequential;
    }
    void setExecutionPolicy(ExecutionPolicy policy) noexcept {
        enable_parallel = policy == ExecutionPolicy::Auto;
        enable_lookahead = true;
        enable_epoch_free_lookahead = true;
    }
    uint64_t pollingIntervalCycles() const noexcept { return epoch_size; }
    void setPollingIntervalCycles(uint64_t cycles) noexcept { epoch_size = cycles; }
};

}  // namespace chronon::sender
