// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <vector>

namespace chronon::sender {

/** @brief Input data for the weighted partitioner: per-unit cost and per-unit adjacency. */
struct PartitionInput {
    size_t num_units;
    size_t num_threads;
    std::vector<double> unit_cost_ns;  ///< Tick cost per unit (ns).
    double sync_cost_ns;               ///< Per-dep cross-thread sync cost (ns).

    /// Weight on max per-thread critical-path chain cost in the SA objective; 0 disables.
    double critical_path_weight = 0.0;

    struct EdgeInfo {
        size_t neighbor;
        size_t num_connections;
        uint32_t min_delay;
    };
    std::vector<std::vector<EdgeInfo>> adjacency;
};

/** @brief Optional diagnostics from a partition solver — populated by SA, defaults from Weighted.
 */
struct PartitionStats {
    double sa_objective = 0.0;
    bool lpt_fallback = false;  ///< SA rejected by balance gate; fell back to LPT.
    size_t active_threads = 0;
    double compute_imbalance_ratio = 1.0;  ///< max_compute / avg_compute (compute-only).
    std::vector<double> per_thread_compute_ns;
};

/** @brief Partition output: unit→thread assignment plus cost diagnostics. */
struct PartitionResult {
    std::vector<size_t> unit_to_thread;
    std::vector<std::vector<size_t>> thread_units;
    double estimated_max_thread_time_ns;
    double imbalance_ratio;
    size_t cross_thread_connections;
    PartitionStats stats;
};

/// Pluggable partition-solver signature; WeightedPartitioner::partition and SA both match.
using PartitionSolver = PartitionResult (*)(const PartitionInput&);

/// Shared helpers used by both WeightedPartitioner and SimulatedAnnealingPartitioner.
namespace partition_utils {

/// delay=0 edges must not cross threads; return a very high factor to force co-location.
inline double delayFactor(uint32_t min_delay) noexcept {
    if (min_delay == 0) return 100.0;
    return 1.0 / static_cast<double>(min_delay);
}

/// Compute per-thread execution time under the partitioner cost model.
inline void computeThreadTimes(const PartitionInput& input, const std::vector<size_t>& assignment,
                               std::vector<double>& thread_times) {
    std::fill(thread_times.begin(), thread_times.end(), 0.0);

    for (size_t u = 0; u < input.num_units; ++u) {
        thread_times[assignment[u]] += input.unit_cost_ns[u];
    }

    // Sync cost scales by 1/delay: delay=1 edges cause tight spin-waiting while
    // higher-delay edges rarely stall the consumer. Adjacency is directed
    // source -> consumer, so charge cross-thread waits to the consumer thread.
    for (size_t u = 0; u < input.num_units; ++u) {
        size_t src_thread = assignment[u];
        for (const auto& edge : input.adjacency[u]) {
            size_t dst_thread = assignment[edge.neighbor];
            if (src_thread != dst_thread) {
                double df = delayFactor(edge.min_delay);
                thread_times[dst_thread] +=
                    input.sync_cost_ns * static_cast<double>(edge.num_connections) * df;
            }
        }
    }
}

/// Count directed cross-thread connections.
inline size_t countCrossThreadConnections(const PartitionInput& input,
                                          const std::vector<size_t>& assignment) {
    size_t count = 0;
    for (size_t u = 0; u < input.num_units; ++u) {
        for (const auto& edge : input.adjacency[u]) {
            if (assignment[u] != assignment[edge.neighbor]) {
                count += edge.num_connections;
            }
        }
    }
    return count;
}

/// LPT (Longest Processing Time) initial placement — classic 4/3-OPT makespan heuristic.
inline std::vector<size_t> initialLPT(const PartitionInput& input, size_t num_threads) {
    std::vector<size_t> assignment(input.num_units, 0);

    std::vector<size_t> order(input.num_units);
    std::iota(order.begin(), order.end(), 0);
    // Total order: heaviest first, ties broken by unit index. The index tie-break
    // makes the result independent of the standard library's std::sort (which is
    // unstable) — without it, uniform-cost inputs sort equal elements in an
    // implementation-defined order and the partition diverges across stdlibs.
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        if (input.unit_cost_ns[a] != input.unit_cost_ns[b]) {
            return input.unit_cost_ns[a] > input.unit_cost_ns[b];
        }
        return a < b;
    });

    std::vector<double> thread_load(num_threads, 0.0);

    for (size_t idx : order) {
        size_t best_thread = 0;
        double min_load = thread_load[0];
        for (size_t t = 1; t < num_threads; ++t) {
            if (thread_load[t] < min_load) {
                min_load = thread_load[t];
                best_thread = t;
            }
        }

        assignment[idx] = best_thread;
        thread_load[best_thread] += input.unit_cost_ns[idx];
    }

    return assignment;
}

/// Build the final PartitionResult from a unit-to-thread assignment.
inline PartitionResult buildResult(const PartitionInput& input,
                                   const std::vector<size_t>& assignment, size_t num_threads) {
    PartitionResult result;
    result.unit_to_thread = assignment;
    result.thread_units.resize(num_threads);

    for (size_t u = 0; u < input.num_units; ++u) {
        result.thread_units[assignment[u]].push_back(u);
    }

    std::vector<double> thread_times(num_threads, 0.0);
    computeThreadTimes(input, assignment, thread_times);

    result.estimated_max_thread_time_ns =
        *std::max_element(thread_times.begin(), thread_times.end());

    double avg_time = 0.0;
    size_t active_threads = 0;
    for (double t : thread_times) {
        if (t > 0.0) {
            avg_time += t;
            active_threads++;
        }
    }
    avg_time = (active_threads > 0) ? avg_time / static_cast<double>(active_threads) : 1.0;
    result.imbalance_ratio = result.estimated_max_thread_time_ns / avg_time;

    result.cross_thread_connections = countCrossThreadConnections(input, assignment);

    return result;
}

}  // namespace partition_utils
}  // namespace chronon::sender
