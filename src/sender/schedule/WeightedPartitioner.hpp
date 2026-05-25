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

/**
 * @brief Cost-aware min-max graph partitioner.
 *
 * Cost model:
 *   thread_time(T) = Σ unit_cost[u] for u ∈ T
 *                  + sync_cost · |cross-thread deps consumed by T|
 *
 * Phases: LPT initial placement → FM refinement (≤5 passes) → pairwise swap →
 * multi-unit relocate from the bottleneck thread.
 */
class WeightedPartitioner {
    /// delay=0 edges must not cross threads → return a very high factor to force co-location.
    static double delayFactor_(uint32_t min_delay) noexcept {
        if (min_delay == 0) return 100.0;
        return 1.0 / static_cast<double>(min_delay);
    }

public:
    static PartitionResult partition(const PartitionInput& input) {
        if (input.num_units == 0 || input.num_threads == 0) {
            return {};
        }

        size_t num_threads = std::min(input.num_threads, input.num_units);

        std::vector<size_t> assignment = initialLPT_(input, num_threads);

        for (int pass = 0; pass < 5; ++pass) {
            bool improved = refinePartition_(input, assignment, num_threads);
            if (!improved) break;
        }

        pairwiseSwap_(input, assignment, num_threads);
        multiUnitRelocate_(input, assignment, num_threads);

        return buildResult_(input, assignment, num_threads);
    }

    /// Returns the maximum thread time (the metric the partitioner minimizes).
    static double evaluatePartition(const PartitionInput& input,
                                    const std::vector<size_t>& assignment) {
        if (input.num_threads == 0) return 0.0;

        size_t num_threads = 0;
        for (size_t t : assignment) {
            num_threads = std::max(num_threads, t + 1);
        }

        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);

        double max_time = 0.0;
        for (double t : thread_times) {
            max_time = std::max(max_time, t);
        }
        return max_time;
    }

    /// Per-thread execution times (ns) under the partitioner's cost model.
    static std::vector<double> evaluatePerThread(const PartitionInput& input,
                                                 const std::vector<size_t>& assignment) {
        if (input.num_threads == 0) return {};

        size_t num_threads = 0;
        for (size_t t : assignment) {
            num_threads = std::max(num_threads, t + 1);
        }

        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);
        return thread_times;
    }

private:
    static void computeThreadTimes_(const PartitionInput& input,
                                    const std::vector<size_t>& assignment,
                                    std::vector<double>& thread_times) {
        std::fill(thread_times.begin(), thread_times.end(), 0.0);

        for (size_t u = 0; u < input.num_units; ++u) {
            thread_times[assignment[u]] += input.unit_cost_ns[u];
        }

        // Sync cost scales by 1/delay: delay=1 edges cause tight spin-waiting while
        // higher-delay edges rarely stall the consumer.
        for (size_t u = 0; u < input.num_units; ++u) {
            size_t dst_thread = assignment[u];
            for (const auto& edge : input.adjacency[u]) {
                size_t src_thread = assignment[edge.neighbor];
                if (src_thread != dst_thread) {
                    double delay_factor = delayFactor_(edge.min_delay);
                    thread_times[dst_thread] += input.sync_cost_ns *
                                                static_cast<double>(edge.num_connections) *
                                                delay_factor;
                }
            }
        }
    }

    static size_t countCrossThreadConnections_(const PartitionInput& input,
                                               const std::vector<size_t>& assignment) {
        size_t count = 0;
        for (size_t u = 0; u < input.num_units; ++u) {
            for (const auto& edge : input.adjacency[u]) {
                if (assignment[u] != assignment[edge.neighbor]) {
                    count += edge.num_connections;
                }
            }
        }
        return count / 2;  // Bidirectional adjacency double-counts each connection.
    }

    /// Phase 1: LPT (Longest Processing Time) — classic 4/3-OPT makespan heuristic.
    static std::vector<size_t> initialLPT_(const PartitionInput& input, size_t num_threads) {
        std::vector<size_t> assignment(input.num_units, 0);

        std::vector<size_t> order(input.num_units);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
            return input.unit_cost_ns[a] > input.unit_cost_ns[b];
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

    /// Phase 2: FM refinement — move units from heaviest to lightest when it reduces max time.
    static bool refinePartition_(const PartitionInput& input, std::vector<size_t>& assignment,
                                 size_t num_threads) {
        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);

        std::vector<bool> locked(input.num_units, false);
        bool any_improvement = false;

        for (size_t iter = 0; iter < input.num_units; ++iter) {
            size_t heaviest = 0, lightest = 0;
            for (size_t t = 1; t < num_threads; ++t) {
                if (thread_times[t] > thread_times[heaviest]) heaviest = t;
                if (thread_times[t] < thread_times[lightest]) lightest = t;
            }

            if (heaviest == lightest) break;

            double best_gain = 0.0;
            size_t best_unit = SIZE_MAX;

            for (size_t u = 0; u < input.num_units; ++u) {
                if (locked[u] || assignment[u] != heaviest) continue;

                double gain =
                    computeMoveGain_(input, assignment, thread_times, u, heaviest, lightest);
                if (gain > best_gain) {
                    best_gain = gain;
                    best_unit = u;
                }
            }

            if (best_unit == SIZE_MAX) break;

            assignment[best_unit] = lightest;
            locked[best_unit] = true;

            computeThreadTimes_(input, assignment, thread_times);
            any_improvement = true;
        }

        return any_improvement;
    }

    /// Gain = (old max thread time) - (new max thread time); positive means improvement.
    static double computeMoveGain_(const PartitionInput& input,
                                   const std::vector<size_t>& assignment,
                                   const std::vector<double>& thread_times, size_t unit_idx,
                                   size_t from_thread, size_t to_thread) {
        double old_max = std::max(thread_times[from_thread], thread_times[to_thread]);

        double from_new = thread_times[from_thread] - input.unit_cost_ns[unit_idx];
        double to_new = thread_times[to_thread] + input.unit_cost_ns[unit_idx];

        for (const auto& edge : input.adjacency[unit_idx]) {
            size_t neighbor_thread = assignment[edge.neighbor];
            double delay_factor = delayFactor_(edge.min_delay);
            double sync =
                input.sync_cost_ns * static_cast<double>(edge.num_connections) * delay_factor;

            if (neighbor_thread == from_thread) {
                // Edge becomes cross-thread on the destination side.
                to_new += sync;
            } else if (neighbor_thread == to_thread) {
                // Edge becomes same-thread; both sides drop the sync cost.
                from_new -= sync;
                to_new -= sync;
            }
        }

        double new_max = std::max(from_new, to_new);
        return old_max - new_max;
    }

    /// Phase 3: pairwise swap across all thread pairs — catches equal-load but uncoupled cases.
    static void pairwiseSwap_(const PartitionInput& input, std::vector<size_t>& assignment,
                              size_t num_threads) {
        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);

        bool improved = true;
        while (improved) {
            improved = false;

            double current_max = *std::max_element(thread_times.begin(), thread_times.end());

            for (size_t ta = 0; ta < num_threads && !improved; ++ta) {
                for (size_t tb = ta + 1; tb < num_threads && !improved; ++tb) {
                    for (size_t u = 0; u < input.num_units && !improved; ++u) {
                        if (assignment[u] != ta) continue;

                        for (size_t v = 0; v < input.num_units; ++v) {
                            if (assignment[v] != tb) continue;

                            assignment[u] = tb;
                            assignment[v] = ta;

                            std::vector<double> new_times(num_threads, 0.0);
                            computeThreadTimes_(input, assignment, new_times);

                            double new_max = *std::max_element(new_times.begin(), new_times.end());

                            if (new_max < current_max - 0.01) {
                                thread_times = new_times;
                                improved = true;
                                break;
                            } else {
                                assignment[u] = ta;
                                assignment[v] = tb;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Phase 4: pull k=2 units off the heaviest thread when no single move helps.
    static void multiUnitRelocate_(const PartitionInput& input, std::vector<size_t>& assignment,
                                   size_t num_threads) {
        if (num_threads < 2) return;

        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);

        bool improved = true;
        while (improved) {
            improved = false;
            double current_max = *std::max_element(thread_times.begin(), thread_times.end());

            size_t heaviest = static_cast<size_t>(std::distance(
                thread_times.begin(), std::max_element(thread_times.begin(), thread_times.end())));

            std::vector<size_t> heavy_units;
            for (size_t u = 0; u < input.num_units; ++u) {
                if (assignment[u] == heaviest) heavy_units.push_back(u);
            }
            if (heavy_units.size() < 2) break;

            std::vector<size_t> targets;
            for (size_t t = 0; t < num_threads; ++t) {
                if (t != heaviest) targets.push_back(t);
            }
            std::sort(targets.begin(), targets.end(),
                      [&](size_t a, size_t b) { return thread_times[a] < thread_times[b]; });
            size_t num_targets = std::min(targets.size(), size_t(2));

            double best_gain = 0.01;
            size_t best_u = SIZE_MAX, best_v = SIZE_MAX;
            size_t best_u_target = 0, best_v_target = 0;

            for (size_t i = 0; i < heavy_units.size(); ++i) {
                for (size_t j = i + 1; j < heavy_units.size(); ++j) {
                    size_t u = heavy_units[i];
                    size_t v = heavy_units[j];

                    for (size_t ti = 0; ti < num_targets; ++ti) {
                        for (size_t tj = 0; tj < num_targets; ++tj) {
                            size_t u_target = targets[ti];
                            size_t v_target = targets[tj];

                            assignment[u] = u_target;
                            assignment[v] = v_target;

                            std::vector<double> new_times(num_threads, 0.0);
                            computeThreadTimes_(input, assignment, new_times);
                            double new_max = *std::max_element(new_times.begin(), new_times.end());

                            double gain = current_max - new_max;
                            if (gain > best_gain) {
                                best_gain = gain;
                                best_u = u;
                                best_v = v;
                                best_u_target = u_target;
                                best_v_target = v_target;
                            }

                            assignment[u] = heaviest;
                            assignment[v] = heaviest;
                        }
                    }
                }
            }

            if (best_u != SIZE_MAX) {
                assignment[best_u] = best_u_target;
                assignment[best_v] = best_v_target;
                computeThreadTimes_(input, assignment, thread_times);
                improved = true;
            }
        }
    }

    static PartitionResult buildResult_(const PartitionInput& input,
                                        const std::vector<size_t>& assignment, size_t num_threads) {
        PartitionResult result;
        result.unit_to_thread = assignment;
        result.thread_units.resize(num_threads);

        for (size_t u = 0; u < input.num_units; ++u) {
            result.thread_units[assignment[u]].push_back(u);
        }

        std::vector<double> thread_times(num_threads, 0.0);
        computeThreadTimes_(input, assignment, thread_times);

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

        result.cross_thread_connections = countCrossThreadConnections_(input, assignment);

        return result;
    }
};

}  // namespace chronon::sender
