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
#include <vector>

#include "sender/schedule/PartitionUtils.hpp"

namespace chronon::sender {

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
public:
    static PartitionResult partition(const PartitionInput& input) {
        if (input.num_units == 0 || input.num_threads == 0) {
            return {};
        }

        size_t num_threads = std::min(input.num_threads, input.num_units);

        std::vector<size_t> assignment = partition_utils::initialLPT(input, num_threads);

        for (int pass = 0; pass < 5; ++pass) {
            bool improved = refinePartition_(input, assignment, num_threads);
            if (!improved) break;
        }

        pairwiseSwap_(input, assignment, num_threads);
        multiUnitRelocate_(input, assignment, num_threads);

        return partition_utils::buildResult(input, assignment, num_threads);
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
        partition_utils::computeThreadTimes(input, assignment, thread_times);

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
        partition_utils::computeThreadTimes(input, assignment, thread_times);
        return thread_times;
    }

private:
    /// Phase 2: FM refinement — move units from heaviest to lightest when it reduces max time.
    static bool refinePartition_(const PartitionInput& input, std::vector<size_t>& assignment,
                                 size_t num_threads) {
        std::vector<double> thread_times(num_threads, 0.0);
        partition_utils::computeThreadTimes(input, assignment, thread_times);

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

            partition_utils::computeThreadTimes(input, assignment, thread_times);
            any_improvement = true;
        }

        return any_improvement;
    }

    /// Gain = (old max thread time) - (new max thread time); positive means improvement.
    static double computeMoveGain_(const PartitionInput& input,
                                   const std::vector<size_t>& assignment,
                                   const std::vector<double>& thread_times, size_t unit_idx,
                                   size_t from_thread, size_t to_thread) {
        (void)from_thread;

        double old_max = *std::max_element(thread_times.begin(), thread_times.end());

        std::vector<size_t> new_assignment = assignment;
        new_assignment[unit_idx] = to_thread;

        std::vector<double> new_times(thread_times.size(), 0.0);
        partition_utils::computeThreadTimes(input, new_assignment, new_times);
        double new_max = *std::max_element(new_times.begin(), new_times.end());

        return old_max - new_max;
    }

    /// Phase 3: pairwise swap across all thread pairs — catches equal-load but uncoupled cases.
    static void pairwiseSwap_(const PartitionInput& input, std::vector<size_t>& assignment,
                              size_t num_threads) {
        std::vector<double> thread_times(num_threads, 0.0);
        partition_utils::computeThreadTimes(input, assignment, thread_times);

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
                            partition_utils::computeThreadTimes(input, assignment, new_times);

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
        partition_utils::computeThreadTimes(input, assignment, thread_times);

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
            // Lightest target first; ties broken by thread index so the choice is
            // independent of std::sort's (unstable) ordering of equal-load threads.
            std::sort(targets.begin(), targets.end(), [&](size_t a, size_t b) {
                if (thread_times[a] != thread_times[b]) return thread_times[a] < thread_times[b];
                return a < b;
            });
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
                            partition_utils::computeThreadTimes(input, assignment, new_times);
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
                partition_utils::computeThreadTimes(input, assignment, thread_times);
                improved = true;
            }
        }
    }
};

}  // namespace chronon::sender
