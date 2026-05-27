// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

#include "sender/schedule/PartitionUtils.hpp"

namespace chronon::sender {

/**
 * @brief Simulated-annealing solver for unit-to-thread partitioning.
 *
 * Same cost model as WeightedPartitioner (max thread time, delay-scaled sync costs) but
 * stochastic instead of greedy. Initial partitioning only — no dynamic rebalance.
 */
class SimulatedAnnealingPartitioner {
    static double maxThreadTime_(const PartitionInput& input, const std::vector<size_t>& assignment,
                                 size_t num_threads) {
        std::vector<double> times(num_threads, 0.0);
        partition_utils::computeThreadTimes(input, assignment, times);
        return *std::max_element(times.begin(), times.end());
    }

    /**
     * @brief Worst per-thread critical-path chain cost.
     *
     * For each thread, the chain is the longest path of "anchor" units in the
     * intra-thread DAG, where an anchor is a unit with at least one cross-thread
     * incoming edge (it gates on the predecessor thread's progress atomic). Computed
     * by topological longest-path DP. If the intra-thread subgraph cycles (e.g. a
     * delay>0 feedback loop co-located on one thread), falls back to per-thread
     * anchor-cost sum (an upper bound). Pure function of (assignment, topology, cost).
     */
    static double maxCriticalPathChain_(const PartitionInput& input,
                                        const std::vector<size_t>& assignment, size_t num_threads) {
        const size_t N = input.num_units;
        if (N == 0 || num_threads <= 1) return 0.0;

        std::vector<bool> is_anchor(N, false);
        std::vector<std::vector<size_t>> intra_succ(N);
        std::vector<size_t> intra_indeg(N, 0);

        for (size_t u = 0; u < N; ++u) {
            size_t tu = assignment[u];
            for (const auto& edge : input.adjacency[u]) {
                size_t v = edge.neighbor;
                size_t tv = assignment[v];
                if (tu == tv) {
                    intra_succ[u].push_back(v);
                    intra_indeg[v]++;
                } else {
                    // v must gate on tu's progress atomic before advancing.
                    is_anchor[v] = true;
                }
            }
        }

        std::vector<double> dp(N, 0.0);
        for (size_t u = 0; u < N; ++u) {
            if (is_anchor[u]) dp[u] = input.unit_cost_ns[u];
        }

        std::vector<size_t> queue;
        queue.reserve(N);
        for (size_t u = 0; u < N; ++u) {
            if (intra_indeg[u] == 0) queue.push_back(u);
        }

        size_t processed = 0;
        for (size_t qi = 0; qi < queue.size(); ++qi) {
            size_t u = queue[qi];
            ++processed;
            for (size_t v : intra_succ[u]) {
                double cand = dp[u] + (is_anchor[v] ? input.unit_cost_ns[v] : 0.0);
                if (cand > dp[v]) dp[v] = cand;
                if (--intra_indeg[v] == 0) queue.push_back(v);
            }
        }

        if (processed < N) {
            std::vector<double> anchor_sum(num_threads, 0.0);
            for (size_t u = 0; u < N; ++u) {
                if (is_anchor[u]) anchor_sum[assignment[u]] += input.unit_cost_ns[u];
            }
            return *std::max_element(anchor_sum.begin(), anchor_sum.end());
        }

        std::vector<double> chain_per_thread(num_threads, 0.0);
        for (size_t u = 0; u < N; ++u) {
            size_t t = assignment[u];
            if (dp[u] > chain_per_thread[t]) chain_per_thread[t] = dp[u];
        }
        return *std::max_element(chain_per_thread.begin(), chain_per_thread.end());
    }

    /**
     * @brief SA objective: max thread time scaled by balance and utilization penalties.
     *
     * Pure max-thread-time minimization incentivizes degenerate partitions (all units
     * on one thread → zero sync cost). The balance + utilization terms ensure SA explores
     * partitions that actually use multiple threads. Critical-path term is additive so
     * it doesn't compound with the multiplicative scaling.
     */
    static double objective_(const PartitionInput& input, const std::vector<size_t>& assignment,
                             size_t num_threads) {
        std::vector<double> times(num_threads, 0.0);
        partition_utils::computeThreadTimes(input, assignment, times);
        double max_time = *std::max_element(times.begin(), times.end());

        std::vector<double> compute(num_threads, 0.0);
        for (size_t u = 0; u < input.num_units; ++u) {
            compute[assignment[u]] += input.unit_cost_ns[u];
        }
        double total_compute = 0.0;
        size_t active = 0;
        double max_compute = 0.0;
        for (size_t t = 0; t < num_threads; ++t) {
            if (compute[t] > 0.0) {
                total_compute += compute[t];
                max_compute = std::max(max_compute, compute[t]);
                active++;
            }
        }
        double avg_compute = (active > 0) ? total_compute / static_cast<double>(active) : 1.0;

        double imbalance = (avg_compute > 0.0) ? (max_compute / avg_compute) - 1.0 : 0.0;

        double utilization_penalty =
            (num_threads > 1 && active < num_threads)
                ? static_cast<double>(num_threads - active) / static_cast<double>(num_threads - 1)
                : 0.0;

        constexpr double balance_weight = 0.5;
        constexpr double utilization_weight = 0.3;
        double base = max_time *
                      (1.0 + balance_weight * imbalance + utilization_weight * utilization_penalty);

        if (input.critical_path_weight > 0.0) {
            base +=
                input.critical_path_weight * maxCriticalPathChain_(input, assignment, num_threads);
        }
        return base;
    }

    struct UnionFind {
        std::vector<size_t> parent;
        std::vector<size_t> rank;

        explicit UnionFind(size_t n) : parent(n), rank(n, 0) {
            std::iota(parent.begin(), parent.end(), 0);
        }

        size_t find(size_t x) {
            while (parent[x] != x) {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        }

        void unite(size_t a, size_t b) {
            a = find(a);
            b = find(b);
            if (a == b) return;
            if (rank[a] < rank[b]) std::swap(a, b);
            parent[b] = a;
            if (rank[a] == rank[b]) rank[a]++;
        }
    };

    /** @brief Output of mergeDelayZeroGroups_(): super-unit problem plus mapping back. */
    struct ReducedProblem {
        PartitionInput reduced_input;
        std::vector<size_t> unit_to_group;
        std::vector<std::vector<size_t>> group_units;
        bool was_reduced;
    };

    /// Coalesce delay=0 components into super-units so SA never separates them.
    static ReducedProblem mergeDelayZeroGroups_(const PartitionInput& input) {
        ReducedProblem rp;
        rp.unit_to_group.resize(input.num_units);

        UnionFind uf(input.num_units);

        for (size_t u = 0; u < input.num_units; ++u) {
            for (const auto& edge : input.adjacency[u]) {
                if (edge.min_delay == 0) {
                    uf.unite(u, edge.neighbor);
                }
            }
        }

        std::vector<size_t> root_to_group(input.num_units, SIZE_MAX);
        size_t num_groups = 0;
        for (size_t u = 0; u < input.num_units; ++u) {
            size_t root = uf.find(u);
            if (root_to_group[root] == SIZE_MAX) {
                root_to_group[root] = num_groups++;
            }
            rp.unit_to_group[u] = root_to_group[root];
        }

        rp.was_reduced = (num_groups < input.num_units);

        if (!rp.was_reduced) {
            rp.reduced_input = input;
            rp.group_units.resize(input.num_units);
            for (size_t u = 0; u < input.num_units; ++u) {
                rp.group_units[u].assign(1, u);
            }
            return rp;
        }

        rp.group_units.resize(num_groups);
        for (size_t u = 0; u < input.num_units; ++u) {
            rp.group_units[rp.unit_to_group[u]].push_back(u);
        }

        auto& ri = rp.reduced_input;
        ri.num_units = num_groups;
        ri.num_threads = input.num_threads;
        ri.sync_cost_ns = input.sync_cost_ns;
        ri.critical_path_weight = input.critical_path_weight;

        ri.unit_cost_ns.assign(num_groups, 0.0);
        for (size_t u = 0; u < input.num_units; ++u) {
            ri.unit_cost_ns[rp.unit_to_group[u]] += input.unit_cost_ns[u];
        }

        ri.adjacency.resize(num_groups);

        // Linear scan is fine: groups are small enough that a hash map would be slower.
        for (size_t g = 0; g < num_groups; ++g) {
            std::vector<std::pair<size_t, uint32_t>> edge_map;
            std::vector<size_t> neighbor_groups;

            for (size_t u : rp.group_units[g]) {
                for (const auto& edge : input.adjacency[u]) {
                    size_t ng = rp.unit_to_group[edge.neighbor];
                    if (ng == g) continue;

                    bool found = false;
                    for (size_t i = 0; i < neighbor_groups.size(); ++i) {
                        if (neighbor_groups[i] == ng) {
                            edge_map[i].first += edge.num_connections;
                            edge_map[i].second = std::min(edge_map[i].second, edge.min_delay);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        neighbor_groups.push_back(ng);
                        edge_map.push_back({edge.num_connections,
                                            edge.min_delay == 0 ? uint32_t(1) : edge.min_delay});
                    }
                }
            }

            for (size_t i = 0; i < neighbor_groups.size(); ++i) {
                ri.adjacency[g].push_back(
                    {neighbor_groups[i], edge_map[i].first, edge_map[i].second});
            }
        }

        return rp;
    }

    /// Deterministic seed derived from input — ensures repeatable SA across runs.
    static uint64_t seedFromInput_(const PartitionInput& input) {
        uint64_t h = input.num_units * 2654435761ULL;
        h ^= input.num_threads * 40503ULL;
        for (double c : input.unit_cost_ns) {
            uint64_t bits = 0;
            std::memcpy(&bits, &c, sizeof(bits));
            h ^= bits * 2246822519ULL;
            h = (h << 13) | (h >> 51);
        }
        return h;
    }

    static std::vector<size_t> saSearch_(const PartitionInput& input,
                                         std::vector<size_t> assignment, size_t num_threads) {
        if (input.num_units <= 1 || num_threads <= 1) {
            return assignment;
        }

        std::mt19937_64 rng(seedFromInput_(input));

        std::vector<size_t> thread_count(num_threads, 0);
        for (size_t t : assignment) {
            thread_count[t]++;
        }

        double current_cost = objective_(input, assignment, num_threads);

        std::vector<size_t> best_assignment = assignment;
        double best_cost = current_cost;

        const size_t N = input.num_units;
        const size_t T = num_threads;
        double temp = 0.10 * current_cost;
        if (temp < 0.1) temp = 0.1;  // Guard against zero-cost inputs.
        const double cooling_rate = (N <= 10) ? 0.90 : 0.95;
        const double temp_final = 0.1;
        const size_t iters_per_temp = std::max(size_t(10), N * T);
        const size_t max_total_iters = 100 * N * T;

        // ceil(N/T) * 2 leaves room while preventing degeneration onto one thread.
        const size_t max_per_thread = std::max(size_t(2), ((N + T - 1) / T) * 2);

        std::uniform_real_distribution<double> unif(0.0, 1.0);
        std::uniform_int_distribution<size_t> unit_dist(0, N - 1);
        std::uniform_int_distribution<size_t> thread_dist(0, num_threads - 1);

        size_t total_iters = 0;

        while (temp > temp_final && total_iters < max_total_iters) {
            for (size_t iter = 0; iter < iters_per_temp && total_iters < max_total_iters;
                 ++iter, ++total_iters) {
                bool do_swap = (unif(rng) < 0.30);

                if (do_swap && N >= 2) {
                    // Swaps preserve per-thread counts — no balance check needed.
                    size_t u = unit_dist(rng);
                    size_t v = unit_dist(rng);
                    size_t attempts = 0;
                    while ((v == u || assignment[v] == assignment[u]) && attempts < N) {
                        v = unit_dist(rng);
                        attempts++;
                    }
                    if (v == u || assignment[v] == assignment[u]) continue;

                    std::swap(assignment[u], assignment[v]);

                    double new_cost = objective_(input, assignment, num_threads);
                    double delta = new_cost - current_cost;

                    if (delta < 0.0 || unif(rng) < std::exp(-delta / temp)) {
                        current_cost = new_cost;
                        if (new_cost < best_cost) {
                            best_cost = new_cost;
                            best_assignment = assignment;
                        }
                    } else {
                        std::swap(assignment[u], assignment[v]);
                    }
                } else {
                    size_t u = unit_dist(rng);
                    size_t old_thread = assignment[u];
                    size_t new_thread = thread_dist(rng);
                    if (new_thread == old_thread) continue;

                    // Don't empty source or overflow destination.
                    if (thread_count[old_thread] <= 1) continue;
                    if (thread_count[new_thread] >= max_per_thread) continue;

                    assignment[u] = new_thread;
                    thread_count[old_thread]--;
                    thread_count[new_thread]++;

                    double new_cost = objective_(input, assignment, num_threads);
                    double delta = new_cost - current_cost;

                    if (delta < 0.0 || unif(rng) < std::exp(-delta / temp)) {
                        current_cost = new_cost;
                        if (new_cost < best_cost) {
                            best_cost = new_cost;
                            best_assignment = assignment;
                        }
                    } else {
                        assignment[u] = old_thread;
                        thread_count[old_thread]++;
                        thread_count[new_thread]--;
                    }
                }
            }

            temp *= cooling_rate;
        }

        return best_assignment;
    }

public:
    /// Exposed for tests and external diagnostics.
    static double evaluateObjective(const PartitionInput& input,
                                    const std::vector<size_t>& assignment, size_t num_threads) {
        return objective_(input, assignment, num_threads);
    }

    /// Exposed for tests.
    static double computeCriticalPathChain(const PartitionInput& input,
                                           const std::vector<size_t>& assignment,
                                           size_t num_threads) {
        return maxCriticalPathChain_(input, assignment, num_threads);
    }

    /// Matches the PartitionSolver function-pointer signature.
    static PartitionResult partition(const PartitionInput& input) {
        if (input.num_units == 0 || input.num_threads == 0) {
            return {};
        }

        size_t num_threads = std::min(input.num_threads, input.num_units);

        auto rp = mergeDelayZeroGroups_(input);
        auto& ri = rp.reduced_input;
        size_t reduced_threads = std::min(num_threads, ri.num_units);

        auto group_assignment = partition_utils::initialLPT(ri, reduced_threads);

        auto lpt_assignment = group_assignment;  // Fallback seed for the balance gate.
        group_assignment = saSearch_(ri, std::move(group_assignment), reduced_threads);

        double sa_obj = objective_(ri, group_assignment, reduced_threads);

        // Balance gate: SA can converge on a partition that concentrates compute on one
        // thread (the simulation would then fall back to sequential mode and waste threads).
        // Fall back to the always-balanced LPT seed when that happens.
        bool lpt_fb = false;
        if (reduced_threads >= 2) {
            std::vector<double> compute(reduced_threads, 0.0);
            for (size_t u = 0; u < ri.num_units; ++u) {
                compute[group_assignment[u]] += ri.unit_cost_ns[u];
            }
            double total = 0.0;
            double max_c = 0.0;
            size_t active = 0;
            for (size_t t = 0; t < reduced_threads; ++t) {
                total += compute[t];
                max_c = std::max(max_c, compute[t]);
                if (compute[t] > 0.0) active++;
            }
            double avg = (active > 0) ? total / static_cast<double>(active) : 1.0;
            // Bottleneck >1.8x average compute or unused threads → fall back to LPT.
            if (active < reduced_threads || (avg > 0.0 && max_c / avg > 1.8)) {
                group_assignment = lpt_assignment;
                lpt_fb = true;
            }
        }

        std::vector<size_t> assignment(input.num_units);
        for (size_t u = 0; u < input.num_units; ++u) {
            assignment[u] = group_assignment[rp.unit_to_group[u]];
        }

        std::vector<double> ptc(num_threads, 0.0);
        for (size_t u = 0; u < input.num_units; ++u) {
            ptc[assignment[u]] += input.unit_cost_ns[u];
        }
        size_t act = 0;
        double mx = 0.0, tot = 0.0;
        for (double c : ptc) {
            if (c > 0.0) {
                act++;
                tot += c;
                mx = std::max(mx, c);
            }
        }
        double avg_c = (act > 0) ? tot / static_cast<double>(act) : 1.0;

        auto result = partition_utils::buildResult(input, assignment, num_threads);
        result.stats.sa_objective = sa_obj;
        result.stats.lpt_fallback = lpt_fb;
        result.stats.active_threads = act;
        result.stats.compute_imbalance_ratio = (avg_c > 0.0) ? mx / avg_c : 1.0;
        result.stats.per_thread_compute_ns = std::move(ptc);
        return result;
    }
};

}  // namespace chronon::sender
