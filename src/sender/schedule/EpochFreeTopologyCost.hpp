// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <numeric>
#include <span>
#include <vector>

#include "sender/schedule/PartitionUtils.hpp"

namespace chronon::sender::epoch_free_cost {

struct RuntimeDependency {
    size_t dependent = 0;
    size_t predecessor = 0;
    uint32_t min_delay = 0;
    size_t num_connections = 1;
};

/// Runtime rebalance is calibrated from measured platform synchronization,
/// independently of the heuristic locality weight used for initial placement.
inline double runtimeSyncCostNs(double atomic_roundtrip_ns) noexcept {
    return std::max(1.0, atomic_roundtrip_ns);
}

/// Dependency stalls ahead of the global frontier overlap useful work on
/// other workers; only a dependent at the exact floor is unhidden loss.
inline bool isUnhiddenDependencyWait(uint64_t dependent_cycle, uint64_t global_floor) noexcept {
    return dependent_cycle == global_floor;
}

/// Build the directed predecessor -> dependent graph used by the rebalance
/// cost model from runtime scheduling and finite-headroom relationships.
inline std::vector<std::vector<PartitionInput::EdgeInfo>> buildRuntimeAdjacency(
    size_t num_clusters, std::span<const RuntimeDependency> dependencies) {
    std::vector<std::vector<PartitionInput::EdgeInfo>> adjacency(num_clusters);
    for (const auto& dep : dependencies) {
        if (dep.predecessor >= num_clusters || dep.dependent >= num_clusters ||
            dep.predecessor == dep.dependent || dep.num_connections == 0) {
            continue;
        }
        adjacency[dep.predecessor].push_back({dep.dependent, dep.num_connections, dep.min_delay});
    }

    for (auto& edges : adjacency) {
        std::sort(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            if (a.neighbor != b.neighbor) return a.neighbor < b.neighbor;
            return a.min_delay < b.min_delay;
        });
        auto output = edges.begin();
        for (auto it = edges.begin(); it != edges.end(); ++it) {
            if (output != edges.begin() && std::prev(output)->neighbor == it->neighbor) {
                auto& aggregate = *std::prev(output);
                aggregate.min_delay = std::min(aggregate.min_delay, it->min_delay);
                const size_t remaining =
                    std::numeric_limits<size_t>::max() - aggregate.num_connections;
                aggregate.num_connections += std::min(remaining, it->num_connections);
                continue;
            }
            *output++ = *it;
        }
        edges.erase(output, edges.end());
    }
    return adjacency;
}

struct ObjectiveSummary {
    double objective = 0.0;
    double max_active = 0.0;
    double cross_pressure = 0.0;
    double max_incoming_pressure = 0.0;
    double heavy_colocation_penalty = 0.0;
    double idle_thread_penalty = 0.0;
    size_t active_threads = 0;
    std::vector<double> active;
    std::vector<double> incoming_pressure;
    std::vector<size_t> heavy_count;
};

struct MoveBreakdown {
    bool valid = false;
    double score = -std::numeric_limits<double>::infinity();
    double objective_gain = 0.0;
    double active_gain = 0.0;
    double topology_delta = 0.0;
    double measured_dep_bonus = 0.0;
    double floor_slack_bonus = 0.0;
    double target_dep_penalty = 0.0;
    double active_stack_penalty = 0.0;
    double churn_penalty = 0.0;
    double old_max_active = 0.0;
    double new_max_active = 0.0;
    double old_target_dep_pressure = 0.0;
    double new_target_dep_pressure = 0.0;
    double target_active_after = 0.0;
    double active_budget = 0.0;
    size_t target_heavy_before = 0;
    size_t target_heavy_after = 0;
};

struct RuntimeWaits {
    const std::vector<uint64_t>* thread_floor_wait_ns = nullptr;
    const std::vector<uint64_t>* thread_dep_wait_ns = nullptr;
    const std::vector<uint64_t>* thread_no_ready_wait_ns = nullptr;
    const std::vector<uint64_t>* cluster_blocked_wait_ns = nullptr;
    const std::vector<uint64_t>* cluster_blocker_wait_ns = nullptr;
};

inline double edgePressure(const PartitionInput& input,
                           const PartitionInput::EdgeInfo& edge) noexcept {
    const double sync = std::max(1.0, input.sync_cost_ns);
    const double delay_weight = edge.min_delay == 0   ? 100.0
                                : edge.min_delay == 1 ? 1.50
                                                      : 1.0 / static_cast<double>(edge.min_delay);
    return sync * static_cast<double>(edge.num_connections) * delay_weight;
}

inline double waitAt(const std::vector<uint64_t>* values, size_t idx) noexcept {
    if (!values || idx >= values->size()) return 0.0;
    return static_cast<double>((*values)[idx]);
}

inline double directPressureBetween(const PartitionInput& input, size_t a, size_t b) noexcept {
    double pressure = 0.0;
    if (a < input.adjacency.size()) {
        for (const auto& edge : input.adjacency[a]) {
            if (edge.neighbor == b) pressure += edgePressure(input, edge);
        }
    }
    if (b < input.adjacency.size()) {
        for (const auto& edge : input.adjacency[b]) {
            if (edge.neighbor == a) pressure += edgePressure(input, edge);
        }
    }
    return pressure;
}

inline std::vector<double> incidentPressure(const PartitionInput& input) {
    std::vector<double> pressure(input.num_units, 0.0);
    for (size_t u = 0; u < input.num_units; ++u) {
        for (const auto& edge : input.adjacency[u]) {
            const double p = edgePressure(input, edge);
            pressure[u] += p;
            if (edge.neighbor < pressure.size()) pressure[edge.neighbor] += p;
        }
    }
    return pressure;
}

inline double crossPressureForCluster(const PartitionInput& input,
                                      const std::vector<size_t>& assignment, size_t cluster,
                                      size_t cluster_thread) {
    double pressure = 0.0;
    if (cluster < input.adjacency.size()) {
        for (const auto& edge : input.adjacency[cluster]) {
            if (edge.neighbor < assignment.size() && assignment[edge.neighbor] != cluster_thread) {
                pressure += edgePressure(input, edge);
            }
        }
    }
    for (size_t u = 0; u < input.num_units; ++u) {
        if (u == cluster || u >= input.adjacency.size()) continue;
        for (const auto& edge : input.adjacency[u]) {
            if (edge.neighbor == cluster && assignment[u] != cluster_thread) {
                pressure += edgePressure(input, edge);
            }
        }
    }
    return pressure;
}

inline ObjectiveSummary summarize(const PartitionInput& input,
                                  const std::vector<size_t>& assignment, size_t num_threads) {
    ObjectiveSummary out;
    if (input.num_units == 0 || num_threads == 0) return out;

    out.active.assign(num_threads, 0.0);
    out.incoming_pressure.assign(num_threads, 0.0);
    out.heavy_count.assign(num_threads, 0);

    double total_active = 0.0;
    for (size_t u = 0; u < input.num_units; ++u) {
        const size_t t = assignment[u];
        if (t >= num_threads) continue;
        const double cost = u < input.unit_cost_ns.size() ? input.unit_cost_ns[u] : 1.0;
        out.active[t] += cost;
        total_active += cost;
    }
    out.max_active = *std::max_element(out.active.begin(), out.active.end());

    for (size_t u = 0; u < input.num_units; ++u) {
        const size_t src_thread = assignment[u];
        if (u >= input.adjacency.size()) continue;
        for (const auto& edge : input.adjacency[u]) {
            if (edge.neighbor >= assignment.size()) continue;
            const size_t dst_thread = assignment[edge.neighbor];
            if (src_thread == dst_thread) continue;
            const double p = edgePressure(input, edge);
            out.cross_pressure += p;
            if (dst_thread < out.incoming_pressure.size()) out.incoming_pressure[dst_thread] += p;
        }
    }
    out.max_incoming_pressure =
        *std::max_element(out.incoming_pressure.begin(), out.incoming_pressure.end());

    const auto incident = incidentPressure(input);
    std::vector<double> effective(input.num_units, 0.0);
    double total_effective = 0.0;
    for (size_t u = 0; u < input.num_units; ++u) {
        const double cost = u < input.unit_cost_ns.size() ? input.unit_cost_ns[u] : 1.0;
        effective[u] = cost + 0.02 * std::sqrt(incident[u]);
        total_effective += effective[u];
    }
    const double avg_effective =
        input.num_units > 0 ? total_effective / static_cast<double>(input.num_units) : 0.0;

    std::vector<size_t> heavy;
    for (size_t u = 0; u < input.num_units; ++u) {
        if (effective[u] > avg_effective * 1.25) heavy.push_back(u);
    }
    std::sort(heavy.begin(), heavy.end(), [&](size_t a, size_t b) {
        if (effective[a] != effective[b]) return effective[a] > effective[b];
        return a < b;
    });
    if (heavy.size() > num_threads * 2) heavy.resize(num_threads * 2);

    for (size_t u : heavy) {
        const size_t t = assignment[u];
        if (t < out.heavy_count.size()) out.heavy_count[t]++;
    }
    for (size_t i = 0; i < heavy.size(); ++i) {
        for (size_t j = i + 1; j < heavy.size(); ++j) {
            const size_t a = heavy[i];
            const size_t b = heavy[j];
            if (assignment[a] != assignment[b]) continue;
            const double coupling = directPressureBetween(input, a, b);
            const double protection = 1.0 + coupling / std::max(1.0, avg_effective);
            out.heavy_colocation_penalty +=
                0.20 * std::min(effective[a], effective[b]) / protection;
        }
    }

    const double avg_active =
        num_threads > 0 ? total_active / static_cast<double>(num_threads) : 0.0;
    double balance_penalty = 0.0;
    for (double active : out.active) {
        if (active > 0.0) out.active_threads++;
        const double diff = active - avg_active;
        balance_penalty += diff * diff / std::max(1.0, avg_active);
    }
    if (out.active_threads < num_threads) {
        out.idle_thread_penalty = 0.25 * total_active *
                                  static_cast<double>(num_threads - out.active_threads) /
                                  static_cast<double>(num_threads);
    }

    out.objective = out.max_active + 0.05 * balance_penalty + 0.35 * out.max_incoming_pressure +
                    0.12 * out.cross_pressure + out.heavy_colocation_penalty +
                    out.idle_thread_penalty;
    return out;
}

inline bool moveWouldSplitZeroDelay(const PartitionInput& input,
                                    const std::vector<size_t>& assignment, size_t unit_idx,
                                    size_t to_thread) {
    if (unit_idx >= input.adjacency.size()) return false;
    for (const auto& edge : input.adjacency[unit_idx]) {
        if (edge.min_delay == 0 && assignment[edge.neighbor] != to_thread) return true;
    }
    for (size_t u = 0; u < input.num_units; ++u) {
        if (u >= input.adjacency.size()) continue;
        for (const auto& edge : input.adjacency[u]) {
            if (edge.neighbor == unit_idx && edge.min_delay == 0 && assignment[u] != to_thread) {
                return true;
            }
        }
    }
    return false;
}

inline std::vector<size_t> improveInitialPlacement(const PartitionInput& input,
                                                   std::vector<size_t> assignment,
                                                   size_t num_threads) {
    if (input.num_units <= 1 || num_threads <= 1 || input.sync_cost_ns <= 0.0) return assignment;

    ObjectiveSummary best = summarize(input, assignment, num_threads);
    for (size_t pass = 0; pass < 3; ++pass) {
        size_t best_unit = SIZE_MAX;
        size_t best_target = SIZE_MAX;
        ObjectiveSummary best_candidate = best;

        for (size_t u = 0; u < input.num_units; ++u) {
            const size_t from = assignment[u];
            for (size_t target = 0; target < num_threads; ++target) {
                if (target == from) continue;
                if (moveWouldSplitZeroDelay(input, assignment, u, target)) continue;

                assignment[u] = target;
                ObjectiveSummary cand = summarize(input, assignment, num_threads);
                assignment[u] = from;
                if (cand.active_threads < best.active_threads) continue;
                if (cand.max_active > best.max_active + 0.01 &&
                    cand.max_incoming_pressure >= best.max_incoming_pressure - 0.01 &&
                    cand.cross_pressure >= best.cross_pressure - 0.01) {
                    continue;
                }
                if (cand.objective < best_candidate.objective - 0.01) {
                    best_candidate = std::move(cand);
                    best_unit = u;
                    best_target = target;
                }
            }
        }

        if (best_unit == SIZE_MAX) break;
        assignment[best_unit] = best_target;
        best = std::move(best_candidate);
    }
    return assignment;
}

inline MoveBreakdown scoreMove(const PartitionInput& input, const std::vector<size_t>& assignment,
                               size_t cluster, size_t target_thread, const RuntimeWaits& waits,
                               double min_gain_fraction, double churn_penalty) {
    MoveBreakdown out;
    if (cluster >= input.num_units || cluster >= assignment.size()) return out;
    const size_t num_threads = input.num_threads;
    const size_t source_thread = assignment[cluster];
    if (target_thread >= num_threads || target_thread == source_thread) return out;
    if (moveWouldSplitZeroDelay(input, assignment, cluster, target_thread)) return out;

    ObjectiveSummary old_summary = summarize(input, assignment, num_threads);
    std::vector<size_t> candidate = assignment;
    candidate[cluster] = target_thread;
    ObjectiveSummary new_summary = summarize(input, candidate, num_threads);

    const double cluster_cost =
        cluster < input.unit_cost_ns.size() ? input.unit_cost_ns[cluster] : 1.0;
    const double old_local_cross =
        crossPressureForCluster(input, assignment, cluster, source_thread);
    const double new_local_cross =
        crossPressureForCluster(input, candidate, cluster, target_thread);
    const double local_topology_delta = old_local_cross - new_local_cross;

    const double target_floor = waitAt(waits.thread_floor_wait_ns, target_thread);
    const double target_dep = waitAt(waits.thread_dep_wait_ns, target_thread);
    const double target_no_ready = waitAt(waits.thread_no_ready_wait_ns, target_thread);
    const double target_wait_total = target_floor + target_dep + target_no_ready;
    const double floor_ratio = target_wait_total > 0.0 ? target_floor / target_wait_total : 0.0;
    const double dep_ratio = target_wait_total > 0.0 ? target_dep / target_wait_total : 0.0;
    const double no_ready_ratio =
        target_wait_total > 0.0 ? target_no_ready / target_wait_total : 0.0;

    double total_active = 0.0;
    for (double active : old_summary.active) total_active += active;
    const double avg_active =
        num_threads > 0 ? total_active / static_cast<double>(num_threads) : 0.0;
    const double target_active_before =
        target_thread < old_summary.active.size() ? old_summary.active[target_thread] : 0.0;
    const double source_active_before =
        source_thread < old_summary.active.size() ? old_summary.active[source_thread] : 0.0;
    out.target_active_after = target_active_before + cluster_cost;
    out.active_budget = std::max(avg_active * 1.15, old_summary.max_active * 0.72);

    const double blocker_wait = waitAt(waits.cluster_blocker_wait_ns, cluster);
    const double cluster_wait = waitAt(waits.cluster_blocked_wait_ns, cluster) + blocker_wait;
    double total_dep_wait = 0.0;
    if (waits.thread_dep_wait_ns) {
        for (uint64_t wait : *waits.thread_dep_wait_ns) total_dep_wait += static_cast<double>(wait);
    }
    const double avg_dep_wait =
        waits.thread_dep_wait_ns && !waits.thread_dep_wait_ns->empty()
            ? total_dep_wait / static_cast<double>(waits.thread_dep_wait_ns->size())
            : 0.0;
    const double wait_scale = avg_dep_wait > 0.0 ? std::min(3.0, cluster_wait / avg_dep_wait) : 0.0;

    out.objective_gain = old_summary.objective - new_summary.objective;
    out.active_gain = old_summary.max_active - new_summary.max_active;
    out.topology_delta = (old_summary.cross_pressure + old_summary.max_incoming_pressure) -
                         (new_summary.cross_pressure + new_summary.max_incoming_pressure);
    out.measured_dep_bonus = std::max(0.0, local_topology_delta) * wait_scale * 0.20;
    out.floor_slack_bonus = cluster_cost * (0.45 * floor_ratio + 0.10 * no_ready_ratio);
    out.target_dep_penalty = cluster_cost * 0.55 * dep_ratio +
                             std::max(0.0, new_summary.incoming_pressure[target_thread] -
                                               old_summary.incoming_pressure[target_thread]) *
                                 0.20;
    if (out.topology_delta > 0.0) {
        out.target_dep_penalty = std::max(0.0, out.target_dep_penalty - out.topology_delta * 0.50);
    }
    const size_t old_heavy = old_summary.heavy_count[target_thread];
    const size_t new_heavy = new_summary.heavy_count[target_thread];
    out.active_stack_penalty =
        new_heavy > old_heavy ? cluster_cost * 0.15 * static_cast<double>(new_heavy - old_heavy)
                              : 0.0;
    out.churn_penalty = churn_penalty;
    out.old_max_active = old_summary.max_active;
    out.new_max_active = new_summary.max_active;
    out.old_target_dep_pressure = old_summary.incoming_pressure[target_thread];
    out.new_target_dep_pressure = new_summary.incoming_pressure[target_thread];
    out.target_heavy_before = old_summary.heavy_count[target_thread];
    out.target_heavy_after = new_summary.heavy_count[target_thread];

    const bool strong_dep_relief =
        out.topology_delta > std::max(cluster_cost * 0.20, old_summary.objective * 0.02) ||
        out.measured_dep_bonus > cluster_cost * 0.25;
    const bool stacks_heavy =
        out.target_heavy_after > out.target_heavy_before && out.target_heavy_before > 0;
    const bool target_over_budget = out.target_active_after > out.active_budget;
    const bool relieves_source_balance = source_active_before > avg_active * 1.15 &&
                                         target_active_before < source_active_before &&
                                         out.target_active_after <= source_active_before &&
                                         new_summary.max_active <= old_summary.max_active * 1.02;
    const bool relieves_active =
        out.active_gain > std::max(cluster_cost * 0.05, old_summary.max_active * min_gain_fraction);
    const bool active_or_balance_relief = relieves_active || relieves_source_balance;
    const bool relieves_critical_blocker =
        active_or_balance_relief && avg_dep_wait > 0.0 && blocker_wait > avg_dep_wait * 0.50;
    if (stacks_heavy && !(strong_dep_relief || active_or_balance_relief)) return out;
    if (target_over_budget && !(strong_dep_relief || active_or_balance_relief)) return out;
    if (out.active_gain < -std::max(0.01, old_summary.max_active * min_gain_fraction * 0.50) &&
        !strong_dep_relief) {
        return out;
    }

    const double floor_capacity =
        avg_active > 0.0 ? std::max(0.0, (avg_active - target_active_before) / avg_active) : 0.0;
    out.floor_slack_bonus =
        cluster_cost * (0.08 * floor_ratio + 0.03 * no_ready_ratio) * std::min(1.0, floor_capacity);
    if (stacks_heavy && !relieves_source_balance) {
        out.active_stack_penalty += cluster_cost * 0.50;
    }
    if (target_over_budget) {
        out.active_stack_penalty += out.target_active_after - out.active_budget;
    }

    out.score = out.objective_gain + out.measured_dep_bonus + out.floor_slack_bonus -
                out.target_dep_penalty - out.active_stack_penalty - out.churn_penalty;

    const double min_score = std::max(0.01, min_gain_fraction * old_summary.objective * 0.25);
    if (new_summary.max_active > old_summary.max_active * (1.0 + min_gain_fraction * 0.5) &&
        out.topology_delta <= 0.0) {
        return out;
    }
    if (dep_ratio > 0.50 && out.topology_delta <= 0.0 && !relieves_critical_blocker) return out;
    if (relieves_source_balance && out.score >= -cluster_cost * 0.25) {
        out.valid = true;
        return out;
    }
    out.valid = out.score >= min_score;
    return out;
}

}  // namespace chronon::sender::epoch_free_cost
