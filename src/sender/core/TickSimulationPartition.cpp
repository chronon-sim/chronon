// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// TickSimulation partitioning methods: cost-aware weighted partitioning,
/// cluster affinity assignment, queue optimization, and cross-thread
/// dependency analysis.

#include <algorithm>
#include <string>
#include <unordered_map>

#include "TickSimulation.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"

namespace chronon::sender {

// ---------------------------------------------------------------------------
// Adjacency helpers (file-local)
// ---------------------------------------------------------------------------

namespace {
struct PairKey {
    size_t u, v;
    bool operator==(const PairKey& o) const { return u == o.u && v == o.v; }
};
struct PairHash {
    size_t operator()(const PairKey& k) const {
        return k.u ^ (k.v * 0x9e3779b97f4a7c15ULL + (k.u << 6) + (k.u >> 2));
    }
};

}  // namespace

void TickSimulation::buildPartitionAdjacency_(
    const std::unordered_map<Unit*, size_t>& unit_ptr_to_idx, PartitionInput& input) const {
    std::unordered_map<PairKey, std::pair<size_t, uint32_t>, PairHash> pair_info;

    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;
        auto si = unit_ptr_to_idx.find(src);
        auto di = unit_ptr_to_idx.find(dst);
        if (si == unit_ptr_to_idx.end() || di == unit_ptr_to_idx.end()) continue;

        size_t s = si->second, d = di->second;
        // Directed edge only; reciprocal traffic has its own Connection.
        auto& fwd = pair_info[{s, d}];
        fwd.first++;
        fwd.second = (fwd.second == 0) ? conn->delay() : std::min(fwd.second, conn->delay());
    }

    // Deterministic edge order keeps solver floating-point sums repeatable.
    std::vector<PairKey> keys;
    keys.reserve(pair_info.size());
    for (const auto& [key, info] : pair_info) keys.push_back(key);
    std::sort(keys.begin(), keys.end(), [](const PairKey& a, const PairKey& b) {
        return a.u != b.u ? a.u < b.u : a.v < b.v;
    });
    for (const PairKey& key : keys) {
        const auto& info = pair_info[key];
        input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }
}

PartitionInput TickSimulation::buildUnitPartitionInput_(double sync_cost_ns) const {
    PartitionInput input;
    input.num_units = unit_ptrs_.size();
    input.num_threads = thread_units_.size();
    input.unit_cost_ns = unit_costs_;
    input.sync_cost_ns = sync_cost_ns;
    input.critical_path_weight = config_.sa_critical_path_weight;
    input.adjacency.resize(unit_ptrs_.size());

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }
    buildPartitionAdjacency_(unit_ptr_to_idx, input);

    return input;
}

// ---------------------------------------------------------------------------
// Cost-aware thread assignment
// ---------------------------------------------------------------------------

void TickSimulation::assignThreadsDeterministic_() {
    size_t num_threads = normalizeThreadCount(config_.num_threads);
    config_.num_threads = num_threads;

    // Uniform unit costs + a fixed, wall-clock-free sync cost make the initial
    // partition fully deterministic (no measurement noise) while still rewarding
    // locality: the non-zero sync term drives the solver to minimize the
    // cross-thread edge cut, co-locating connected units (e.g. a core's pipeline)
    // on one thread so their intra-component edges resolve intra-thread for free.
    // A zero cost (config opt-out) reverts to pure load balance.
    //
    // The locality weight is scoped to THIS initial partition: it is passed
    // straight to the solver and deliberately NOT stored in platform_metrics_,
    // so dynamic rebalance keeps deciding migrations on its own inputs (measured
    // unit costs + platform_metrics_, which stays zero in the deterministic flow)
    // rather than on this fixed heuristic.
    platform_metrics_ = PlatformMetrics{};

    unit_costs_.assign(unit_ptrs_.size(), 1.0);

    if (observe_ctx_) {
        observe::log_info<"Locality-aware partitioning: unit_cost=1.0, sync_cost={}ns ({} units)">(
            observe_ctx_, static_cast<uint64_t>(config_.initial_partition_sync_cost_ns),
            unit_costs_.size());
    }

    applyClusteredThreadAssignment_(num_threads, config_.initial_partition_sync_cost_ns);
}

void TickSimulation::assignThreadsFromPrecomputedCosts_() {
    size_t num_threads = normalizeThreadCount(config_.num_threads);
    config_.num_threads = num_threads;

    platform_metrics_ = precomputed_platform_metrics_;
    unit_costs_ = precomputed_unit_costs_;

    if (observe_ctx_) {
        observe::log_info<"Using pre-computed profiling data ({} units)">(observe_ctx_,
                                                                          unit_costs_.size());
        observe::log_info<"Platform sync cost: {}ns (pre-computed)">(
            observe_ctx_, static_cast<uint64_t>(platform_metrics_.atomic_roundtrip_ns));
        for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
            observe::log_info<"  {}: cost={}ns (pre-computed)">(
                observe_ctx_, unit_ptrs_[i]->name().c_str(), static_cast<uint64_t>(unit_costs_[i]));
        }
    }

    // Precomputed flow carries a real measured platform sync cost; rebalance
    // uses the same value (platform_metrics_) for consistency.
    applyClusteredThreadAssignment_(num_threads, platform_metrics_.atomic_roundtrip_ns);
}

// ---------------------------------------------------------------------------
// Clustered thread assignment
// ---------------------------------------------------------------------------

void TickSimulation::applyClusteredThreadAssignment_(size_t num_threads,
                                                     double partition_sync_cost_ns) {
    if (dep_graph_.graph()) {
        clusters_ = findTightCouplingClusters(*dep_graph_.graph());
    } else {
        clusters_.cluster_id.resize(unit_ptrs_.size());
        clusters_.clusters.resize(unit_ptrs_.size());
        for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
            clusters_.cluster_id[i] = i;
            clusters_.clusters[i].assign(1, i);
        }
    }
    unit_to_cluster_ = clusters_.cluster_id;
    size_t num_clusters = clusters_.numClusters();

    std::vector<double> cluster_costs(num_clusters, 0.0);
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        cluster_costs[clusters_.cluster_id[i]] += unit_costs_[i];
    }

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    std::unordered_map<PairKey, std::pair<size_t, uint32_t>, PairHash> cluster_pair_info;

    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;
        auto si = unit_ptr_to_idx.find(src);
        auto di = unit_ptr_to_idx.find(dst);
        if (si == unit_ptr_to_idx.end() || di == unit_ptr_to_idx.end()) continue;

        size_t sc = clusters_.cluster_id[si->second];
        size_t dc = clusters_.cluster_id[di->second];
        if (sc == dc) continue;

        // Directed edge only — see buildPartitionAdjacency_ for rationale.
        auto& fwd = cluster_pair_info[{sc, dc}];
        fwd.first++;
        fwd.second = (fwd.second == 0) ? conn->delay() : std::min(fwd.second, conn->delay());
    }

    PartitionInput cluster_input;
    cluster_input.num_units = num_clusters;
    cluster_input.num_threads = num_threads;
    cluster_input.unit_cost_ns = cluster_costs;
    cluster_input.sync_cost_ns = partition_sync_cost_ns;
    cluster_input.critical_path_weight = config_.sa_critical_path_weight;
    cluster_input.adjacency.resize(num_clusters);

    // Deterministic (u, v) drain order; the adjacency feeds floating-point sums.
    std::vector<PairKey> cluster_keys;
    cluster_keys.reserve(cluster_pair_info.size());
    for (const auto& [key, info] : cluster_pair_info) cluster_keys.push_back(key);
    std::sort(cluster_keys.begin(), cluster_keys.end(), [](const PairKey& a, const PairKey& b) {
        return a.u != b.u ? a.u < b.u : a.v < b.v;
    });
    for (const PairKey& key : cluster_keys) {
        const auto& info = cluster_pair_info[key];
        cluster_input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }

    auto result = runPartitionSolver_(cluster_input);
    if (config_.enable_lookahead && config_.enable_epoch_free_lookahead && num_threads > 1) {
        auto improved = epoch_free_cost::improveInitialPlacement(
            cluster_input, result.unit_to_thread, num_threads);
        if (improved != result.unit_to_thread) {
            auto stats = result.stats;
            result = partition_utils::buildResult(cluster_input, improved, num_threads);
            result.stats = std::move(stats);
        }
    }

    cluster_to_thread_.resize(num_clusters);
    for (size_t c = 0; c < num_clusters; ++c) {
        cluster_to_thread_[c] = result.unit_to_thread[c];
    }

    thread_units_.resize(num_threads);
    for (auto& tu : thread_units_) {
        tu.clear();
    }
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        size_t cluster = clusters_.cluster_id[i];
        size_t thread = cluster_to_thread_[cluster];
        thread_units_[thread].push_back(i);
    }

    optimizeConnectionQueuesForThreads();

    for (auto* unit : unit_ptrs_) {
        unit->useFastCycleCounter();
    }

    has_thread_assignment_ = true;
    buildCrossThreadDependencies();

    if (observe_ctx_) {
        observe::log_info<"Clustered partition: {} clusters, imbalance_pct={}, cross_thread={}">(
            observe_ctx_, num_clusters, static_cast<uint64_t>(result.imbalance_ratio * 100),
            result.cross_thread_connections);
        for (size_t c = 0; c < num_clusters; ++c) {
            std::string names = buildUnitNameList_(clusters_.clusters[c]);
            observe::log_info<"  Cluster {} -> thread {}: cost={}ns [{}]">(
                observe_ctx_, c, cluster_to_thread_[c], static_cast<uint64_t>(cluster_costs[c]),
                names.c_str());
        }
        for (size_t t = 0; t < num_threads; ++t) {
            if (thread_units_[t].empty()) continue;
            double cost = 0.0;
            for (size_t u : thread_units_[t]) {
                cost += unit_costs_[u];
            }
            std::string names = buildUnitNameList_(thread_units_[t]);
            observe::log_info<"  Thread {}: cost={}ns [{}]">(
                observe_ctx_, t, static_cast<uint64_t>(cost), names.c_str());
        }
        if (config_.partition_solver == TickSimulationConfig::PartitionSolverType::SA) {
            observe::log_info<
                "  SA: objective={}, lpt_fallback={}, active_threads={}, "
                "compute_imbalance_pct={}">(
                observe_ctx_, static_cast<uint64_t>(result.stats.sa_objective),
                result.stats.lpt_fallback ? 1 : 0, result.stats.active_threads,
                static_cast<uint64_t>(result.stats.compute_imbalance_ratio * 100));
        }
    }

    if (config_.trace_execution &&
        config_.partition_solver == TickSimulationConfig::PartitionSolverType::SA) {
        std::fprintf(stderr,
                     "  SA: objective=%lu, lpt_fallback=%s, active_threads=%zu, "
                     "compute_imbalance=%.2f\n",
                     static_cast<unsigned long>(result.stats.sa_objective),
                     result.stats.lpt_fallback ? "yes" : "no", result.stats.active_threads,
                     result.stats.compute_imbalance_ratio);
        for (size_t t = 0; t < result.stats.per_thread_compute_ns.size(); ++t) {
            if (result.stats.per_thread_compute_ns[t] > 0.0) {
                std::fprintf(stderr, "    Thread %zu compute: %luns\n", t,
                             static_cast<unsigned long>(result.stats.per_thread_compute_ns[t]));
            }
        }
    }
}

bool TickSimulation::parallelBeneficialWeighted_() const {
    if (thread_units_.empty() || unit_costs_.empty()) {
        return false;
    }

    double max_cost = 0.0;
    double total_cost = 0.0;
    size_t active_threads = 0;
    std::vector<size_t> assignment(unit_ptrs_.size(), 0);

    for (size_t t = 0; t < thread_units_.size(); ++t) {
        const auto& tu = thread_units_[t];
        if (tu.empty()) continue;
        active_threads++;
        for (size_t u : tu) {
            if (u < assignment.size()) assignment[u] = t;
        }
    }

    if (active_threads < 2) return false;

    for (double cost : unit_costs_) {
        total_cost += cost;
    }

    // In the deterministic path unit_costs_ are uniform placeholders and
    // initial_partition_sync_cost_ns is a placement-only locality weight. Only
    // precomputed profiling carries a measured sync cost suitable for deciding
    // whether parallel execution is beneficial.
    double sync_cost_ns = has_precomputed_costs_ ? platform_metrics_.atomic_roundtrip_ns : 0.0;
    auto input = buildUnitPartitionInput_(sync_cost_ns);
    auto thread_times = WeightedPartitioner::evaluatePerThread(input, assignment);
    thread_times.resize(thread_units_.size(), 0.0);
    for (double thread_time : thread_times) {
        max_cost = std::max(max_cost, thread_time);
    }

    // 10% margin against total sequential compute cost. max_cost uses the
    // same partition cost model as the solver, including cross-thread waits.
    double parallel_overhead_factor = 1.10;
    return max_cost * parallel_overhead_factor < total_cost;
}

// ---------------------------------------------------------------------------
// Cluster affinity (tight-connection grouping)
// ---------------------------------------------------------------------------

void TickSimulation::buildClusterAffinity() {
    if (!dep_graph_.graph()) return;

    clusters_ = findTightCouplingClusters(*dep_graph_.graph());
    unit_to_cluster_ = clusters_.cluster_id;

    cluster_to_thread_.resize(clusters_.numClusters());
    size_t num_threads = normalizeThreadCount(config_.num_threads);
    config_.num_threads = num_threads;

    // Largest-first sort gives better load balancing under greedy assignment.
    std::vector<std::pair<size_t, size_t>> cluster_sizes;
    for (size_t i = 0; i < clusters_.numClusters(); ++i) {
        cluster_sizes.emplace_back(i, clusters_.clusters[i].size());
    }
    // Largest first; ties broken by cluster index so equal-size clusters order
    // identically across standard libraries (std::sort is unstable).
    std::sort(cluster_sizes.begin(), cluster_sizes.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });

    std::vector<size_t> thread_load(num_threads, 0);

    for (const auto& [cluster_idx, cluster_size] : cluster_sizes) {
        size_t min_thread = 0;
        size_t min_load = thread_load[0];
        for (size_t t = 1; t < num_threads; ++t) {
            if (thread_load[t] < min_load) {
                min_load = thread_load[t];
                min_thread = t;
            }
        }

        cluster_to_thread_[cluster_idx] = min_thread;
        thread_load[min_thread] += cluster_size;
    }

    thread_units_.resize(num_threads);
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        size_t cluster = unit_to_cluster_[i];
        size_t thread = cluster_to_thread_[cluster];
        thread_units_[thread].push_back(i);
    }

    optimizeConnectionQueuesForThreads();

    has_tight_inter_cluster_ = hasTightInterClusterConnections();

    if (observe_ctx_) {
        observe::log_info<"Cluster affinity: {} clusters across {} threads">(
            observe_ctx_, clusters_.numClusters(), num_threads);
        for (size_t c = 0; c < clusters_.numClusters(); ++c) {
            std::string names = buildUnitNameList_(clusters_.clusters[c]);
            observe::log_info<"  Cluster {}: {} units -> thread {} [{}]">(
                observe_ctx_, c, clusters_.clusters[c].size(), cluster_to_thread_[c],
                names.c_str());
        }
        for (size_t t = 0; t < num_threads; ++t) {
            std::string names = buildUnitNameList_(thread_units_[t]);
            observe::log_info<"  Thread {}: {} units [{}]">(observe_ctx_, t,
                                                            thread_units_[t].size(), names.c_str());
        }
        observe::log_info<"  Tight inter-cluster: {}">(observe_ctx_,
                                                       has_tight_inter_cluster_ ? "yes" : "no");
    }

    // Gate parallelism by both load balance and minimum work per active
    // worker — prevents overhead from swamping small topologies on many
    // threads.
    size_t max_thread_units = 0;
    size_t active_threads = 0;
    for (const auto& tu : thread_units_) {
        max_thread_units = std::max(max_thread_units, tu.size());
        if (!tu.empty()) {
            active_threads++;
        }
    }

    const bool balanced = (max_thread_units * 2 <= units_.size());
    const bool enough_work_per_active_thread =
        active_threads > 0 && (units_.size() >= active_threads * 3);
    parallel_beneficial_ = balanced && enough_work_per_active_thread;

    if (observe_ctx_) {
        observe::log_info<
            "  Parallel beneficial: {} (max {}/{} units on one thread, active_threads={})">(
            observe_ctx_, parallel_beneficial_ ? "yes" : "no", max_thread_units, units_.size(),
            active_threads);
    }
}

bool TickSimulation::hasTightInterClusterConnections() const {
    std::unordered_map<Unit*, size_t> unit_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    for (const auto* conn : connections_) {
        if (conn->delay() != 0) continue;

        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;

        auto src_it = unit_to_idx.find(src);
        auto dst_it = unit_to_idx.find(dst);
        if (src_it == unit_to_idx.end() || dst_it == unit_to_idx.end()) continue;

        if (unit_to_cluster_[src_it->second] != unit_to_cluster_[dst_it->second]) {
            return true;
        }
    }
    return false;
}

}  // namespace chronon::sender
