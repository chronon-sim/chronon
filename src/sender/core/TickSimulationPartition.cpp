// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// TickSimulation partitioning methods: cost-aware weighted partitioning,
/// cluster affinity assignment, queue optimization, cross-thread dependency
/// analysis, and dynamic rebalancing.

#include <algorithm>
#include <cstdio>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "TickSimulation.hpp"

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
        // Store directed edge only. Bidirectional communication naturally
        // produces edges in both directions because separate Connection
        // objects exist for each. Adding a synthetic reverse entry would
        // double-count bus connections (wakeup/flush buses expand to N*M
        // Connections, each of which would otherwise create 2 adjacency
        // entries instead of 1).
        auto& fwd = pair_info[{s, d}];
        fwd.first++;
        fwd.second = (fwd.second == 0) ? conn->delay() : std::min(fwd.second, conn->delay());
    }

    for (auto& [key, info] : pair_info) {
        input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }
}

// ---------------------------------------------------------------------------
// Cost-aware thread assignment
// ---------------------------------------------------------------------------

void TickSimulation::assignThreadsDeterministic_() {
    size_t num_threads = normalizeThreadCount(config_.num_threads);
    config_.num_threads = num_threads;

    // Zero sync cost + uniform unit costs makes the initial partition
    // purely topology-driven and fully deterministic (no wall-clock
    // measurement noise). Dynamic rebalance calibrates real platform
    // metrics and unit costs from the first sample batch.
    platform_metrics_ = PlatformMetrics{};

    unit_costs_.assign(unit_ptrs_.size(), 1.0);

    if (observe_ctx_) {
        observe::log_info<"Uniform-cost partitioning: unit_cost=1.0, sync_cost=0 ({} units)">(
            observe_ctx_, unit_costs_.size());
    }

    applyClusteredThreadAssignment_(num_threads);
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

    applyClusteredThreadAssignment_(num_threads);
}

// ---------------------------------------------------------------------------
// Clustered thread assignment
// ---------------------------------------------------------------------------

void TickSimulation::applyClusteredThreadAssignment_(size_t num_threads) {
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
    cluster_input.sync_cost_ns = platform_metrics_.atomic_roundtrip_ns;
    cluster_input.critical_path_weight = config_.sa_critical_path_weight;
    cluster_input.adjacency.resize(num_clusters);

    for (auto& [key, info] : cluster_pair_info) {
        cluster_input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }

    auto result = runPartitionSolver_(cluster_input);

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

    if (config_.enable_dynamic_rebalance) {
        thread_sampling_.resize(num_threads);
        for (size_t t = 0; t < num_threads; ++t) {
            size_t units_on_thread = thread_units_[t].size();
            thread_sampling_[t].tick_times.resize(units_on_thread * ThreadSamplingState::RING_SIZE,
                                                  0);
            thread_sampling_[t].write_idx = 0;
            thread_sampling_[t].sample_count = 0;
        }
    }

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

bool TickSimulation::parallelBeneficialWeighted_() const noexcept {
    if (thread_units_.empty() || unit_costs_.empty()) {
        return false;
    }

    double max_cost = 0.0;
    double total_cost = 0.0;
    size_t active_threads = 0;

    for (const auto& tu : thread_units_) {
        if (tu.empty()) continue;
        active_threads++;
        double thread_cost = 0.0;
        for (size_t u : tu) {
            thread_cost += unit_costs_[u];
        }
        max_cost = std::max(max_cost, thread_cost);
        total_cost += thread_cost;
    }

    if (active_threads < 2) return false;

    // 10% sync-overhead margin against total sequential cost. The earlier
    // (max < 1.5 * avg) heuristic rejected parallel runs that delivered
    // >3x speedup at moderate imbalance.
    double parallel_overhead_factor = 1.10;
    return max_cost * parallel_overhead_factor < total_cost;
}

// ---------------------------------------------------------------------------
// Dynamic rebalancing
// ---------------------------------------------------------------------------

bool TickSimulation::shouldRebalance_() const {
    if (thread_sampling_.empty()) return false;

    // Aggregate sampled work per thread. Averaging across all samples on a
    // thread hides imbalance when one stream owns more active units; sum
    // per-unit averages instead.
    std::vector<double> thread_times(thread_units_.size(), 0.0);
    size_t active_threads = 0;

    for (size_t t = 0; t < thread_units_.size(); ++t) {
        const auto& state = thread_sampling_[t];
        if (state.sample_count == 0 || thread_units_[t].empty()) continue;

        double total = 0.0;
        size_t sampled_units = 0;
        for (size_t u_idx = 0; u_idx < thread_units_[t].size(); ++u_idx) {
            size_t base = u_idx * ThreadSamplingState::RING_SIZE;
            if (base >= state.tick_times.size()) continue;

            double unit_sum = 0.0;
            size_t unit_count = 0;
            size_t ring_end =
                std::min(base + ThreadSamplingState::RING_SIZE, state.tick_times.size());
            for (size_t i = base; i < ring_end; ++i) {
                if (state.tick_times[i] > 0) {
                    unit_sum += static_cast<double>(state.tick_times[i]);
                    unit_count++;
                }
            }
            if (unit_count == 0) continue;
            total += unit_sum / static_cast<double>(unit_count);
            sampled_units++;
        }
        if (sampled_units == 0) continue;

        active_threads++;
        thread_times[t] = total;
    }

    if (active_threads < 2) return false;

    double max_time = *std::max_element(thread_times.begin(), thread_times.end());
    double total_time = 0.0;
    for (double t : thread_times) {
        total_time += t;
    }
    double avg_time = total_time / static_cast<double>(active_threads);

    return avg_time > 0 && (max_time / avg_time) > config_.rebalance_imbalance_threshold;
}

bool TickSimulation::performRebalance_() {
    if (!thread_sampling_.empty()) {
        for (size_t t = 0; t < thread_units_.size(); ++t) {
            const auto& state = thread_sampling_[t];
            if (state.sample_count == 0) continue;

            for (size_t u_idx = 0; u_idx < thread_units_[t].size(); ++u_idx) {
                size_t u = thread_units_[t][u_idx];
                size_t base = u_idx * ThreadSamplingState::RING_SIZE;
                if (base >= state.tick_times.size()) continue;

                double sum = 0.0;
                size_t count = 0;
                size_t ring_end =
                    std::min(base + ThreadSamplingState::RING_SIZE, state.tick_times.size());
                for (size_t i = base; i < ring_end; ++i) {
                    if (state.tick_times[i] > 0) {
                        sum += static_cast<double>(state.tick_times[i]);
                        count++;
                    }
                }
                if (count > 0 && u < unit_costs_.size()) {
                    unit_costs_[u] = sum / static_cast<double>(count);
                }
            }
        }
    }

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    const size_t num_clusters = clusters_.numClusters();
    std::vector<double> cluster_costs(num_clusters, 0.0);
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        size_t cluster = unit_to_cluster_[i];
        if (cluster < cluster_costs.size()) {
            cluster_costs[cluster] += unit_costs_[i];
        }
    }

    PartitionInput input;
    input.num_units = num_clusters;
    input.num_threads = thread_units_.size();
    input.unit_cost_ns = cluster_costs;
    input.sync_cost_ns = platform_metrics_.atomic_roundtrip_ns;
    input.critical_path_weight = config_.sa_critical_path_weight;
    input.adjacency.resize(num_clusters);

    std::unordered_map<PairKey, std::pair<size_t, uint32_t>, PairHash> cluster_pair_info;
    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;
        auto si = unit_ptr_to_idx.find(src);
        auto di = unit_ptr_to_idx.find(dst);
        if (si == unit_ptr_to_idx.end() || di == unit_ptr_to_idx.end()) continue;

        size_t sc = unit_to_cluster_[si->second];
        size_t dc = unit_to_cluster_[di->second];
        if (sc == dc) continue;

        auto& fwd = cluster_pair_info[{sc, dc}];
        fwd.first++;
        fwd.second = (fwd.second == 0) ? conn->delay() : std::min(fwd.second, conn->delay());
    }
    for (auto& [key, info] : cluster_pair_info) {
        input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }

    size_t num_threads = thread_units_.size();
    std::vector<size_t> old_assignment = cluster_to_thread_;
    double old_max_time = WeightedPartitioner::evaluatePartition(input, old_assignment);

    auto old_thread_costs = WeightedPartitioner::evaluatePerThread(input, old_assignment);
    old_thread_costs.resize(num_threads, 0.0);
    std::vector<std::string> old_thread_names(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        old_thread_names[t] = buildUnitNameList_(thread_units_[t]);
    }

    auto result = runRebalanceSolver_(input);

    double gain = 0.0;
    if (old_max_time > 0.0) {
        gain = (old_max_time - result.estimated_max_thread_time_ns) / old_max_time;
    }

    if (config_.rebalance_min_gain > 0.0 && gain < config_.rebalance_min_gain) {
        if (observe_ctx_) {
            observe::log_info<"Dynamic rebalance SKIPPED (gain={}% < min_gain={}%)">(
                observe_ctx_, static_cast<uint64_t>(gain * 100),
                static_cast<uint64_t>(config_.rebalance_min_gain * 100));
        }
        return false;
    }

    std::ostringstream rebalance_detail;
    rebalance_detail << "rebalance=" << (rebalance_count_ + 1)
                     << " imbalance=" << result.imbalance_ratio << " predicted_gain=" << gain;
    size_t migration_count = 0;
    for (size_t c = 0; c < num_clusters; ++c) {
        if (old_assignment[c] == result.unit_to_thread[c]) continue;
        migration_count++;
        if (migration_count <= 16) {
            rebalance_detail << " C" << c << "(" << buildUnitNameList_(clusters_.clusters[c])
                             << ") T" << old_assignment[c] << "->T" << result.unit_to_thread[c];
        }
    }
    if (migration_count > 16) {
        rebalance_detail << " +" << (migration_count - 16) << " more";
    }
    last_rebalance_detail_ = rebalance_detail.str();

    if (observe_ctx_) {
        observe::log_info<"Dynamic rebalance #{}: imbalance={}%, predicted_gain={}%">(
            observe_ctx_, rebalance_count_ + 1, static_cast<uint64_t>(result.imbalance_ratio * 100),
            static_cast<uint64_t>(gain * 100));

        observe::log_info<"  OLD assignment:">(observe_ctx_);
        for (size_t t = 0; t < num_threads; ++t) {
            if (old_thread_names[t].empty()) continue;
            observe::log_info<"    Thread {}: cost={}ns [{}]">(
                observe_ctx_, t, static_cast<uint64_t>(old_thread_costs[t]),
                old_thread_names[t].c_str());
        }
    }

    for (size_t c = 0; c < num_clusters; ++c) {
        cluster_to_thread_[c] = result.unit_to_thread[c];
    }

    thread_units_.assign(num_threads, {});
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        size_t cluster = unit_to_cluster_[i];
        if (cluster >= cluster_to_thread_.size()) continue;
        size_t thread = cluster_to_thread_[cluster];
        if (thread < thread_units_.size()) {
            thread_units_[thread].push_back(i);
        }
    }

    // Queue adapters are intentionally NOT reconfigured during dynamic
    // rebalance: InPorts may already hold messages for future cycles, and
    // replacing the adapter would drop them. When dynamic rebalance is
    // enabled, initialization picks migration-stable queue adapters.
    buildCrossThreadDependencies();

    initProgressSync();
    installMPSCProducerProgress_();
    initTimelineTraceScratch_();

    thread_sampling_.resize(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        auto& state = thread_sampling_[t];
        state.tick_times.assign(thread_units_[t].size() * ThreadSamplingState::RING_SIZE, 0);
        state.write_idx = 0;
        state.sample_count = 0;
    }

    rebalance_count_++;

    if (observe_ctx_) {
        auto new_thread_costs =
            WeightedPartitioner::evaluatePerThread(input, result.unit_to_thread);
        new_thread_costs.resize(num_threads, 0.0);

        observe::log_info<"  NEW assignment:">(observe_ctx_);
        for (size_t t = 0; t < num_threads; ++t) {
            if (thread_units_[t].empty()) continue;
            std::string names = buildUnitNameList_(thread_units_[t]);
            observe::log_info<"    Thread {}: cost={}ns [{}]">(
                observe_ctx_, t, static_cast<uint64_t>(new_thread_costs[t]), names.c_str());
        }

        for (size_t c = 0; c < num_clusters; ++c) {
            if (old_assignment[c] != result.unit_to_thread[c]) {
                std::string names = buildUnitNameList_(clusters_.clusters[c]);
                observe::log_info<"  Migrated cluster {} [{}] : T{} -> T{}">(
                    observe_ctx_, c, names.c_str(), old_assignment[c], result.unit_to_thread[c]);
            }
        }
    }

    return true;
}

void TickSimulation::recordTickSample_(size_t thread_idx, size_t unit_local_idx, uint64_t ticks) {
    if (thread_idx >= thread_sampling_.size()) return;
    auto& state = thread_sampling_[thread_idx];
    size_t slot = unit_local_idx * ThreadSamplingState::RING_SIZE +
                  (state.write_idx % ThreadSamplingState::RING_SIZE);
    if (slot < state.tick_times.size()) {
        state.tick_times[slot] = ticks;
    }
    state.write_idx++;
    state.sample_count++;
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
    std::sort(cluster_sizes.begin(), cluster_sizes.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

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
