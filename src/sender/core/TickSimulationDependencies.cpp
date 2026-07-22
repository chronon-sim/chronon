// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Cross-cluster progress dependency construction for lookahead execution.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <unordered_map>
#include <vector>

#include "TickSimulation.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"
#include "sender/schedule/WeightedDependencyReduction.hpp"

namespace chronon::sender {

namespace {

bool hasZeroDelayCycle(
    const std::vector<std::unordered_map<size_t, uint32_t>>& cluster_min_delays) {
    enum class VisitState : uint8_t { Unvisited, Visiting, Done };

    std::vector<VisitState> state(cluster_min_delays.size(), VisitState::Unvisited);

    auto dfs = [&](auto&& self, size_t cluster) -> bool {
        state[cluster] = VisitState::Visiting;
        for (const auto& [pred_cluster, delay] : cluster_min_delays[cluster]) {
            if (delay != 0 || pred_cluster >= cluster_min_delays.size()) continue;
            if (state[pred_cluster] == VisitState::Visiting) return true;
            if (state[pred_cluster] == VisitState::Unvisited && self(self, pred_cluster)) {
                return true;
            }
        }
        state[cluster] = VisitState::Done;
        return false;
    };

    for (size_t c = 0; c < cluster_min_delays.size(); ++c) {
        if (state[c] == VisitState::Unvisited && dfs(dfs, c)) return true;
    }
    return false;
}

bool transitiveDependencyPruneEnabled() noexcept {
    const char* value = std::getenv("CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE");
    // Pruning is the feature-branch default. An exact zero retains the legacy
    // dependency graph for A/B testing without requiring a second binary.
    return value == nullptr || value[0] != '0' || value[1] != '\0';
}

}  // namespace

void TickSimulation::buildCrossThreadDependencies() {
    const size_t num_clusters = clusters_.numClusters();
    thread_cross_deps_temp_.clear();
    thread_cross_deps_temp_.resize(num_clusters);
    dynamic_rebalance_adjacency_.clear();
    has_zero_delay_cross_thread_cycle_ = false;
    if (num_clusters == 0) return;

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    std::vector<std::unordered_map<size_t, uint32_t>> min_delay(num_clusters);
    std::vector<epoch_free_cost::RuntimeDependency> runtime_dependencies;
    if (config_.enable_dynamic_rebalance) {
        runtime_dependencies.reserve(connections_.size());
    }
    auto add_dep = [&](size_t cluster, size_t pred_cluster, uint32_t delay) {
        auto [it, inserted] = min_delay[cluster].try_emplace(pred_cluster, delay);
        if (!inserted && delay < it->second) {
            it->second = delay;
        }
    };
    auto add_rebalance_dep = [&](size_t dependent, size_t predecessor, uint32_t delay) {
        if (!config_.enable_dynamic_rebalance) return;
        runtime_dependencies.push_back({dependent, predecessor, delay, 1});
    };

    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;

        auto src_it = unit_ptr_to_idx.find(src);
        auto dst_it = unit_ptr_to_idx.find(dst);
        if (src_it == unit_ptr_to_idx.end() || dst_it == unit_ptr_to_idx.end()) continue;

        size_t src_cluster = unit_to_cluster_[src_it->second];
        size_t dst_cluster = unit_to_cluster_[dst_it->second];

        if (src_cluster == dst_cluster) continue;

        add_dep(dst_cluster, src_cluster, conn->delay());
        add_rebalance_dep(dst_cluster, src_cluster, conn->delay());

        const size_t headroom = conn->crossThreadHeadroom();
        if (headroom != std::numeric_limits<size_t>::max() && headroom > 0) {
            const uint64_t safe_cap = static_cast<uint64_t>(headroom - 1);
            const auto delay = static_cast<uint32_t>(std::min<uint64_t>(safe_cap, UINT32_MAX));
            // A finite queue still couples producer and consumer placement
            // when the global floor makes this reverse edge unnecessary as an
            // execution gate. Preserve that relationship for migration cost.
            add_rebalance_dep(src_cluster, dst_cluster, delay);
            // The global lookahead floor already bounds producer run-ahead
            // when it can retain fewer cycles than this connection. Install
            // an execution reverse edge only when the connection is tighter.
            if (config_.max_lookahead_cycles == 0 || headroom <= config_.max_lookahead_cycles) {
                add_dep(src_cluster, dst_cluster, delay);
            }
        }
    }

    // Execution-mode selection must use the original pair-minimum graph. A
    // reduction must never hide a zero-delay cycle and enable lookahead.
    has_zero_delay_cross_thread_cycle_ = hasZeroDelayCycle(min_delay);

    if (transitiveDependencyPruneEnabled()) {
        std::vector<weighted_dependency_reduction::Edge> edges;
        size_t total_edges = 0;
        for (const auto& dependencies : min_delay) total_edges += dependencies.size();
        edges.reserve(total_edges);
        for (size_t cluster = 0; cluster < num_clusters; ++cluster) {
            for (const auto& [pred_cluster, delay] : min_delay[cluster]) {
                edges.push_back({cluster, pred_cluster, delay});
            }
        }

        const auto reduction = weighted_dependency_reduction::reduce(num_clusters, edges);
        for (const auto& edge : reduction.retained) {
            thread_cross_deps_temp_[edge.dependent].push_back(
                {edge.predecessor, static_cast<uint32_t>(edge.delay)});
        }

        if (observe_ctx_) {
            observe::log_info<"Transitive dependency pruning: {} -> {} cluster dependencies">(
                observe_ctx_, edges.size(), reduction.retained.size());
        }
    } else {
        for (size_t c = 0; c < num_clusters; ++c) {
            for (const auto& [src_cluster, delay] : min_delay[c]) {
                thread_cross_deps_temp_[c].push_back({src_cluster, delay});
            }
        }
    }

    if (!config_.enable_dynamic_rebalance) return;

    // Rebalance scores physical synchronization and finite-headroom coupling.
    // Preserve direct physical edges and their multiplicity: an execution edge
    // can be transitively redundant while its queue and atomic cost remain real.
    dynamic_rebalance_adjacency_ =
        epoch_free_cost::buildRuntimeAdjacency(num_clusters, runtime_dependencies);
}

void TickSimulation::collectMultiProducerPorts_() {
    multi_producer_ports_.clear();
    std::unordered_map<void*, bool> seen;
    for (auto* conn : connections_) {
        if (!conn->hasThreadQueueId()) continue;
        void* port_ptr = conn->destPortPtr();
        if (!port_ptr) continue;
        IMultiProducerPort* port = conn->registerOnDestMPSC();
        if (!port) continue;
        if (seen.emplace(port_ptr, true).second) {
            multi_producer_ports_.push_back(port);
        }
    }
}

}  // namespace chronon::sender
