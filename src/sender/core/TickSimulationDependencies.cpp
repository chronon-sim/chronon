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
#include <limits>
#include <unordered_map>
#include <vector>

#include "TickSimulation.hpp"

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

}  // namespace

void TickSimulation::buildCrossThreadDependencies() {
    const size_t num_clusters = clusters_.numClusters();
    thread_cross_deps_temp_.clear();
    thread_cross_deps_temp_.resize(num_clusters);
    has_zero_delay_cross_thread_cycle_ = false;
    if (num_clusters == 0) return;

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    std::vector<std::unordered_map<size_t, uint32_t>> min_delay(num_clusters);
    auto add_dep = [&](size_t cluster, size_t pred_cluster, uint32_t delay) {
        auto [it, inserted] = min_delay[cluster].try_emplace(pred_cluster, delay);
        if (!inserted && delay < it->second) {
            it->second = delay;
        }
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

        const size_t headroom = conn->crossThreadHeadroom();
        if (headroom != std::numeric_limits<size_t>::max() && headroom > 0) {
            const uint64_t safe_cap = static_cast<uint64_t>(headroom - 1);
            const auto delay = static_cast<uint32_t>(std::min<uint64_t>(safe_cap, UINT32_MAX));
            add_dep(src_cluster, dst_cluster, delay);
        }
    }

    has_zero_delay_cross_thread_cycle_ = hasZeroDelayCycle(min_delay);

    for (size_t c = 0; c < num_clusters; ++c) {
        for (auto& [src_cluster, delay] : min_delay[c]) {
            thread_cross_deps_temp_[c].push_back({src_cluster, delay});
        }
    }
}

void TickSimulation::collectMPSCInPorts_() {
    mpsc_inports_.clear();
    std::unordered_map<void*, bool> seen;
    for (auto* conn : connections_) {
        if (!conn->hasThreadQueueId()) continue;
        void* port_ptr = conn->destPortPtr();
        if (!port_ptr) continue;
        IArbitratablePort* arb = conn->registerOnDestMPSC();
        if (!arb) continue;
        if (seen.emplace(port_ptr, true).second) {
            mpsc_inports_.push_back(arb);
        }
    }
}

}  // namespace chronon::sender
