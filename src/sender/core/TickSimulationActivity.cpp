// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

/// @file
/// Cluster-local activity-scheduling summary initialization.

#include "TickSimulation.hpp"

namespace chronon::sender {

void TickSimulation::initClusterActivityScheduling_() {
    cluster_activity_scheduling_.reset();
    if (unit_ptrs_.empty()) return;
    const size_t cluster_count = std::max<size_t>(1, clusters_.numClusters());

    cluster_activity_scheduling_ =
        std::make_unique<detail::ActivitySchedulingState[]>(cluster_count);
    for (size_t cluster = 0; cluster < cluster_count; ++cluster) {
        cluster_activity_scheduling_[cluster].root = &any_activity_scheduling_;
    }
    for (size_t unit_idx = 0; unit_idx < unit_ptrs_.size(); ++unit_idx) {
        const size_t cluster = unit_idx < unit_to_cluster_.size() ? unit_to_cluster_[unit_idx] : 0;
        if (cluster >= cluster_count) continue;
        unit_ptrs_[unit_idx]->bindActivitySchedulingState_(&cluster_activity_scheduling_[cluster]);
    }
}

uint64_t TickSimulation::computeIdleClusterTargetIfEnabled_(size_t cluster, uint64_t cycle,
                                                            uint64_t end_cycle,
                                                            uint64_t* predecessor_cache) const {
    // Keep the per-cluster acquire out of the all-active scheduler instruction
    // stream. A stale false only declines bulk idle; per-Unit ticks stay exact.
    if (cluster >= cluster_unit_ptrs_.size() ||
        !cluster_activity_scheduling_[cluster].enabled.load(std::memory_order_acquire)) {
        return cycle;
    }
    return computeIdleClusterTarget_(cluster, cycle, end_cycle, predecessor_cache);
}

}  // namespace chronon::sender
