// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Runtime cost sampling and activation-rate normalization.

#include <algorithm>
#include <atomic>
#include <limits>

#include "TickSimulation.hpp"

namespace chronon::sender {

namespace {
constexpr uint64_t kDynamicClusterMinSamples = 4;
}

void TickSimulation::recordClusterTickSample_(size_t cluster, uint64_t ticks, bool active) {
    if (!cluster_sample_time_ns_ || cluster >= dynamic_runtime_cluster_count_) return;
    cluster_sample_time_ns_[cluster].fetch_add(ticks, std::memory_order_relaxed);
    cluster_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
    if (active) {
        cluster_active_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
    }
}

void TickSimulation::recordDynamicUnitTickSample_(size_t unit, uint64_t ticks, bool active) {
    if (unit >= dynamic_runtime_unit_count_) return;
    auto* time = active ? dynamic_unit_active_sample_time_ns_.get()
                        : dynamic_unit_inactive_sample_time_ns_.get();
    auto* count = active ? dynamic_unit_active_sample_count_.get()
                         : dynamic_unit_inactive_sample_count_.get();
    if (!time || !count) return;
    time[unit].fetch_add(ticks, std::memory_order_relaxed);
    count[unit].fetch_add(1, std::memory_order_relaxed);
}

void TickSimulation::recordDynamicWaitSample_(size_t thread_idx, const BlockedClusterInfo& blocker,
                                              uint64_t wait_ns) {
    if (thread_idx >= dynamic_runtime_thread_count_ || !dynamic_thread_floor_wait_ns_) return;
    if (blocker.cluster == SIZE_MAX) {
        dynamic_thread_no_ready_wait_ns_[thread_idx].fetch_add(wait_ns, std::memory_order_relaxed);
    } else if (blocker.pred_cluster == SIZE_MAX) {
        dynamic_thread_floor_wait_ns_[thread_idx].fetch_add(wait_ns, std::memory_order_relaxed);
    } else {
        dynamic_thread_dep_wait_ns_[thread_idx].fetch_add(wait_ns, std::memory_order_relaxed);
        if (dynamic_cluster_blocked_wait_ns_ && blocker.cluster < dynamic_runtime_cluster_count_) {
            dynamic_cluster_blocked_wait_ns_[blocker.cluster].fetch_add(wait_ns,
                                                                        std::memory_order_relaxed);
        }
        if (dynamic_cluster_blocker_wait_ns_ &&
            blocker.pred_cluster < dynamic_runtime_cluster_count_) {
            dynamic_cluster_blocker_wait_ns_[blocker.pred_cluster].fetch_add(
                wait_ns, std::memory_order_relaxed);
        }
    }
}

void TickSimulation::refineDynamicWaitBlocker_(size_t thread_idx,
                                               const std::vector<size_t>& owned_clusters,
                                               bool stable_sweep, uint64_t end_cycle,
                                               uint64_t* predecessor_cache,
                                               BlockedClusterInfo& blocker) const {
    // The common readiness sweep may stop at the first proof of blocking,
    // whereas sampled attribution feeds migration scoring and must retain the
    // previous maximum-deficit semantics.
    BlockedClusterInfo sampled_blocker{};
    for (size_t cluster : owned_clusters) {
        const uint64_t cycle =
            thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed);
        if (cycle >= end_cycle) continue;
        if (!stable_sweep &&
            (!cluster_runtime_owner_ ||
             cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx)) {
            continue;
        }
        if (!stable_sweep && dynamicMigrationBlocksCluster_(cluster, cycle)) continue;

        BlockedClusterInfo candidate{};
        if (!clusterCanAdvance_(cluster, cycle, candidate, predecessor_cache,
                                /*stop_on_first_blocker=*/false) &&
            candidate.deficit > sampled_blocker.deficit) {
            sampled_blocker = candidate;
        }
    }
    if (sampled_blocker.cluster != SIZE_MAX) blocker = sampled_blocker;
}

TickSimulation::DynamicRuntimeCostEstimate TickSimulation::dynamicUnitRuntimeCost_(
    size_t unit, double fallback) const {
    DynamicRuntimeCostEstimate estimate;
    estimate.cost = std::max(0.001, fallback);
    estimate.low_frequency = true;
    if (unit >= dynamic_runtime_unit_count_ || !dynamic_unit_observed_cycles_) return estimate;

    const uint64_t cycles = dynamic_unit_observed_cycles_[unit].load(std::memory_order_relaxed);
    if (cycles == 0) return estimate;
    const uint64_t active_ticks =
        std::min(cycles, dynamic_unit_observed_active_ticks_[unit].load(std::memory_order_relaxed));
    const uint64_t active_samples =
        dynamic_unit_active_sample_count_[unit].load(std::memory_order_relaxed);
    const uint64_t inactive_samples =
        dynamic_unit_inactive_sample_count_[unit].load(std::memory_order_relaxed);
    const bool needs_active = active_ticks != 0;
    const bool needs_inactive = active_ticks != cycles;

    uint64_t confidence = std::numeric_limits<uint64_t>::max();
    if (needs_active) confidence = std::min(confidence, active_samples);
    if (needs_inactive) confidence = std::min(confidence, inactive_samples);
    if (confidence == std::numeric_limits<uint64_t>::max()) confidence = 0;
    estimate.samples = confidence;
    estimate.ready = (!needs_active || active_samples >= kDynamicClusterMinSamples) &&
                     (!needs_inactive || inactive_samples >= kDynamicClusterMinSamples);
    if (!estimate.ready) return estimate;

    // Active samples are deliberately taken on real executions, so their raw
    // mean is a conditional cost rather than a per-cycle cost. Recombine the
    // independently measured active and inactive means with the exact activity
    // ratio accumulated over complete sampling windows.
    const double active_rate = static_cast<double>(active_ticks) / static_cast<double>(cycles);
    double cost = 0.0;
    if (needs_active) {
        const uint64_t total =
            dynamic_unit_active_sample_time_ns_[unit].load(std::memory_order_relaxed);
        cost += active_rate * static_cast<double>(total) / static_cast<double>(active_samples);
    }
    if (needs_inactive) {
        const uint64_t total =
            dynamic_unit_inactive_sample_time_ns_[unit].load(std::memory_order_relaxed);
        cost += (1.0 - active_rate) * static_cast<double>(total) /
                static_cast<double>(inactive_samples);
    }
    estimate.cost = std::max(0.001, cost);
    return estimate;
}

TickSimulation::DynamicRuntimeCostEstimate TickSimulation::dynamicClusterRuntimeCost_(
    size_t cluster) {
    DynamicRuntimeCostEstimate estimate;
    if (cluster >= clusters_.clusters.size()) return estimate;

    const auto load_unit_cost = [this](size_t unit) noexcept {
        if (unit >= unit_costs_.size()) return 1.0;
        return std::atomic_ref<double>(unit_costs_[unit]).load(std::memory_order_relaxed);
    };

    double fallback = 0.0;
    bool low_frequency = false;
    for (size_t unit : clusters_.clusters[cluster]) {
        fallback += load_unit_cost(unit);
        if (unit < unit_ptrs_.size() &&
            (unit_ptrs_[unit]->tickInterval() > 1 || unit_ptrs_[unit]->usesActivityScheduling())) {
            low_frequency = true;
        }
    }
    estimate.cost = std::max(0.001, fallback);
    estimate.low_frequency = low_frequency;

    if (!low_frequency) {
        if (!cluster_sample_count_ || cluster >= dynamic_runtime_cluster_count_) return estimate;
        const uint64_t samples = cluster_sample_count_[cluster].load(std::memory_order_relaxed);
        estimate.samples = samples;
        estimate.ready = samples != 0;
        if (estimate.ready) {
            const uint64_t total = cluster_sample_time_ns_[cluster].load(std::memory_order_relaxed);
            estimate.cost =
                std::max(0.001, static_cast<double>(total) / static_cast<double>(samples));
        }
        return estimate;
    }

    double measured_cost = 0.0;
    uint64_t confidence = std::numeric_limits<uint64_t>::max();
    bool ready = !clusters_.clusters[cluster].empty();
    for (size_t unit : clusters_.clusters[cluster]) {
        const double unit_fallback = load_unit_cost(unit);
        const auto unit_estimate = dynamicUnitRuntimeCost_(unit, unit_fallback);
        measured_cost += unit_estimate.cost;
        confidence = std::min(confidence, unit_estimate.samples);
        ready = ready && unit_estimate.ready;
    }
    estimate.cost = std::max(0.001, measured_cost);
    estimate.samples = confidence == std::numeric_limits<uint64_t>::max() ? 0 : confidence;
    estimate.ready = ready;
    return estimate;
}

}  // namespace chronon::sender
