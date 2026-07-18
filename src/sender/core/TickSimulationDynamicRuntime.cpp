// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Epoch-free dynamic migration runtime ownership and request bookkeeping.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "TickSimulation.hpp"

namespace chronon::sender {

namespace {
constexpr uint64_t kNoMigrationCycle = std::numeric_limits<uint64_t>::max();
}

void TickSimulation::clearDynamicMigrationRequest_() {
    migration_request_.cluster.store(SIZE_MAX, std::memory_order_relaxed);
    migration_request_.source_thread.store(SIZE_MAX, std::memory_order_relaxed);
    migration_request_.target_thread.store(SIZE_MAX, std::memory_order_relaxed);
    migration_request_.fence_cycle.store(0, std::memory_order_relaxed);
    migration_request_.state.store(static_cast<uint8_t>(MigrationRequestState::None),
                                   std::memory_order_release);
}

void TickSimulation::initDynamicMigrationRuntime_() {
    const size_t num_clusters = clusters_.numClusters();
    if (num_clusters == 0 || thread_units_.empty()) return;
    const size_t num_threads = thread_units_.size();

    const bool rebuild_clusters =
        dynamic_runtime_cluster_count_ != num_clusters || !cluster_runtime_owner_;
    const bool rebuild_threads =
        dynamic_runtime_thread_count_ != num_threads || !dynamic_thread_floor_wait_ns_;
    const bool reset_runtime = rebuild_clusters || rebuild_threads;

    if (rebuild_clusters) {
        cluster_runtime_owner_ = std::make_unique<std::atomic<size_t>[]>(num_clusters);
        cluster_execution_owner_ = std::make_unique<std::atomic<size_t>[]>(num_clusters);
        cluster_migration_pending_ = std::make_unique<std::atomic<uint8_t>[]>(num_clusters);
        cluster_sample_time_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        cluster_sample_count_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        cluster_active_sample_count_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        dynamic_cluster_blocked_wait_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        dynamic_cluster_blocker_wait_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        dynamic_cluster_last_migration_cycle_.assign(num_clusters, kNoMigrationCycle);
        dynamic_cluster_last_source_thread_.assign(num_clusters, SIZE_MAX);
        dynamic_cluster_last_target_thread_.assign(num_clusters, SIZE_MAX);
        dynamic_runtime_cluster_count_ = num_clusters;
    }
    if (rebuild_threads) {
        dynamic_thread_floor_wait_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_threads);
        dynamic_thread_dep_wait_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_threads);
        dynamic_thread_no_ready_wait_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_threads);
        dynamic_runtime_thread_count_ = num_threads;
    }

    for (size_t c = 0; c < num_clusters; ++c) {
        cluster_execution_owner_[c].store(SIZE_MAX, std::memory_order_relaxed);
        if (!reset_runtime) continue;
        const size_t owner = c < cluster_to_thread_.size() ? cluster_to_thread_[c] : 0;
        cluster_runtime_owner_[c].store(owner, std::memory_order_relaxed);
        cluster_migration_pending_[c].store(0, std::memory_order_relaxed);
        cluster_sample_time_ns_[c].store(0, std::memory_order_relaxed);
        cluster_sample_count_[c].store(0, std::memory_order_relaxed);
        cluster_active_sample_count_[c].store(0, std::memory_order_relaxed);
        dynamic_cluster_blocked_wait_ns_[c].store(0, std::memory_order_relaxed);
        dynamic_cluster_blocker_wait_ns_[c].store(0, std::memory_order_relaxed);
    }
    if (reset_runtime) {
        for (size_t t = 0; t < num_threads; ++t) {
            dynamic_thread_floor_wait_ns_[t].store(0, std::memory_order_relaxed);
            dynamic_thread_dep_wait_ns_[t].store(0, std::memory_order_relaxed);
            dynamic_thread_no_ready_wait_ns_[t].store(0, std::memory_order_relaxed);
        }
        cluster_assignment_generation_.store(1, std::memory_order_release);
        clearDynamicMigrationRequest_();
    }
}

void TickSimulation::rebuildThreadUnitsFromClusterOwners_() {
    if (!cluster_runtime_owner_ || dynamic_runtime_cluster_count_ == 0 || thread_units_.empty()) {
        return;
    }

    const size_t num_threads = thread_units_.size();
    thread_units_.assign(num_threads, {});
    thread_clusters_.assign(num_threads, {});

    if (cluster_to_thread_.size() < dynamic_runtime_cluster_count_) {
        cluster_to_thread_.resize(dynamic_runtime_cluster_count_, 0);
    }

    for (size_t c = 0; c < dynamic_runtime_cluster_count_; ++c) {
        size_t owner = cluster_runtime_owner_[c].load(std::memory_order_acquire);
        if (owner >= num_threads) owner = 0;
        cluster_to_thread_[c] = owner;
        thread_clusters_[owner].push_back(c);
        if (c >= clusters_.clusters.size()) continue;
        for (size_t unit_idx : clusters_.clusters[c]) {
            if (unit_idx < unit_ptrs_.size()) {
                thread_units_[owner].push_back(unit_idx);
            }
        }
    }

    thread_unit_ptrs_.assign(num_threads, {});
    for (size_t t = 0; t < num_threads; ++t) {
        thread_unit_ptrs_[t].reserve(thread_units_[t].size());
        for (size_t idx : thread_units_[t]) {
            thread_unit_ptrs_[t].push_back(unit_ptrs_[idx]);
        }
    }

    std::vector<std::unordered_map<size_t, size_t>> thread_unit_pos(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        for (size_t pos = 0; pos < thread_units_[t].size(); ++pos) {
            thread_unit_pos[t][thread_units_[t][pos]] = pos;
        }
    }

    cluster_thread_unit_positions_.assign(dynamic_runtime_cluster_count_, {});
    for (size_t c = 0; c < dynamic_runtime_cluster_count_; ++c) {
        size_t owner = cluster_runtime_owner_[c].load(std::memory_order_acquire);
        if (owner >= num_threads || c >= clusters_.clusters.size()) continue;
        auto& positions = cluster_thread_unit_positions_[c];
        positions.reserve(clusters_.clusters[c].size());
        for (size_t unit_idx : clusters_.clusters[c]) {
            auto it = thread_unit_pos[owner].find(unit_idx);
            positions.push_back(it == thread_unit_pos[owner].end() ? 0 : it->second);
        }
    }
}

bool TickSimulation::forceEpochFreeMigrationAtBoundary_(Unit* unit, size_t target_thread) {
    if (!unit || !initialized_ || !config_.enable_parallel || !config_.enable_lookahead ||
        !config_.enable_epoch_free_lookahead || !config_.enable_dynamic_rebalance ||
        !shouldUseParallelExecution_() ||
        epoch_free_dynamic_runtime_active_.load(std::memory_order_acquire)) {
        return false;
    }

    initDynamicMigrationRuntime_();
    if (!cluster_runtime_owner_ || !cluster_execution_owner_ ||
        target_thread >= dynamic_runtime_thread_count_ ||
        migration_request_.state.load(std::memory_order_acquire) !=
            static_cast<uint8_t>(MigrationRequestState::None)) {
        return false;
    }

    size_t unit_index = SIZE_MAX;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        if (static_cast<Unit*>(unit_ptrs_[i]) == unit) {
            unit_index = i;
            break;
        }
    }
    if (unit_index == SIZE_MAX || unit_index >= unit_to_cluster_.size()) {
        return false;
    }

    const size_t cluster = unit_to_cluster_[unit_index];
    if (cluster >= dynamic_runtime_cluster_count_ ||
        cluster_execution_owner_[cluster].load(std::memory_order_acquire) != SIZE_MAX ||
        cluster_migration_pending_[cluster].load(std::memory_order_acquire) != 0) {
        return false;
    }

    const size_t source = cluster_runtime_owner_[cluster].load(std::memory_order_acquire);
    if (source >= dynamic_runtime_thread_count_ || source == target_thread) {
        return false;
    }

    // run() has joined every persistent worker before this seam is callable.
    // No remote observer exists, so relaxed publication is sufficient here;
    // the next worker launch acquires the ownership generation and owner slots.
    cluster_runtime_owner_[cluster].store(target_thread, std::memory_order_relaxed);
    if (cluster < dynamic_cluster_last_migration_cycle_.size()) {
        dynamic_cluster_last_migration_cycle_[cluster] = current_cycle_;
        dynamic_cluster_last_source_thread_[cluster] = source;
        dynamic_cluster_last_target_thread_[cluster] = target_thread;
    }
    cluster_assignment_generation_.fetch_add(1, std::memory_order_relaxed);
    ++rebalance_count_;
    cycles_since_last_actual_rebalance_ = 0;
    rebuildThreadUnitsFromClusterOwners_();
    return true;
}

}  // namespace chronon::sender
