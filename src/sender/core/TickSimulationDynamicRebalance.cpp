// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Epoch-free dynamic rebalance runtime: local migration safe points,
/// one-cluster migration commits, and dynamic worker ownership refresh.

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"
#include "sender/schedule/SchedulerTimelineStyle.hpp"

namespace chronon::sender {

namespace {
constexpr uint64_t kFloorRefreshSpinMask = 0xFF;
constexpr uint64_t kDynamicClusterMinSamples = 4;
constexpr uint64_t kNoMigrationCycle = std::numeric_limits<uint64_t>::max();

struct PairKey {
    size_t u, v;
    bool operator==(const PairKey& o) const { return u == o.u && v == o.v; }
};

struct PairHash {
    size_t operator()(const PairKey& k) const {
        return k.u ^ (k.v * 0x9e3779b97f4a7c15ULL + (k.u << 6) + (k.u >> 2));
    }
};

uint64_t saturatingCycleAdd(uint64_t base, uint64_t delta) noexcept {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return delta > max - base ? max : base + delta;
}

}  // namespace

void TickSimulation::recordClusterTickSample_(size_t cluster, uint64_t ticks, bool active) {
    if (!cluster_sample_time_ns_ || cluster >= dynamic_runtime_cluster_count_) return;
    cluster_sample_time_ns_[cluster].fetch_add(ticks, std::memory_order_relaxed);
    cluster_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
    if (active) {
        cluster_active_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
    }
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

void TickSimulation::resetDynamicSchedulerMarkers_() {
    dynamic_scheduler_marker_count_.store(0, std::memory_order_relaxed);
    dynamic_scheduler_marker_drops_.store(0, std::memory_order_relaxed);
}

void TickSimulation::recordDynamicSchedulerMarker_(std::string name, uint64_t cycle,
                                                   std::string detail) {
    if (!timeline_trace_.enabled()) return;
    const size_t slot = dynamic_scheduler_marker_count_.fetch_add(1, std::memory_order_relaxed);
    if (slot >= dynamic_scheduler_markers_.size()) {
        dynamic_scheduler_marker_drops_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto& marker = dynamic_scheduler_markers_[slot];
    marker.time = SchedulerTimelineTrace::Clock::now();
    marker.cycle = cycle;
    marker.name = std::move(name);
    marker.detail = std::move(detail);
}

void TickSimulation::flushDynamicSchedulerMarkers_() {
    if (!timeline_trace_.enabled()) {
        resetDynamicSchedulerMarkers_();
        return;
    }

    const size_t count = std::min(dynamic_scheduler_marker_count_.load(std::memory_order_relaxed),
                                  dynamic_scheduler_markers_.size());
    for (size_t i = 0; i < count; ++i) {
        const auto& marker = dynamic_scheduler_markers_[i];
        if (marker.name.empty()) continue;
        timeline_trace_.recordInstant(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                      marker.name, marker.cycle, marker.time, marker.detail);
    }

    const uint64_t drops = dynamic_scheduler_marker_drops_.load(std::memory_order_relaxed);
    if (drops > 0) {
        const auto now = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordInstant(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                      "Chronon epoch-free rebalance markers dropped",
                                      current_cycle_, now, "drops=" + std::to_string(drops));
    }

    resetDynamicSchedulerMarkers_();
}

bool TickSimulation::maybeRequestEpochFreeMigration_(uint64_t cycle) {
    if (!config_.enable_dynamic_rebalance || !cluster_runtime_owner_ ||
        dynamic_runtime_cluster_count_ == 0 || config_.num_threads < 2) {
        return false;
    }

    const uint64_t interval = std::max<uint64_t>(1, config_.rebalance_check_interval_cycles);
    const uint64_t floor_cycle = lookahead_floor_.load(std::memory_order_acquire);
    const uint64_t gate_cycle = std::max(floor_cycle, cycle);
    uint64_t next = next_dynamic_rebalance_check_cycle_.load(std::memory_order_relaxed);
    while (gate_cycle >= next) {
        if (next_dynamic_rebalance_check_cycle_.compare_exchange_weak(
                next, saturatingCycleAdd(gate_cycle, interval), std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            break;
        }
    }
    if (gate_cycle < next) {
        return false;
    }

    uint8_t expected = static_cast<uint8_t>(MigrationRequestState::None);
    if (!migration_request_.state.compare_exchange_strong(
            expected, static_cast<uint8_t>(MigrationRequestState::Requested),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return false;
    }

    const size_t num_threads = thread_units_.size();
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    std::vector<double> cluster_cost(num_clusters, 0.0);
    std::vector<uint64_t> cluster_samples(num_clusters, 0);
    std::vector<double> thread_cost(num_threads, 0.0);
    std::vector<size_t> thread_active_clusters(num_threads, 0);
    std::vector<uint64_t> thread_floor_wait(num_threads, 0);
    std::vector<uint64_t> thread_dep_wait(num_threads, 0);
    std::vector<uint64_t> thread_no_ready_wait(num_threads, 0);
    std::vector<uint64_t> cluster_blocked_wait(num_clusters, 0);
    std::vector<uint64_t> cluster_blocker_wait(num_clusters, 0);
    std::vector<size_t> assignment(num_clusters, 0);
    std::vector<uint8_t> cluster_low_frequency(num_clusters, 0);

    for (size_t c = 0; c < num_clusters; ++c) {
        double fallback = 0.0;
        if (c < clusters_.clusters.size()) {
            for (size_t u : clusters_.clusters[c]) {
                fallback += u < unit_costs_.size() ? unit_costs_[u] : 1.0;
                if (u < unit_ptrs_.size() && (unit_ptrs_[u]->tickInterval() > 1 ||
                                              unit_ptrs_[u]->usesActivityScheduling())) {
                    cluster_low_frequency[c] = 1;
                }
            }
        }
        if (fallback <= 0.0) fallback = 1.0;

        const uint64_t samples = cluster_sample_count_[c].load(std::memory_order_relaxed);
        const uint64_t total = cluster_sample_time_ns_[c].load(std::memory_order_relaxed);
        cluster_samples[c] = samples;
        cluster_cost[c] =
            samples > 0 && (!cluster_low_frequency[c] || samples >= kDynamicClusterMinSamples)
                ? static_cast<double>(total) / static_cast<double>(samples)
                : fallback;

        size_t owner = cluster_runtime_owner_[c].load(std::memory_order_acquire);
        if (owner >= num_threads) owner = 0;
        assignment[c] = owner;
        if (owner < num_threads) {
            thread_cost[owner] += cluster_cost[c];
            if (cluster_cost[c] > 0.0) {
                ++thread_active_clusters[owner];
            }
        }
    }
    for (size_t t = 0; t < num_threads && t < dynamic_runtime_thread_count_; ++t) {
        thread_floor_wait[t] = dynamic_thread_floor_wait_ns_[t].load(std::memory_order_relaxed);
        thread_dep_wait[t] = dynamic_thread_dep_wait_ns_[t].load(std::memory_order_relaxed);
        thread_no_ready_wait[t] =
            dynamic_thread_no_ready_wait_ns_[t].load(std::memory_order_relaxed);
    }
    for (size_t c = 0; c < num_clusters; ++c) {
        cluster_blocked_wait[c] =
            dynamic_cluster_blocked_wait_ns_[c].load(std::memory_order_relaxed);
        cluster_blocker_wait[c] =
            dynamic_cluster_blocker_wait_ns_[c].load(std::memory_order_relaxed);
    }

    double total_cost = 0.0;
    for (double cost : thread_cost) total_cost += cost;
    if (total_cost <= 0.0) {
        clearDynamicMigrationRequest_();
        return false;
    }

    const double avg = total_cost / static_cast<double>(num_threads);
    if (avg <= 0.0) {
        clearDynamicMigrationRequest_();
        return false;
    }

    std::vector<size_t> source_threads;
    uint64_t total_dep_wait = 0;
    for (uint64_t wait : thread_dep_wait) total_dep_wait += wait;
    const double avg_dep_wait =
        num_threads > 0 ? static_cast<double>(total_dep_wait) / static_cast<double>(num_threads)
                        : 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        const bool active_overloaded =
            (thread_cost[t] / avg) > config_.rebalance_imbalance_threshold;
        const bool dep_overloaded = avg_dep_wait > 0.0 &&
                                    static_cast<double>(thread_dep_wait[t]) > avg_dep_wait * 1.25 &&
                                    thread_cost[t] > avg * 0.90;
        if (active_overloaded || dep_overloaded) {
            source_threads.push_back(t);
        }
    }
    if (source_threads.empty()) {
        clearDynamicMigrationRequest_();
        return false;
    }

    PartitionInput input;
    input.num_units = num_clusters;
    input.num_threads = num_threads;
    input.unit_cost_ns = cluster_cost;
    input.sync_cost_ns =
        std::max(platform_metrics_.atomic_roundtrip_ns, config_.initial_partition_sync_cost_ns);
    input.critical_path_weight = config_.sa_critical_path_weight;
    input.adjacency.resize(num_clusters);

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
        size_t sc = unit_to_cluster_[si->second];
        size_t dc = unit_to_cluster_[di->second];
        if (sc == dc) continue;
        auto& fwd = cluster_pair_info[{sc, dc}];
        fwd.first++;
        fwd.second = (fwd.second == 0) ? conn->delay() : std::min(fwd.second, conn->delay());
    }
    std::vector<PairKey> keys;
    keys.reserve(cluster_pair_info.size());
    for (const auto& [key, info] : cluster_pair_info) keys.push_back(key);
    std::sort(keys.begin(), keys.end(), [](const PairKey& a, const PairKey& b) {
        return a.u != b.u ? a.u < b.u : a.v < b.v;
    });
    for (const PairKey& key : keys) {
        const auto& info = cluster_pair_info[key];
        input.adjacency[key.u].push_back({key.v, info.first, info.second});
    }

    epoch_free_cost::RuntimeWaits waits{&thread_floor_wait, &thread_dep_wait, &thread_no_ready_wait,
                                        &cluster_blocked_wait, &cluster_blocker_wait};
    const uint64_t history_cooldown = std::max(config_.rebalance_cooldown_cycles, interval * 2);
    const uint64_t pingpong_cooldown = std::max(history_cooldown, interval * 4);
    auto last_migration_cycle = [&](size_t c) -> uint64_t {
        return c < dynamic_cluster_last_migration_cycle_.size()
                   ? dynamic_cluster_last_migration_cycle_[c]
                   : kNoMigrationCycle;
    };
    auto in_history_cooldown = [&](uint64_t last_cycle) {
        return last_cycle != kNoMigrationCycle &&
               cycle < saturatingCycleAdd(last_cycle, history_cooldown);
    };
    auto in_pingpong_cooldown = [&](size_t c, size_t candidate_source, size_t candidate_target,
                                    uint64_t last_cycle) {
        return last_cycle != kNoMigrationCycle && c < dynamic_cluster_last_source_thread_.size() &&
               c < dynamic_cluster_last_target_thread_.size() &&
               dynamic_cluster_last_source_thread_[c] == candidate_target &&
               dynamic_cluster_last_target_thread_[c] == candidate_source &&
               cycle < saturatingCycleAdd(last_cycle, pingpong_cooldown);
    };
    size_t source = SIZE_MAX;
    size_t cluster = SIZE_MAX;
    size_t target = SIZE_MAX;
    double best_cluster_cost = 0.0;
    epoch_free_cost::MoveBreakdown best_breakdown;
    for (size_t candidate_source : source_threads) {
        for (size_t c = 0; c < num_clusters; ++c) {
            if (cluster_runtime_owner_[c].load(std::memory_order_acquire) != candidate_source) {
                continue;
            }
            if (cluster_migration_pending_[c].load(std::memory_order_acquire) != 0) continue;
            if (cluster_samples[c] == 0) continue;
            if (cluster_low_frequency[c] && cluster_samples[c] < kDynamicClusterMinSamples) {
                continue;
            }
            if (cluster_cost[c] <= 0.0) continue;
            const uint64_t last_cycle = last_migration_cycle(c);
            if (in_history_cooldown(last_cycle)) continue;

            for (size_t candidate_target = 0; candidate_target < num_threads; ++candidate_target) {
                if (candidate_target == candidate_source) continue;
                if (in_pingpong_cooldown(c, candidate_source, candidate_target, last_cycle))
                    continue;

                const double churn =
                    last_cycle == kNoMigrationCycle ? 0.0 : std::max(0.001, cluster_cost[c] * 0.05);
                auto breakdown =
                    epoch_free_cost::scoreMove(input, assignment, c, candidate_target, waits,
                                               config_.rebalance_min_gain, churn);
                if (!breakdown.valid) continue;
                if (breakdown.score > best_breakdown.score ||
                    (breakdown.score == best_breakdown.score && c < cluster)) {
                    best_breakdown = breakdown;
                    best_cluster_cost = cluster_cost[c];
                    source = candidate_source;
                    cluster = c;
                    target = candidate_target;
                }
            }
        }
    }
    if (source == SIZE_MAX) {
        size_t fallback_source = source_threads.front();
        for (size_t t : source_threads) {
            if (thread_cost[t] > thread_cost[fallback_source]) fallback_source = t;
        }
        size_t fallback_target = SIZE_MAX;
        for (size_t t = 0; t < num_threads; ++t) {
            if (t == fallback_source) continue;
            if (fallback_target == SIZE_MAX || thread_cost[t] < thread_cost[fallback_target]) {
                fallback_target = t;
            }
        }
        if (fallback_target != SIZE_MAX) {
            for (size_t c = 0; c < num_clusters; ++c) {
                if (cluster_runtime_owner_[c].load(std::memory_order_acquire) != fallback_source) {
                    continue;
                }
                if (cluster_migration_pending_[c].load(std::memory_order_acquire) != 0) continue;
                if (cluster_samples[c] == 0) continue;
                if (cluster_low_frequency[c] && cluster_samples[c] < kDynamicClusterMinSamples) {
                    continue;
                }
                const uint64_t last_cycle = last_migration_cycle(c);
                if (in_history_cooldown(last_cycle)) continue;
                if (in_pingpong_cooldown(c, fallback_source, fallback_target, last_cycle)) continue;
                const double target_after = thread_cost[fallback_target] + cluster_cost[c];
                if (target_after > thread_cost[fallback_source] * 1.02) continue;
                if (cluster == SIZE_MAX || cluster_cost[c] > best_cluster_cost) {
                    source = fallback_source;
                    target = fallback_target;
                    cluster = c;
                    best_cluster_cost = cluster_cost[c];
                }
            }
        }
        if (cluster != SIZE_MAX) {
            auto old_summary = epoch_free_cost::summarize(input, assignment, num_threads);
            auto candidate_assignment = assignment;
            candidate_assignment[cluster] = target;
            auto new_summary = epoch_free_cost::summarize(input, candidate_assignment, num_threads);
            best_breakdown.objective_gain = old_summary.objective - new_summary.objective;
            best_breakdown.active_gain = old_summary.max_active - new_summary.max_active;
            best_breakdown.topology_delta =
                (old_summary.cross_pressure + old_summary.max_incoming_pressure) -
                (new_summary.cross_pressure + new_summary.max_incoming_pressure);
            best_breakdown.score = std::max(0.0, best_breakdown.active_gain);
            best_breakdown.old_max_active = old_summary.max_active;
            best_breakdown.new_max_active = new_summary.max_active;
            best_breakdown.target_active_after = thread_cost[target] + best_cluster_cost;
            best_breakdown.active_budget = std::max(avg * 1.15, old_summary.max_active * 0.72);
            best_breakdown.target_heavy_before = old_summary.heavy_count[target];
            best_breakdown.target_heavy_after = new_summary.heavy_count[target];
            const double min_active_gain = std::max(
                best_cluster_cost * 0.05, old_summary.max_active * config_.rebalance_min_gain);
            const double min_score =
                std::max(0.01, config_.rebalance_min_gain * old_summary.objective * 0.25);
            best_breakdown.valid =
                best_breakdown.active_gain > min_active_gain && best_breakdown.score >= min_score;
        }
    }
    if (source == SIZE_MAX || cluster == SIZE_MAX || target == SIZE_MAX || !best_breakdown.valid ||
        best_cluster_cost <= 0.0) {
        clearDynamicMigrationRequest_();
        return false;
    }

    const uint64_t progress =
        thread_progress_array_[cluster].completed_cycle.load(std::memory_order_acquire);
    const uint64_t fence = saturatingCycleAdd(progress, 1);

    std::string names;
    if (cluster < clusters_.clusters.size()) {
        names = buildUnitNameList_(clusters_.clusters[cluster]);
    }
    last_rebalance_detail_ =
        "rebalance=" + std::to_string(rebalance_count_ + 1) + " C" + std::to_string(cluster) + "(" +
        names + ") T" + std::to_string(source) + "->T" + std::to_string(target) +
        " fence=" + std::to_string(fence) + " src_active=" + std::to_string(thread_cost[source]) +
        " dst_active=" + std::to_string(thread_cost[target]) +
        " score=" + std::to_string(best_breakdown.score) +
        " obj_gain=" + std::to_string(best_breakdown.objective_gain) +
        " active_gain=" + std::to_string(best_breakdown.active_gain) +
        " topology_delta=" + std::to_string(best_breakdown.topology_delta) +
        " dep_bonus=" + std::to_string(best_breakdown.measured_dep_bonus) +
        " floor_bonus=" + std::to_string(best_breakdown.floor_slack_bonus) +
        " dep_penalty=" + std::to_string(best_breakdown.target_dep_penalty) +
        " stack_penalty=" + std::to_string(best_breakdown.active_stack_penalty) +
        " churn_penalty=" + std::to_string(best_breakdown.churn_penalty) +
        " new_max_active=" + std::to_string(best_breakdown.new_max_active) +
        " target_active_after=" + std::to_string(best_breakdown.target_active_after) +
        " active_budget=" + std::to_string(best_breakdown.active_budget) +
        " target_heavy_before=" + std::to_string(best_breakdown.target_heavy_before) +
        " target_heavy_after=" + std::to_string(best_breakdown.target_heavy_after) +
        " dst_clusters=" + std::to_string(thread_active_clusters[target]) +
        " dst_floor_wait_ns=" + std::to_string(thread_floor_wait[target]) +
        " dst_dep_wait_ns=" + std::to_string(thread_dep_wait[target]) +
        " dst_no_ready_wait_ns=" + std::to_string(thread_no_ready_wait[target]) +
        " cluster_blocked_wait_ns=" + std::to_string(cluster_blocked_wait[cluster]) +
        " cluster_blocker_wait_ns=" + std::to_string(cluster_blocker_wait[cluster]);

    if (observe_ctx_) {
        observe::log_info<"Dynamic rebalance requested: cluster {} [{}] T{} -> T{} fence={}">(
            observe_ctx_, cluster, names.c_str(), source, target, fence);
    }
    recordDynamicSchedulerMarker_("Chronon epoch-free rebalance requested", cycle,
                                  last_rebalance_detail_);

    migration_request_.cluster.store(cluster, std::memory_order_relaxed);
    migration_request_.source_thread.store(source, std::memory_order_relaxed);
    migration_request_.target_thread.store(target, std::memory_order_relaxed);
    migration_request_.fence_cycle.store(fence, std::memory_order_release);
    cluster_migration_pending_[cluster].store(1, std::memory_order_release);
    migration_request_.state.store(static_cast<uint8_t>(MigrationRequestState::Quiescing),
                                   std::memory_order_release);

    return true;
}

void TickSimulation::serviceEpochFreeMigration_(size_t worker_thread) {
    if (!cluster_runtime_owner_ || dynamic_runtime_cluster_count_ == 0) return;

    const uint8_t quiescing = static_cast<uint8_t>(MigrationRequestState::Quiescing);
    if (migration_request_.state.load(std::memory_order_acquire) != quiescing) return;

    const size_t cluster = migration_request_.cluster.load(std::memory_order_relaxed);
    const size_t source = migration_request_.source_thread.load(std::memory_order_relaxed);
    const size_t target = migration_request_.target_thread.load(std::memory_order_relaxed);
    const uint64_t fence = migration_request_.fence_cycle.load(std::memory_order_acquire);
    if (cluster >= dynamic_runtime_cluster_count_ || source >= config_.num_threads ||
        target >= config_.num_threads) {
        if (cluster < dynamic_runtime_cluster_count_) {
            cluster_migration_pending_[cluster].store(0, std::memory_order_release);
        }
        clearDynamicMigrationRequest_();
        return;
    }

    // Only the losing owner commits after finishing a complete sweep. A
    // request published during a stable no-CAS sweep therefore permits at
    // most that sweep to finish; the target cannot observe ownership until no
    // source execution remains in flight.
    if (worker_thread != source) return;

    const uint64_t progress =
        thread_progress_array_[cluster].completed_cycle.load(std::memory_order_acquire);
    if (progress < fence) return;

    uint8_t expected = quiescing;
    if (!migration_request_.state.compare_exchange_strong(
            expected, static_cast<uint8_t>(MigrationRequestState::ReadyToCommit),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return;
    }

    cluster_runtime_owner_[cluster].store(target, std::memory_order_release);
    cluster_migration_pending_[cluster].store(0, std::memory_order_release);
    if (cluster < dynamic_cluster_last_migration_cycle_.size()) {
        dynamic_cluster_last_migration_cycle_[cluster] = fence;
        dynamic_cluster_last_source_thread_[cluster] = source;
        dynamic_cluster_last_target_thread_[cluster] = target;
    }
    cluster_assignment_generation_.fetch_add(1, std::memory_order_acq_rel);

    const uint64_t delay =
        std::max(config_.rebalance_check_interval_cycles, config_.rebalance_cooldown_cycles);
    const uint64_t next_check = saturatingCycleAdd(fence, delay);
    uint64_t old_next = next_dynamic_rebalance_check_cycle_.load(std::memory_order_relaxed);
    while (next_check > old_next &&
           !next_dynamic_rebalance_check_cycle_.compare_exchange_weak(
               old_next, next_check, std::memory_order_release, std::memory_order_relaxed)) {
    }

    for (size_t c = 0; c < dynamic_runtime_cluster_count_; ++c) {
        const uint64_t samples = cluster_sample_count_[c].load(std::memory_order_relaxed);
        const uint64_t total = cluster_sample_time_ns_[c].load(std::memory_order_relaxed);
        bool low_frequency = false;
        if (c < clusters_.clusters.size()) {
            for (size_t unit_idx : clusters_.clusters[c]) {
                if (unit_idx < unit_ptrs_.size() &&
                    (unit_ptrs_[unit_idx]->tickInterval() > 1 ||
                     unit_ptrs_[unit_idx]->usesActivityScheduling())) {
                    low_frequency = true;
                    break;
                }
            }
        }
        if (samples > 0 && (!low_frequency || samples >= kDynamicClusterMinSamples) &&
            c < clusters_.clusters.size() && !clusters_.clusters[c].empty()) {
            const double avg = static_cast<double>(total) / static_cast<double>(samples);
            const double per_unit =
                std::max(0.001, avg / static_cast<double>(clusters_.clusters[c].size()));
            if (unit_costs_.size() < unit_ptrs_.size()) unit_costs_.resize(unit_ptrs_.size(), 1.0);
            for (size_t unit_idx : clusters_.clusters[c]) {
                if (unit_idx < unit_costs_.size()) unit_costs_[unit_idx] = per_unit;
            }
        }
        cluster_sample_time_ns_[c].store(0, std::memory_order_relaxed);
        cluster_sample_count_[c].store(0, std::memory_order_relaxed);
        cluster_active_sample_count_[c].store(0, std::memory_order_relaxed);
        dynamic_cluster_blocked_wait_ns_[c].store(0, std::memory_order_relaxed);
        dynamic_cluster_blocker_wait_ns_[c].store(0, std::memory_order_relaxed);
    }
    for (size_t t = 0; t < dynamic_runtime_thread_count_; ++t) {
        dynamic_thread_floor_wait_ns_[t].store(0, std::memory_order_relaxed);
        dynamic_thread_dep_wait_ns_[t].store(0, std::memory_order_relaxed);
        dynamic_thread_no_ready_wait_ns_[t].store(0, std::memory_order_relaxed);
    }

    ++rebalance_count_;
    cycles_since_last_actual_rebalance_ = 0;
    migration_request_.state.store(static_cast<uint8_t>(MigrationRequestState::Committed),
                                   std::memory_order_release);

    if (observe_ctx_) {
        observe::log_info<"Dynamic rebalance committed: {}">(observe_ctx_,
                                                             last_rebalance_detail_.c_str());
    }
    recordDynamicSchedulerMarker_("Chronon epoch-free rebalance committed", fence,
                                  last_rebalance_detail_);
    clearDynamicMigrationRequest_();
}

template <bool PushPeriodicCounters>
void TickSimulation::executeThreadRunDynamicImpl_(size_t thread_idx, uint64_t end_cycle,
                                                  uint64_t run_start, uint64_t period,
                                                  stdexec::inplace_stop_token token) {
    const bool trace_units = timeline_trace_.traceUnits();
    const bool trace_waits = timeline_trace_.traceWaits();
    const uint64_t max_lookahead = config_.max_lookahead_cycles;
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    std::vector<size_t> owned_clusters;
    std::vector<size_t> refreshed_clusters;
    // Cluster ids and progress slots remain stable across ownership migration,
    // so this worker-private cache can span the entire EpochFree invocation.
    WorkerPredecessorCycleCache predecessor_cache(thread_progress_count_);
    uint64_t* const predecessor_cycles = predecessor_cache.data();
    std::vector<uint64_t> priority_blocker_ns(num_clusters, 0);
    std::vector<double> priority_cost_ns(num_clusters, 0.0);
    std::vector<uint64_t> ready_through_cycle(num_clusters, 0);
    uint64_t seen_generation = 0;
    uint64_t priority_refresh = 0;
    uint64_t wait_sample_sequence = 0;
    observe::ThreadContext* counter_producer = nullptr;
    if constexpr (PushPeriodicCounters) {
        counter_producer = observe::ObservationManager::instance().periodicCounterProducer();
    }

    auto refresh_owned_clusters = [&]() {
        if (!cluster_runtime_owner_) return;
        refreshed_clusters.reserve(num_clusters);
        for (;;) {
            const uint64_t generation_before =
                cluster_assignment_generation_.load(std::memory_order_acquire);
            refreshed_clusters.clear();
            for (size_t cluster = 0; cluster < num_clusters; ++cluster) {
                if (cluster_runtime_owner_[cluster].load(std::memory_order_acquire) == thread_idx) {
                    refreshed_clusters.push_back(cluster);
                }
            }
            const uint64_t generation_after =
                cluster_assignment_generation_.load(std::memory_order_acquire);
            if (generation_before == generation_after) {
                owned_clusters.swap(refreshed_clusters);
                seen_generation = generation_after;
                return;
            }
        }
    };

    auto all_clusters_done = [&]() {
        for (size_t cluster = 0; cluster < num_clusters; ++cluster) {
            if (thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed) <
                end_cycle) {
                return false;
            }
        }
        return true;
    };

    auto prioritize_owned_clusters = [&]() {
        if (owned_clusters.size() < 2) return;
        for (size_t cluster : owned_clusters) {
            priority_blocker_ns[cluster] =
                dynamic_cluster_blocker_wait_ns_
                    ? dynamic_cluster_blocker_wait_ns_[cluster].load(std::memory_order_relaxed)
                    : 0;
            const uint64_t samples = cluster_sample_count_[cluster].load(std::memory_order_relaxed);
            priority_cost_ns[cluster] =
                samples > 0 ? static_cast<double>(cluster_sample_time_ns_[cluster].load(
                                  std::memory_order_relaxed)) /
                                  static_cast<double>(samples)
                            : 0.0;
        }
        std::sort(owned_clusters.begin(), owned_clusters.end(), [&](size_t a, size_t b) {
            const uint64_t a_blocker = priority_blocker_ns[a];
            const uint64_t b_blocker = priority_blocker_ns[b];
            if (a_blocker != b_blocker) return a_blocker > b_blocker;

            const double a_cost = priority_cost_ns[a];
            const double b_cost = priority_cost_ns[b];
            if (a_cost != b_cost) return a_cost > b_cost;
            return a < b;
        });
    };

    auto migration_blocks_cluster = [&](size_t cluster, uint64_t cycle) {
        if (cluster_migration_pending_[cluster].load(std::memory_order_acquire) == 0) {
            return false;
        }
        const uint8_t state = migration_request_.state.load(std::memory_order_acquire);
        const bool committing = state == static_cast<uint8_t>(MigrationRequestState::Quiescing) ||
                                state == static_cast<uint8_t>(MigrationRequestState::ReadyToCommit);
        if (!committing || migration_request_.cluster.load(std::memory_order_relaxed) != cluster) {
            return false;
        }
        return cycle >= migration_request_.fence_cycle.load(std::memory_order_acquire);
    };

    auto prepare_stable_sweep = [&](bool& ownership_refreshed) {
        constexpr uint8_t none = static_cast<uint8_t>(MigrationRequestState::None);
        const uint8_t state = migration_request_.state.load(std::memory_order_acquire);
        const uint64_t generation = cluster_assignment_generation_.load(std::memory_order_acquire);
        if (generation != seen_generation) {
            refresh_owned_clusters();
            ownership_refreshed = true;
        }
        return state == none && generation == seen_generation;
    };

    auto cluster_can_advance_cached = [&](size_t cluster, uint64_t cycle,
                                          BlockedClusterInfo& blocker) {
        if (cycle < ready_through_cycle[cluster]) return true;
        if (!clusterCanAdvance_(cluster, cycle, blocker, predecessor_cycles)) return false;

        uint64_t ready_through = std::numeric_limits<uint64_t>::max();
        if (cluster < thread_resolved_deps_.size()) {
            for (const auto& dep : thread_resolved_deps_[cluster]) {
                const uint64_t observed = predecessor_cycles[dep.pred_id];
                const uint64_t bound =
                    observed > std::numeric_limits<uint64_t>::max() - dep.min_delay
                        ? std::numeric_limits<uint64_t>::max()
                        : observed + static_cast<uint64_t>(dep.min_delay);
                ready_through = std::min(ready_through, bound);
            }
        }
        ready_through_cycle[cluster] = ready_through;
        return true;
    };

    while (!token.stop_requested()) {
        bool ownership_refreshed = false;
        const bool stable_sweep = prepare_stable_sweep(ownership_refreshed);
        if (ownership_refreshed) {
            prioritize_owned_clusters();
            priority_refresh = 0;
        } else if ((priority_refresh++ & 0xFFu) == 0) {
            prioritize_owned_clusters();
        }

        bool made_progress = false;
        bool local_done = true;
        uint64_t newest_progress = 0;
        uint64_t blocked_floor_needed = 0;
        BlockedClusterInfo blocker{};

        for (size_t cluster : owned_clusters) {
            auto& progress = thread_progress_array_[cluster].completed_cycle;
            uint64_t cycle = progress.load(std::memory_order_relaxed);
            if (!stable_sweep &&
                (!cluster_runtime_owner_ ||
                 cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx))
                continue;
            if (cycle >= end_cycle) continue;
            local_done = false;

            if (!stable_sweep && migration_blocks_cluster(cluster, cycle)) continue;

            BlockedClusterInfo candidate{};
            if (!cluster_can_advance_cached(cluster, cycle, candidate)) {
                blocked_floor_needed =
                    std::max(blocked_floor_needed,
                             cycle + 1 > max_lookahead ? cycle + 1 - max_lookahead : 0);
                if (candidate.deficit > blocker.deficit) {
                    blocker = candidate;
                }
                continue;
            }

            uint64_t idle_target = cycle;
            if (any_activity_scheduling_.enabled.load(std::memory_order_acquire)) [[unlikely]] {
                idle_target = computeIdleClusterTargetIfEnabled_(cluster, cycle, end_cycle,
                                                                 predecessor_cycles);
            }
            if (idle_target > cycle) {
                SchedulerTimelineTrace::TimePoint idle_begin{};
                if (trace_units) {
                    idle_begin = SchedulerTimelineTrace::Clock::now();
                }
                advanceClusterIdle_(cluster, idle_target - cycle);
                uint64_t reached_cycle = idle_target;
                const uint64_t refreshed_target =
                    computeIdleClusterTarget_(cluster, cycle, idle_target, predecessor_cycles);
                if (refreshed_target < idle_target) {
                    reached_cycle = refreshed_target;
                    for (auto* unit : cluster_unit_ptrs_[cluster]) {
                        unit->setLocalCycle(reached_cycle);
                    }
                }
                if (trace_units) {
                    auto idle_end = SchedulerTimelineTrace::Clock::now();
                    recordClusterIdle_(thread_idx, cluster, cycle, reached_cycle - cycle,
                                       idle_begin, idle_end);
                }
                progress.store(reached_cycle, std::memory_order_release);
            } else {
                const uint64_t burst_end = cycle + 1;
                do {
                    const bool sample_tick = (cycle & 1023u) == 0;
                    SchedulerTimelineTrace::TimePoint begin{};
                    if (sample_tick) begin = SchedulerTimelineTrace::Clock::now();
                    executeClusterOneCycle_(thread_idx, cluster, cycle, trace_units);
                    if (sample_tick) {
                        auto end = SchedulerTimelineTrace::Clock::now();
                        uint64_t elapsed_ns = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                                .count());
                        recordClusterTickSample_(cluster, elapsed_ns, true);
                    }
                    ++cycle;
                    progress.store(cycle, std::memory_order_release);
                } while (cycle < burst_end && !token.stop_requested());
            }

            made_progress = true;
            newest_progress = std::max(newest_progress, progress.load(std::memory_order_relaxed));
        }

        if constexpr (PushPeriodicCounters) {
            auto& obs_mgr = observe::ObservationManager::instance();
            for (size_t cluster : owned_clusters) {
                if (!counter_producer || !cluster_runtime_owner_ ||
                    cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx) {
                    continue;
                }
                const uint64_t observed =
                    thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed);
                uint64_t nominal_cycle =
                    obs_mgr.nextPeriodicCounterCycle(cluster, run_start, period);
                while (OBSERVE_UNLIKELY(nominal_cycle <= observed && nominal_cycle <= end_cycle)) {
                    size_t claim = SIZE_MAX;
                    if (!cluster_execution_owner_[cluster].compare_exchange_strong(
                            claim, thread_idx, std::memory_order_acq_rel)) {
                        break;
                    }
                    if (cluster_runtime_owner_[cluster].load(std::memory_order_acquire) !=
                        thread_idx) {
                        cluster_execution_owner_[cluster].store(SIZE_MAX,
                                                                std::memory_order_release);
                        break;
                    }

                    const size_t snapshot_cluster = cluster;
                    (void)obs_mgr.pushPeriodicCounterSnapshots(
                        nominal_cycle, std::span<const size_t>(&snapshot_cluster, 1),
                        *counter_producer);
                    cluster_execution_owner_[cluster].store(SIZE_MAX, std::memory_order_release);

                    const uint64_t next_cycle =
                        obs_mgr.nextPeriodicCounterCycle(cluster, run_start, period);
                    if (next_cycle <= nominal_cycle) break;
                    nominal_cycle = next_cycle;
                }
            }
        }

        const bool globally_done = local_done && all_clusters_done();
        if (!globally_done && thread_idx == 0) {
            maybeRequestEpochFreeMigration_(newest_progress);
        }
        serviceEpochFreeMigration_(thread_idx);
        if (globally_done) return;
        if (made_progress) {
            if (blocker.cluster != SIZE_MAX &&
                blocked_floor_needed > lookahead_floor_.load(std::memory_order_relaxed)) {
                refreshLookaheadFloor_();
            }
            continue;
        }

        if (local_done) {
            if (cluster_assignment_generation_.load(std::memory_order_acquire) != seen_generation) {
                continue;
            }
        }

        constexpr uint64_t kWaitSampleMask = 0x0F;
        const bool sample_wait = trace_waits || ((wait_sample_sequence++ & kWaitSampleMask) == 0);
        SchedulerTimelineTrace::TimePoint wait_begin{};
        if (sample_wait) wait_begin = SchedulerTimelineTrace::Clock::now();

        uint64_t spin = 0;
        auto pause_or_yield_hidden_wait = [&](uint64_t spin_iteration) {
            if ((spin_iteration & kFloorRefreshSpinMask) == kFloorRefreshSpinMask &&
                blocker.cluster < num_clusters) {
                const uint64_t dependent_cycle =
                    thread_progress_array_[blocker.cluster].completed_cycle.load(
                        std::memory_order_relaxed);
                if (dependent_cycle > lookahead_floor_.load(std::memory_order_relaxed)) {
                    std::this_thread::yield();
                    return;
                }
            }
            cpuPause();
        };
        while (!token.stop_requested()) {
            const uint64_t spin_iteration = spin++;
            if ((spin_iteration & kFloorRefreshSpinMask) == 0) {
                if (blocked_floor_needed > lookahead_floor_.load(std::memory_order_relaxed)) {
                    refreshLookaheadFloor_();
                }
                if (thread_idx == 0) {
                    maybeRequestEpochFreeMigration_(0);
                }
                serviceEpochFreeMigration_(thread_idx);
            }
            if (cluster_assignment_generation_.load(std::memory_order_acquire) != seen_generation) {
                break;
            }

            constexpr uint64_t kReadyRescanMask = 0x0F;
            bool should_rescan = (spin_iteration & kReadyRescanMask) == 0;
            if (!should_rescan && blocker.cluster != SIZE_MAX) {
                const uint64_t observed =
                    blocker.pred_cluster == SIZE_MAX
                        ? lookahead_floor_.load(std::memory_order_relaxed)
                        : thread_progress_array_[blocker.pred_cluster].completed_cycle.load(
                              std::memory_order_acquire);
                should_rescan = observed >= blocker.needed;
            }
            if (!should_rescan) {
                pause_or_yield_hidden_wait(spin_iteration);
                continue;
            }

            bool any_ready = false;
            bool all_done = local_done ? all_clusters_done() : true;
            for (size_t cluster : owned_clusters) {
                uint64_t cycle =
                    thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed);
                if (cycle < end_cycle) {
                    all_done = false;
                }
                if (cycle >= end_cycle) {
                    continue;
                }
                if (!stable_sweep &&
                    (!cluster_runtime_owner_ || cluster_runtime_owner_[cluster].load(
                                                    std::memory_order_acquire) != thread_idx)) {
                    continue;
                }
                BlockedClusterInfo ignored{};
                if (cluster_can_advance_cached(cluster, cycle, ignored)) {
                    if (!stable_sweep && migration_blocks_cluster(cluster, cycle)) {
                        continue;
                    }
                    any_ready = true;
                    break;
                }
            }
            if (all_done || any_ready) break;
            pause_or_yield_hidden_wait(spin_iteration);
        }

        if (!token.stop_requested() && sample_wait) {
            auto wait_end = SchedulerTimelineTrace::Clock::now();
            const uint64_t raw_wait_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(wait_end - wait_begin)
                    .count());
            const uint64_t wait_ns =
                trace_waits ||
                        raw_wait_ns > std::numeric_limits<uint64_t>::max() / (kWaitSampleMask + 1)
                    ? raw_wait_ns
                    : raw_wait_ns * (kWaitSampleMask + 1);
            recordDynamicWaitSample_(thread_idx, blocker, wait_ns);
            if (!trace_waits) continue;

            const char* stall_name = blocker.cluster == SIZE_MAX        ? "stall: no-ready-cluster"
                                     : blocker.pred_cluster == SIZE_MAX ? "stall: lookahead-floor"
                                                                        : "stall: cluster-dep";
            const auto stall_style = schedulerStallStyle(stall_name);
            timeline_trace_.recordDuration(
                thread_idx, stall_style.category, stall_style.name,
                blocker.cluster == SIZE_MAX
                    ? current_cycle_
                    : thread_progress_array_[blocker.cluster].completed_cycle.load(
                          std::memory_order_relaxed),
                wait_begin, wait_end, formatBlockerDetail_(blocker));
        }
    }
}

void TickSimulation::executeThreadRunDynamic_(size_t thread_idx, uint64_t end_cycle,
                                              stdexec::inplace_stop_token token) {
    executeThreadRunDynamicImpl_<false>(thread_idx, end_cycle, 0, 0, token);
}

void TickSimulation::executeThreadRunDynamicWithPeriodicCounters_(
    size_t thread_idx, uint64_t end_cycle, uint64_t run_start, uint64_t period,
    stdexec::inplace_stop_token token) {
    executeThreadRunDynamicImpl_<true>(thread_idx, end_cycle, run_start, period, token);
}

}  // namespace chronon::sender
