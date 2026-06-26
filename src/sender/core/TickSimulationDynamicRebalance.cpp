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
#include <unordered_map>
#include <vector>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"

namespace chronon::sender {

namespace {
constexpr uint64_t kFloorRefreshSpinMask = 0xFF;

uint64_t saturatingCycleAdd(uint64_t base, uint64_t delta) noexcept {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return delta > max - base ? max : base + delta;
}

bool clusterHasMPSCConnections(const std::vector<TickableUnit*>& units) noexcept {
    for (const auto* unit : units) {
        for (const auto* port : unit->ports()) {
            if (port->hasMPSCConnections()) {
                return true;
            }
        }
    }
    return false;
}
}  // namespace

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

    if (dynamic_runtime_cluster_count_ != num_clusters || !cluster_runtime_owner_) {
        cluster_runtime_owner_ = std::make_unique<std::atomic<size_t>[]>(num_clusters);
        cluster_execution_owner_ = std::make_unique<std::atomic<size_t>[]>(num_clusters);
        cluster_migration_pending_ = std::make_unique<std::atomic<uint8_t>[]>(num_clusters);
        cluster_sample_time_ns_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        cluster_sample_count_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        cluster_active_sample_count_ = std::make_unique<std::atomic<uint64_t>[]>(num_clusters);
        dynamic_runtime_cluster_count_ = num_clusters;
    }

    for (size_t c = 0; c < num_clusters; ++c) {
        const size_t owner = c < cluster_to_thread_.size() ? cluster_to_thread_[c] : 0;
        cluster_runtime_owner_[c].store(owner, std::memory_order_relaxed);
        cluster_execution_owner_[c].store(SIZE_MAX, std::memory_order_relaxed);
        cluster_migration_pending_[c].store(0, std::memory_order_relaxed);
        cluster_sample_time_ns_[c].store(0, std::memory_order_relaxed);
        cluster_sample_count_[c].store(0, std::memory_order_relaxed);
        cluster_active_sample_count_[c].store(0, std::memory_order_relaxed);
    }
    cluster_assignment_generation_.store(1, std::memory_order_release);
    clearDynamicMigrationRequest_();
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

void TickSimulation::recordClusterTickSample_(size_t cluster, uint64_t ticks, bool active) {
    if (!cluster_sample_time_ns_ || cluster >= dynamic_runtime_cluster_count_) return;
    cluster_sample_time_ns_[cluster].fetch_add(ticks, std::memory_order_relaxed);
    cluster_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
    if (active) {
        cluster_active_sample_count_[cluster].fetch_add(1, std::memory_order_relaxed);
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
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                       marker.name, marker.cycle, marker.time,
                                       marker.time + std::chrono::nanoseconds(1), marker.detail);
    }

    const uint64_t drops = dynamic_scheduler_marker_drops_.load(std::memory_order_relaxed);
    if (drops > 0) {
        const auto now = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                       "Chronon epoch-free rebalance markers dropped",
                                       current_cycle_, now, now + std::chrono::nanoseconds(1),
                                       "drops=" + std::to_string(drops));
    }

    resetDynamicSchedulerMarkers_();
}

bool TickSimulation::maybeRequestEpochFreeMigration_(uint64_t cycle) {
    if (!config_.enable_dynamic_rebalance || !cluster_runtime_owner_ ||
        dynamic_runtime_cluster_count_ == 0 || config_.num_threads < 2) {
        return false;
    }

    const uint64_t interval = std::max<uint64_t>(1, config_.rebalance_check_interval_cycles);
    const uint64_t gate_cycle = lookahead_floor_.load(std::memory_order_acquire);
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

    const size_t num_threads = config_.num_threads;
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    std::vector<double> cluster_cost(num_clusters, 0.0);
    std::vector<double> thread_cost(num_threads, 0.0);

    for (size_t c = 0; c < num_clusters; ++c) {
        double fallback = 0.0;
        if (c < clusters_.clusters.size()) {
            for (size_t u : clusters_.clusters[c]) {
                fallback += u < unit_costs_.size() ? unit_costs_[u] : 1.0;
            }
        }
        if (fallback <= 0.0) fallback = 1.0;

        const uint64_t samples = cluster_sample_count_[c].load(std::memory_order_relaxed);
        const uint64_t total = cluster_sample_time_ns_[c].load(std::memory_order_relaxed);
        cluster_cost[c] =
            samples > 0 ? static_cast<double>(total) / static_cast<double>(samples) : fallback;

        size_t owner = cluster_runtime_owner_[c].load(std::memory_order_acquire);
        if (owner < num_threads) {
            thread_cost[owner] += cluster_cost[c];
        }
    }

    double total_cost = 0.0;
    for (double cost : thread_cost) total_cost += cost;
    if (total_cost <= 0.0) {
        clearDynamicMigrationRequest_();
        return false;
    }

    size_t source = 0;
    size_t target = 0;
    for (size_t t = 1; t < num_threads; ++t) {
        if (thread_cost[t] > thread_cost[source]) source = t;
        if (thread_cost[t] < thread_cost[target]) target = t;
    }
    if (source == target) {
        clearDynamicMigrationRequest_();
        return false;
    }

    const double avg = total_cost / static_cast<double>(num_threads);
    if (avg <= 0.0 || (thread_cost[source] / avg) <= config_.rebalance_imbalance_threshold) {
        clearDynamicMigrationRequest_();
        return false;
    }

    size_t cluster = SIZE_MAX;
    double best_cluster_cost = 0.0;
    for (size_t c = 0; c < num_clusters; ++c) {
        if (cluster_runtime_owner_[c].load(std::memory_order_acquire) != source) continue;
        if (cluster_migration_pending_[c].load(std::memory_order_acquire) != 0) continue;
        if (cluster_cost[c] > best_cluster_cost) {
            best_cluster_cost = cluster_cost[c];
            cluster = c;
        }
    }
    if (cluster == SIZE_MAX || best_cluster_cost <= 0.0) {
        clearDynamicMigrationRequest_();
        return false;
    }

    std::vector<double> candidate_cost = thread_cost;
    candidate_cost[source] -= best_cluster_cost;
    candidate_cost[target] += best_cluster_cost;
    const double old_max = *std::max_element(thread_cost.begin(), thread_cost.end());
    const double new_max = *std::max_element(candidate_cost.begin(), candidate_cost.end());
    const double gain = old_max > 0.0 ? (old_max - new_max) / old_max : 0.0;
    if (config_.rebalance_min_gain > 0.0 && gain < config_.rebalance_min_gain) {
        clearDynamicMigrationRequest_();
        return false;
    }

    const uint64_t progress =
        thread_progress_array_[cluster].completed_cycle.load(std::memory_order_acquire);
    const uint64_t fence = saturatingCycleAdd(progress, 1);

    migration_request_.cluster.store(cluster, std::memory_order_relaxed);
    migration_request_.source_thread.store(source, std::memory_order_relaxed);
    migration_request_.target_thread.store(target, std::memory_order_relaxed);
    migration_request_.fence_cycle.store(fence, std::memory_order_release);
    cluster_migration_pending_[cluster].store(1, std::memory_order_release);
    migration_request_.state.store(static_cast<uint8_t>(MigrationRequestState::Quiescing),
                                   std::memory_order_release);

    std::string names;
    if (cluster < clusters_.clusters.size()) {
        names = buildUnitNameList_(clusters_.clusters[cluster]);
    }
    last_rebalance_detail_ = "rebalance=" + std::to_string(rebalance_count_ + 1) + " C" +
                             std::to_string(cluster) + "(" + names + ") T" +
                             std::to_string(source) + "->T" + std::to_string(target) +
                             " fence=" + std::to_string(fence);

    if (observe_ctx_) {
        observe::log_info<"Dynamic rebalance requested: cluster {} [{}] T{} -> T{} fence={}">(
            observe_ctx_, cluster, names.c_str(), source, target, fence);
    }
    recordDynamicSchedulerMarker_("Chronon epoch-free rebalance requested", cycle,
                                  last_rebalance_detail_);

    return true;
}

void TickSimulation::serviceEpochFreeMigration_() {
    if (!cluster_runtime_owner_ || dynamic_runtime_cluster_count_ == 0) return;

    const uint8_t quiescing = static_cast<uint8_t>(MigrationRequestState::Quiescing);
    if (migration_request_.state.load(std::memory_order_acquire) != quiescing) {
        return;
    }

    const size_t cluster = migration_request_.cluster.load(std::memory_order_relaxed);
    const size_t target = migration_request_.target_thread.load(std::memory_order_relaxed);
    const uint64_t fence = migration_request_.fence_cycle.load(std::memory_order_acquire);
    if (cluster >= dynamic_runtime_cluster_count_ || target >= config_.num_threads) {
        clearDynamicMigrationRequest_();
        return;
    }

    const uint64_t progress =
        thread_progress_array_[cluster].completed_cycle.load(std::memory_order_acquire);
    if (progress < fence) return;
    if (cluster_execution_owner_[cluster].load(std::memory_order_acquire) != SIZE_MAX) return;

    uint8_t expected = quiescing;
    if (!migration_request_.state.compare_exchange_strong(
            expected, static_cast<uint8_t>(MigrationRequestState::ReadyToCommit),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return;
    }

    cluster_runtime_owner_[cluster].store(target, std::memory_order_release);
    cluster_migration_pending_[cluster].store(0, std::memory_order_release);
    rebuildThreadUnitsFromClusterOwners_();
    cluster_assignment_generation_.fetch_add(1, std::memory_order_acq_rel);

    const uint64_t delay =
        std::max(config_.rebalance_check_interval_cycles, config_.rebalance_cooldown_cycles);
    const uint64_t base_cycle = lookahead_floor_.load(std::memory_order_acquire);
    const uint64_t next_check = saturatingCycleAdd(base_cycle, delay);
    uint64_t old_next = next_dynamic_rebalance_check_cycle_.load(std::memory_order_relaxed);
    while (next_check > old_next &&
           !next_dynamic_rebalance_check_cycle_.compare_exchange_weak(
               old_next, next_check, std::memory_order_release, std::memory_order_relaxed)) {
    }

    for (size_t c = 0; c < dynamic_runtime_cluster_count_; ++c) {
        const uint64_t samples = cluster_sample_count_[c].load(std::memory_order_relaxed);
        const uint64_t total = cluster_sample_time_ns_[c].load(std::memory_order_relaxed);
        if (samples > 0 && c < clusters_.clusters.size() && !clusters_.clusters[c].empty()) {
            const double avg = static_cast<double>(total) / static_cast<double>(samples);
            const double per_unit =
                std::max(0.001, avg / static_cast<double>(clusters_.clusters[c].size()));
            if (unit_costs_.size() < unit_ptrs_.size()) {
                unit_costs_.resize(unit_ptrs_.size(), 1.0);
            }
            for (size_t unit_idx : clusters_.clusters[c]) {
                if (unit_idx < unit_costs_.size()) {
                    unit_costs_[unit_idx] = per_unit;
                }
            }
        }
        cluster_sample_time_ns_[c].store(0, std::memory_order_relaxed);
        cluster_sample_count_[c].store(0, std::memory_order_relaxed);
        cluster_active_sample_count_[c].store(0, std::memory_order_relaxed);
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

uint64_t TickSimulation::executeRunEpochFreeDynamic_(uint64_t total_cycles) {
    const size_t nthreads = thread_units_.size();
    if (nthreads == 0 || total_cycles == 0) return 0;

    initDynamicMigrationRuntime_();

    auto token = stop_source_->get_token();
    auto sched = pool_.get_scheduler();

    const uint64_t run_start =
        thread_progress_array_[0].completed_cycle.load(std::memory_order_relaxed);
    const uint64_t run_target = saturatingCycleAdd(run_start, total_cycles);

    lookahead_floor_.store(run_start, std::memory_order_relaxed);
    next_dynamic_rebalance_check_cycle_.store(
        saturatingCycleAdd(run_start, config_.rebalance_check_interval_cycles),
        std::memory_order_relaxed);
    epoch_free_dynamic_runtime_active_.store(true, std::memory_order_release);

    SchedulerTimelineTrace::TimePoint run_begin{};
    if (timeline_trace_.traceEpochs()) {
        run_begin = SchedulerTimelineTrace::Clock::now();
    }
    resetDynamicSchedulerMarkers_();

    std::exception_ptr captured;
    std::atomic_flag captured_set = ATOMIC_FLAG_INIT;

    auto work =
        stdexec::bulk(stdexec::just(), stdexec::par, nthreads, [&, token](std::size_t thread_idx) {
            try {
                executeThreadEpochDynamic_(thread_idx, run_target, token);
            } catch (...) {
                if (!captured_set.test_and_set(std::memory_order_relaxed)) {
                    captured = std::current_exception();
                }
                stop_source_->request_stop();
            }
        });

    stdexec::sync_wait(stdexec::starts_on(sched, std::move(work)));

    epoch_free_dynamic_runtime_active_.store(false, std::memory_order_release);

    if (captured) std::rethrow_exception(captured);

    arbitrateAllMPSCPorts_();
    flushDynamicSchedulerMarkers_();

    if (timeline_trace_.traceEpochs()) {
        auto run_end = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler",
                                       "epoch-free dynamic lookahead run", run_start, run_begin,
                                       run_end, "cycles=" + std::to_string(total_cycles));
    }

    for (size_t i = 0; i < units_.size(); ++i) {
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_release);
    }

    rebuildThreadUnitsFromClusterOwners_();

    return completedCyclesForRun_(run_start, run_target);
}

void TickSimulation::executeThreadEpochDynamic_(size_t thread_idx, uint64_t end_cycle,
                                                stdexec::inplace_stop_token token) {
    const bool trace_units = timeline_trace_.traceUnits();
    const bool trace_waits = timeline_trace_.traceWaits();
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    std::vector<size_t> owned_clusters;
    uint64_t seen_generation = 0;

    auto refresh_owned_clusters = [&]() {
        owned_clusters.clear();
        if (!cluster_runtime_owner_) return;
        owned_clusters.reserve(num_clusters);
        for (size_t cluster = 0; cluster < num_clusters; ++cluster) {
            if (cluster_runtime_owner_[cluster].load(std::memory_order_acquire) == thread_idx) {
                owned_clusters.push_back(cluster);
            }
        }
        seen_generation = cluster_assignment_generation_.load(std::memory_order_acquire);
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

    while (!token.stop_requested()) {
        serviceEpochFreeMigration_();
        const uint64_t generation = cluster_assignment_generation_.load(std::memory_order_acquire);
        if (generation != seen_generation) {
            refresh_owned_clusters();
        }

        bool made_progress = false;
        bool local_done = true;
        BlockedClusterInfo blocker{};

        for (size_t cluster : owned_clusters) {
            auto& progress = thread_progress_array_[cluster].completed_cycle;
            uint64_t cycle = progress.load(std::memory_order_relaxed);
            if (!cluster_runtime_owner_ ||
                cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx) {
                continue;
            }
            if (cycle >= end_cycle) continue;
            local_done = false;

            cluster_execution_owner_[cluster].store(thread_idx, std::memory_order_release);

            auto release_cluster = [&]() noexcept {
                cluster_execution_owner_[cluster].store(SIZE_MAX, std::memory_order_release);
            };

            if (cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx) {
                release_cluster();
                continue;
            }

            const uint8_t state = migration_request_.state.load(std::memory_order_acquire);
            if (cluster_migration_pending_[cluster].load(std::memory_order_acquire) != 0 &&
                state == static_cast<uint8_t>(MigrationRequestState::Quiescing) &&
                migration_request_.cluster.load(std::memory_order_relaxed) == cluster) {
                const uint64_t fence =
                    migration_request_.fence_cycle.load(std::memory_order_acquire);
                if (cycle >= fence) {
                    release_cluster();
                    continue;
                }
            }

            BlockedClusterInfo candidate{};
            if (!clusterCanAdvance_(cluster, cycle, candidate)) {
                if (candidate.deficit > blocker.deficit) {
                    blocker = candidate;
                }
                release_cluster();
                continue;
            }

            uint64_t idle_target = computeIdleClusterTarget_(cluster, cycle, end_cycle);
            if (idle_target > cycle) {
                const bool cluster_has_mpsc =
                    clusterHasMPSCConnections(cluster_unit_ptrs_[cluster]);
                if (cluster_has_mpsc) {
                    idle_target = std::min(idle_target, cycle + 1);
                }
                SchedulerTimelineTrace::TimePoint idle_begin{};
                if (trace_units) {
                    idle_begin = SchedulerTimelineTrace::Clock::now();
                }
                advanceClusterIdle_(cluster, idle_target - cycle);
                uint64_t reached_cycle = idle_target;
                if (!cluster_has_mpsc) {
                    const uint64_t refreshed_target =
                        computeIdleClusterTarget_(cluster, cycle, idle_target);
                    if (refreshed_target < idle_target) {
                        reached_cycle = refreshed_target;
                        for (auto* unit : cluster_unit_ptrs_[cluster]) {
                            unit->setLocalCycle(reached_cycle);
                        }
                    }
                }
                if (trace_units) {
                    auto idle_end = SchedulerTimelineTrace::Clock::now();
                    recordClusterIdle_(thread_idx, cluster, cycle, reached_cycle - cycle,
                                       idle_begin, idle_end);
                }
                progress.store(reached_cycle, std::memory_order_release);
            } else {
                const bool sample_tick = (cycle & 1023u) == 0;
                SchedulerTimelineTrace::TimePoint begin{};
                if (sample_tick) {
                    begin = SchedulerTimelineTrace::Clock::now();
                }
                executeClusterOneCycle_(thread_idx, cluster, cycle, trace_units);
                if (sample_tick) {
                    auto end = SchedulerTimelineTrace::Clock::now();
                    uint64_t elapsed_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
                    recordClusterTickSample_(cluster, elapsed_ns, true);
                }
                progress.store(cycle + 1, std::memory_order_release);
            }

            release_cluster();
            made_progress = true;
            maybeRequestEpochFreeMigration_(cycle + 1);
            serviceEpochFreeMigration_();
        }

        if (local_done && all_clusters_done()) return;
        if (made_progress) {
            if (blocker.cluster != SIZE_MAX) {
                refreshLookaheadFloor_();
            }
            continue;
        }

        if (local_done) {
            serviceEpochFreeMigration_();
            if (cluster_assignment_generation_.load(std::memory_order_acquire) != seen_generation) {
                continue;
            }
        }

        SchedulerTimelineTrace::TimePoint wait_begin{};
        if (trace_waits) {
            wait_begin = SchedulerTimelineTrace::Clock::now();
        }

        uint64_t spin = 0;
        while (!token.stop_requested()) {
            if ((spin++ & kFloorRefreshSpinMask) == 0) {
                refreshLookaheadFloor_();
                serviceEpochFreeMigration_();
            }
            if (cluster_assignment_generation_.load(std::memory_order_acquire) != seen_generation) {
                break;
            }

            bool any_ready = false;
            bool all_done = local_done ? all_clusters_done() : true;
            for (size_t cluster : owned_clusters) {
                uint64_t cycle =
                    thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed);
                if (cycle < end_cycle) {
                    all_done = false;
                }
                if (!cluster_runtime_owner_ ||
                    cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != thread_idx ||
                    cycle >= end_cycle) {
                    continue;
                }
                BlockedClusterInfo ignored{};
                if (clusterCanAdvance_(cluster, cycle, ignored)) {
                    const uint8_t state = migration_request_.state.load(std::memory_order_acquire);
                    if (cluster_migration_pending_[cluster].load(std::memory_order_acquire) != 0 &&
                        state == static_cast<uint8_t>(MigrationRequestState::Quiescing) &&
                        migration_request_.cluster.load(std::memory_order_relaxed) == cluster &&
                        cycle >= migration_request_.fence_cycle.load(std::memory_order_acquire)) {
                        continue;
                    }
                    any_ready = true;
                    break;
                }
            }
            if (all_done || any_ready) break;
            cpuPause();
        }

        if (trace_waits && !token.stop_requested()) {
            auto wait_end = SchedulerTimelineTrace::Clock::now();
            const char* stall_name = blocker.cluster == SIZE_MAX        ? "stall: no-ready-cluster"
                                     : blocker.pred_cluster == SIZE_MAX ? "stall: lookahead-floor"
                                                                        : "stall: cluster-dep";
            timeline_trace_.recordDuration(
                thread_idx, "wait", stall_name,
                blocker.cluster == SIZE_MAX
                    ? current_cycle_
                    : thread_progress_array_[blocker.cluster].completed_cycle.load(
                          std::memory_order_relaxed),
                wait_begin, wait_end, formatBlockerDetail_(blocker));
        }
    }
}

}  // namespace chronon::sender
