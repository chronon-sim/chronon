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
#include <array>
#include <chrono>
#include <exception>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "../../chronon/CpuPause.hpp"
#include "../../observe/ObservationManager.hpp"
#include "DynamicWaitPolicy.hpp"
#include "TickSimulation.hpp"
#include "TickSimulationCycleUtils.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"
#include "sender/schedule/SchedulerTimelineStyle.hpp"

namespace chronon::sender {

namespace {
// Entering the OS scheduler is far more expensive than a CPU pause/yield hint,
// especially for the short dependency waits common in cycle-level simulation.
// Keep lookahead-floor waits cooperative so lagging workers can advance, but
// only yield after a sustained cluster-dependency wait.
constexpr uint64_t kFloorRefreshSpinMask = detail::kFloorWaitThreadYieldSpinMask;

uint64_t saturatingCycleAdd(uint64_t base, uint64_t delta) noexcept {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return delta > max - base ? max : base + delta;
}

}  // namespace

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

    const uint64_t progress = dynamicActorProgress_(cluster);
    if (progress < fence) return;
    if (clock_mode_ && !clockActorCanMigrate_(cluster)) return;

    uint8_t expected = quiescing;
    if (!migration_request_.state.compare_exchange_strong(
            expected, static_cast<uint8_t>(MigrationRequestState::ReadyToCommit),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return;
    }

    const uint64_t migration_cycle = clock_mode_ ? dynamicMigrationCycle_() : fence;
    cluster_runtime_owner_[cluster].store(target, std::memory_order_release);
    cluster_migration_pending_[cluster].store(0, std::memory_order_release);
    if (cluster < dynamic_cluster_last_migration_cycle_.size()) {
        dynamic_cluster_last_migration_cycle_[cluster] = migration_cycle;
        dynamic_cluster_last_source_thread_[cluster] = source;
        dynamic_cluster_last_target_thread_[cluster] = target;
    }
    cluster_assignment_generation_.fetch_add(1, std::memory_order_acq_rel);

    const uint64_t delay =
        std::max(config_.rebalance_check_interval_cycles, config_.rebalance_cooldown_cycles);
    const uint64_t next_check = saturatingCycleAdd(migration_cycle, delay);
    uint64_t old_next = next_dynamic_rebalance_check_cycle_.load(std::memory_order_relaxed);
    while (next_check > old_next &&
           !next_dynamic_rebalance_check_cycle_.compare_exchange_weak(
               old_next, next_check, std::memory_order_release, std::memory_order_relaxed)) {
    }

    for (size_t c = 0; c < dynamic_runtime_cluster_count_; ++c) {
        const auto cluster_estimate = dynamicClusterRuntimeCost_(c);
        if (cluster_estimate.ready && c < clusters_.clusters.size() &&
            !clusters_.clusters[c].empty()) {
            if (cluster_estimate.low_frequency) {
                for (size_t unit_idx : clusters_.clusters[c]) {
                    if (unit_idx >= unit_costs_.size()) continue;
                    std::atomic_ref<double> unit_cost(unit_costs_[unit_idx]);
                    const auto unit_estimate = dynamicUnitRuntimeCost_(
                        unit_idx, unit_cost.load(std::memory_order_relaxed));
                    if (unit_estimate.ready) {
                        unit_cost.store(unit_estimate.cost, std::memory_order_relaxed);
                    }
                }
            } else {
                const double per_unit =
                    std::max(0.001, cluster_estimate.cost /
                                        static_cast<double>(clusters_.clusters[c].size()));
                for (size_t unit_idx : clusters_.clusters[c]) {
                    if (unit_idx < unit_costs_.size()) {
                        std::atomic_ref<double>(unit_costs_[unit_idx])
                            .store(per_unit, std::memory_order_relaxed);
                    }
                }
            }
        }
        // Clock bridges retain their sparse cumulative estimate, just as the
        // clock units retain per-unit samples. They have no unit_costs_ slot.
        if (c < clusters_.numClusters()) {
            cluster_sample_time_ns_[c].store(0, std::memory_order_relaxed);
            cluster_sample_count_[c].store(0, std::memory_order_relaxed);
            cluster_active_sample_count_[c].store(0, std::memory_order_relaxed);
        }
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
    recordDynamicSchedulerMarker_("Chronon epoch-free rebalance committed", migration_cycle,
                                  last_rebalance_detail_);
    clearDynamicMigrationRequest_();
}

bool TickSimulation::dynamicMigrationBlocksCluster_(size_t cluster, uint64_t cycle) const {
    if (cluster_migration_pending_[cluster].load(std::memory_order_acquire) == 0) return false;

    const uint8_t state = migration_request_.state.load(std::memory_order_acquire);
    const bool committing = state == static_cast<uint8_t>(MigrationRequestState::Quiescing) ||
                            state == static_cast<uint8_t>(MigrationRequestState::ReadyToCommit);
    if (!committing || migration_request_.cluster.load(std::memory_order_relaxed) != cluster) {
        return false;
    }
    return cycle >= migration_request_.fence_cycle.load(std::memory_order_acquire);
}

template <bool PushPeriodicCounters, bool TraceEnabled, bool CheckTraceWindow>
void TickSimulation::executeThreadRunDynamicImpl_(size_t thread_idx, uint64_t end_cycle,
                                                  uint64_t run_start, uint64_t period,
                                                  stdexec::inplace_stop_token token) {
    const uint64_t max_lookahead = config_.max_lookahead_cycles;
    const bool trace_units_enabled = TraceEnabled && timeline_trace_.traceUnits();
    bool trace_units = trace_units_enabled;
    const bool trace_waits_enabled = TraceEnabled && timeline_trace_.traceWaits();
    const auto trace_cycle = [&](bool enabled, uint64_t cycle) {
        if constexpr (CheckTraceWindow) return enabled && timeline_trace_.capturesCycle(cycle);
        return enabled;
    };
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    auto& scratch = schedulerScratch_().workers[thread_idx];
    // Keep the vector headers invocation-local while borrowing their retained
    // allocations. Unit callbacks cannot alias these local ownership headers.
    auto owned_clusters = std::move(scratch.owned_clusters);
    auto refreshed_clusters = std::move(scratch.ownership);
    struct RestoreOwnershipStorage {
        WorkerRunScratch& scratch;
        std::vector<size_t>& owned;
        std::vector<size_t>& refreshed;
        ~RestoreOwnershipStorage() {
            scratch.owned_clusters.swap(owned);
            scratch.ownership.swap(refreshed);
        }
    } restore_ownership{scratch, owned_clusters, refreshed_clusters};
    owned_clusters.clear();
    refreshed_clusters.clear();
    InvocationPredecessorCache predecessor_cache(&scratch.predecessor, thread_progress_count_);
    uint64_t* const predecessor_cycles = predecessor_cache.data();
    // Like predecessor progress, small-graph ranking and readiness are private
    // to this invocation. Pool task stealing must not bounce retained buffer
    // cache lines between cores on every short run. Larger graphs reuse capacity.
    constexpr size_t inline_slots = InvocationPredecessorCache::kInlineSlots;
    alignas(64) std::array<uint64_t, inline_slots> local_priority_blocker;
    alignas(64) std::array<double, inline_slots> local_priority_cost;
    alignas(64) std::array<uint64_t, inline_slots> local_ready_through;
    auto* priority_blocker_ns = local_priority_blocker.data();
    auto* priority_cost_ns = local_priority_cost.data();
    auto* ready_through_cycle = local_ready_through.data();
    if (num_clusters > inline_slots) {
        scratch.priority_blocker.resize(num_clusters);
        scratch.priority_cost.resize(num_clusters);
        scratch.ready_through.resize(num_clusters);
        priority_blocker_ns = scratch.priority_blocker.data();
        priority_cost_ns = scratch.priority_cost.data();
        ready_through_cycle = scratch.ready_through.data();
    }
    // Ranking is written for every owned cluster before sorting. Readiness must
    // start empty even when the previous invocation reached a larger frontier.
    std::fill_n(ready_through_cycle, num_clusters, 0);
    uint64_t seen_generation = 0;
    uint64_t priority_refresh = 0;
    uint64_t wait_sample_sequence = 0;
    observe::ThreadContext* counter_producer = nullptr;
    if constexpr (PushPeriodicCounters) {
        counter_producer = observe::ObservationManager::instance().periodicCounterProducer();
    }

    auto refresh_owned_clusters = [&]() {
        if (!cluster_runtime_owner_) return;
        refreshDynamicOwnedActors_(thread_idx, owned_clusters, refreshed_clusters, seen_generation);
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
        // A final sweep still performs migration/counter/completion handling,
        // but no owned actor at the invocation limit can execute another tick.
        // Avoid estimating and sorting costs for that already-completed work.
        if (std::none_of(owned_clusters.begin(), owned_clusters.end(), [&](size_t cluster) {
                return thread_progress_array_[cluster].completed_cycle.load(
                           std::memory_order_relaxed) < end_cycle;
            }))
            return;
        for (size_t cluster : owned_clusters) {
            priority_blocker_ns[cluster] =
                dynamic_cluster_blocker_wait_ns_
                    ? dynamic_cluster_blocker_wait_ns_[cluster].load(std::memory_order_relaxed)
                    : 0;
            const auto estimate = dynamicClusterRuntimeCost_(cluster);
            priority_cost_ns[cluster] = estimate.ready ? estimate.cost : 0.0;
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
        return dynamicMigrationBlocksCluster_(cluster, cycle);
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
        if (!clusterCanAdvance_(cluster, cycle, blocker, predecessor_cycles,
                                !trace_cycle(trace_waits_enabled, cycle))) {
            return false;
        }

        // Eligibility for the final tick is already proven. No burst can cross
        // this invocation's limit, so a later dependency frontier is unused.
        if (cycle + 1 == end_cycle) {
            ready_through_cycle[cluster] = end_cycle;
            return true;
        }

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

    auto enable_cluster_unit_sampling = [&](size_t cluster, uint64_t completed_cycle) {
        if (cluster >= dynamic_cluster_unit_sampling_.size() ||
            dynamic_cluster_unit_sampling_[cluster] != 0) {
            return;
        }
        dynamic_cluster_unit_sampling_[cluster] = 1;
        if (cluster >= clusters_.clusters.size()) return;
        for (size_t unit : clusters_.clusters[cluster]) {
            if (unit >= dynamic_runtime_unit_count_) continue;
            dynamic_unit_last_active_sample_cycle_[unit] = completed_cycle;
            dynamic_unit_last_inactive_sample_cycle_[unit] = completed_cycle;
            dynamic_unit_last_activity_checkpoint_cycle_[unit] = completed_cycle;
            dynamic_unit_active_ticks_since_checkpoint_[unit] = 0;
        }
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

            if (cluster < dynamic_cluster_unit_sampling_.size() &&
                dynamic_cluster_unit_sampling_[cluster] == 0 && cluster_activity_scheduling_ &&
                cluster_activity_scheduling_[cluster].enabled.load(std::memory_order_acquire)) {
                enable_cluster_unit_sampling(cluster, cycle == 0 ? 0 : cycle - 1);
            }

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
                if constexpr (CheckTraceWindow) {
                    trace_units = trace_cycle(trace_units_enabled, cycle);
                }
                const bool sample_units = cluster < dynamic_cluster_unit_sampling_.size() &&
                                          dynamic_cluster_unit_sampling_[cluster] != 0;
                SchedulerTimelineTrace::TimePoint idle_begin{};
                if (trace_units || sample_units) {
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
                if (sample_units && reached_cycle > cycle && cluster < clusters_.clusters.size()) {
                    const auto idle_end = SchedulerTimelineTrace::Clock::now();
                    const uint64_t delta = reached_cycle - cycle;
                    const uint64_t elapsed_ns = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(idle_end - idle_begin)
                            .count());
                    const size_t units = std::max<size_t>(1, clusters_.clusters[cluster].size());
                    const uint64_t per_unit_cycle_ns = elapsed_ns / delta / units;
                    for (size_t unit : clusters_.clusters[cluster]) {
                        if (unit >= dynamic_runtime_unit_count_) continue;
                        if (detail::shouldSampleDynamicTick(
                                reached_cycle - 1,
                                dynamic_unit_last_inactive_sample_cycle_[unit])) {
                            recordDynamicUnitTickSample_(unit, per_unit_cycle_ns, false);
                            dynamic_unit_last_inactive_sample_cycle_[unit] = reached_cycle - 1;
                        }

                        const uint64_t last = dynamic_unit_last_activity_checkpoint_cycle_[unit];
                        const uint64_t first_unobserved =
                            last == detail::kNoDynamicTickSample ? 0 : last + 1;
                        if (reached_cycle > first_unobserved) {
                            dynamic_unit_observed_active_ticks_[unit].fetch_add(
                                dynamic_unit_active_ticks_since_checkpoint_[unit],
                                std::memory_order_relaxed);
                            dynamic_unit_observed_cycles_[unit].fetch_add(
                                reached_cycle - first_unobserved, std::memory_order_relaxed);
                            dynamic_unit_active_ticks_since_checkpoint_[unit] = 0;
                            dynamic_unit_last_activity_checkpoint_cycle_[unit] = reached_cycle - 1;
                        }
                    }
                }
                progress.store(reached_cycle, std::memory_order_release);
                predecessor_cycles[cluster] = reached_cycle;
            } else {
                uint64_t next_counter_cycle = UINT64_MAX;
                if constexpr (PushPeriodicCounters) {
                    next_counter_cycle = detail::nextPeriodicCycle(cycle, period);
                }
                const uint64_t burst_end = detail::dynamicClusterBurstEnd(
                    cycle, end_cycle, ready_through_cycle[cluster], next_counter_cycle);
                // Ownership cannot migrate within this burst. Cache the stable
                // topology and owner-private sampling state, while still
                // observing runtime activity opt-in after every tick below.
                const auto units = std::span(cluster_unit_ptrs_[cluster]);
                bool sample_units = cluster < dynamic_cluster_unit_sampling_.size() &&
                                    dynamic_cluster_unit_sampling_[cluster] != 0;
                uint64_t last_sample = dynamic_cluster_last_tick_sample_cycle_[cluster];
                do {
                    const bool sample_tick =
                        !sample_units && detail::shouldSampleDynamicTick(cycle, last_sample);
                    SchedulerTimelineTrace::TimePoint begin{};
                    if (sample_tick) begin = SchedulerTimelineTrace::Clock::now();
                    if constexpr (CheckTraceWindow) {
                        trace_units = trace_cycle(trace_units_enabled, cycle);
                    }
                    if (!trace_units && !sample_units) {
                        // No trace scratch or per-unit samples are needed in this hot path.
                        for (auto* unit : units) {
                            executeUnitCycle_(unit, cycle);
                        }
                    } else {
                        executeClusterOneCycle_(thread_idx, cluster, cycle, trace_units,
                                                sample_units);
                    }
                    if (sample_tick) {
                        auto end = SchedulerTimelineTrace::Clock::now();
                        uint64_t elapsed_ns = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                                .count());
                        recordClusterTickSample_(cluster, elapsed_ns, true);
                        last_sample = cycle;
                        dynamic_cluster_last_tick_sample_cycle_[cluster] = last_sample;
                    }
                    if (!sample_units && cluster_activity_scheduling_ &&
                        cluster_activity_scheduling_[cluster].enabled.load(
                            std::memory_order_acquire)) {
                        enable_cluster_unit_sampling(cluster, cycle);
                        sample_units = cluster < dynamic_cluster_unit_sampling_.size() &&
                                       dynamic_cluster_unit_sampling_[cluster] != 0;
                    }
                    ++cycle;
                    progress.store(cycle, std::memory_order_release);
                } while (cycle < burst_end && !token.stop_requested());
                // All work in this burst is sequenced before local dependents.
                // The bound remains valid if ownership migrates afterwards.
                predecessor_cycles[cluster] = cycle;
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
        uint64_t wait_cycle = 0;
        if (trace_waits_enabled) {
            wait_cycle = blocker.cluster == SIZE_MAX
                             ? current_cycle_
                             : thread_progress_array_[blocker.cluster].completed_cycle.load(
                                   std::memory_order_relaxed);
        }
        const bool trace_waits = trace_cycle(trace_waits_enabled, wait_cycle);
        const bool sample_wait = trace_waits || ((wait_sample_sequence++ & kWaitSampleMask) == 0);
        if (sample_wait && !trace_waits && blocker.cluster < num_clusters) {
            refineDynamicWaitBlocker_(thread_idx, owned_clusters, stable_sweep, end_cycle,
                                      predecessor_cycles, blocker);
        }
        bool sampled_wait_on_floor = false;
        if (sample_wait && blocker.cluster < num_clusters) {
            // A worker ahead of the exact frontier is consuming lookahead
            // slack, so its dependency wait overlaps useful work elsewhere.
            // Classify only sampled waits to keep this global scan off the
            // common no-progress spin path.
            uint64_t exact_floor = std::numeric_limits<uint64_t>::max();
            for (size_t cluster = 0; cluster < num_clusters; ++cluster) {
                exact_floor =
                    std::min(exact_floor, thread_progress_array_[cluster].completed_cycle.load(
                                              std::memory_order_acquire));
            }
            const uint64_t dependent_cycle =
                thread_progress_array_[blocker.cluster].completed_cycle.load(
                    std::memory_order_acquire);
            sampled_wait_on_floor =
                epoch_free_cost::isUnhiddenDependencyWait(dependent_cycle, exact_floor);
        }
        SchedulerTimelineTrace::TimePoint wait_begin{};
        if (sample_wait) wait_begin = SchedulerTimelineTrace::Clock::now();

        uint64_t spin = 0;
        const uint64_t thread_yield_spin_mask =
            detail::dynamicWaitThreadYieldSpinMask(blocker.pred_cluster == SIZE_MAX);
        auto pause_or_yield_hidden_wait = [&](uint64_t spin_iteration) {
            if (detail::shouldYieldDynamicWaitThread(spin_iteration, thread_yield_spin_mask) &&
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
            if (blocker.cluster == SIZE_MAX || blocker.pred_cluster == SIZE_MAX ||
                sampled_wait_on_floor) {
                recordDynamicWaitSample_(thread_idx, blocker, wait_ns);
            }
            if (!trace_waits) continue;

            const char* stall_name = blocker.cluster == SIZE_MAX        ? "stall: no-ready-cluster"
                                     : blocker.pred_cluster == SIZE_MAX ? "stall: lookahead-floor"
                                                                        : "stall: cluster-dep";
            const auto stall_style = schedulerStallStyle(stall_name);
            timeline_trace_.recordDuration(thread_idx, stall_style.category, stall_style.name,
                                           wait_cycle, wait_begin, wait_end,
                                           formatBlockerDetail_(blocker));
        }
    }
}

void TickSimulation::executeThreadRunDynamic_(size_t thread_idx, uint64_t end_cycle,
                                              stdexec::inplace_stop_token token) {
    if (!timeline_trace_.needsWorkerTracing(end_cycle)) {
        executeThreadRunDynamicImpl_<false, false, false>(thread_idx, end_cycle, 0, 0, token);
    } else if (timeline_trace_.needsWorkerCycleChecks(end_cycle)) {
        executeThreadRunDynamicImpl_<false, true, true>(thread_idx, end_cycle, 0, 0, token);
    } else {
        executeThreadRunDynamicImpl_<false, true, false>(thread_idx, end_cycle, 0, 0, token);
    }
}

void TickSimulation::executeThreadRunDynamicWithPeriodicCounters_(
    size_t thread_idx, uint64_t end_cycle, uint64_t run_start, uint64_t period,
    stdexec::inplace_stop_token token) {
    if (!timeline_trace_.needsWorkerTracing(end_cycle)) {
        executeThreadRunDynamicImpl_<true, false, false>(thread_idx, end_cycle, run_start, period,
                                                         token);
    } else if (timeline_trace_.needsWorkerCycleChecks(end_cycle)) {
        executeThreadRunDynamicImpl_<true, true, true>(thread_idx, end_cycle, run_start, period,
                                                       token);
    } else {
        executeThreadRunDynamicImpl_<true, true, false>(thread_idx, end_cycle, run_start, period,
                                                        token);
    }
}

}  // namespace chronon::sender
