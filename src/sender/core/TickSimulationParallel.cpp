// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// TickSimulation parallel runtime: epoch-free execution, cross-thread
/// dependency spin-waits, and progress-sync initialization.

#include <bit>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <limits>
#include <new>
#include <string>
#include <unordered_map>

#if defined(__linux__)
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"
#include "TickSimulationCycleUtils.hpp"
#include "sender/schedule/SchedulerTimelineStyle.hpp"

namespace chronon::sender {

namespace {
/// Throttle for the slow-path floor refresh: scan once per 256 spins (refresh
/// fires when (spin & mask) == 0) to keep the cross-core scan off the hot path.
constexpr uint64_t kFloorRefreshSpinMask = 0xFF;

uint64_t saturatingCycleAdd(uint64_t base, uint64_t delta) noexcept {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return delta > max - base ? max : base + delta;
}

}  // namespace

// ---------------------------------------------------------------------------
// MPSC producer progress wiring
// ---------------------------------------------------------------------------

void TickSimulation::installMultiProducerProgress_() {
    if (!thread_progress_array_ || thread_progress_count_ == 0) return;

    std::unordered_map<Unit*, size_t> unit_to_cluster;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        if (i < unit_to_cluster_.size()) {
            unit_to_cluster[static_cast<Unit*>(unit_ptrs_[i])] = unit_to_cluster_[i];
        }
    }

    // Per-connection producer progress proves that every direct lane is covered
    // by the epoch-free scheduler's dependency protocol.
    std::unordered_map<Unit*, const std::atomic<uint64_t>*> src_to_progress;
    src_to_progress.reserve(unit_to_cluster.size());
    for (const auto& [unit, cluster] : unit_to_cluster) {
        if (cluster < thread_progress_count_) {
            src_to_progress[unit] = &thread_progress_array_[cluster].completed_cycle;
        }
    }

    for (IMultiProducerPort* port : multi_producer_ports_) {
        port->setProducerProgress(src_to_progress);
    }
}

// ---------------------------------------------------------------------------
// Progress-sync allocation
// ---------------------------------------------------------------------------

void TickSimulation::freeThreadProgressArray() {
    if (thread_progress_array_) {
        for (size_t i = 0; i < thread_progress_count_; ++i) {
            thread_progress_array_[i].~ThreadProgress();
        }
        std::free(thread_progress_array_);
        thread_progress_array_ = nullptr;
        thread_progress_count_ = 0;
    }
}

void TickSimulation::initProgressSync() {
    if (thread_units_.size() <= 1) return;

    const size_t num_threads = thread_units_.size();
    const size_t num_clusters = clusters_.numClusters();
    if (num_clusters == 0) return;

    freeThreadProgressArray();
    thread_progress_count_ = num_clusters;
    void* mem = std::aligned_alloc(64, num_clusters * sizeof(ThreadProgress));
    thread_progress_array_ = static_cast<ThreadProgress*>(mem);
    for (size_t i = 0; i < num_clusters; ++i) {
        new (&thread_progress_array_[i]) ThreadProgress();
        uint64_t cycle = 0;
        if (i < clusters_.clusters.size() && !clusters_.clusters[i].empty()) {
            size_t unit_idx = clusters_.clusters[i].front();
            if (unit_idx < unit_ptrs_.size()) {
                cycle = unit_ptrs_[unit_idx]->localCycle();
            }
        }
        thread_progress_array_[i].completed_cycle.store(cycle, std::memory_order_relaxed);
    }

    thread_resolved_deps_.resize(num_clusters);
    for (size_t c = 0; c < num_clusters; ++c) {
        thread_resolved_deps_[c].clear();
        if (c < thread_cross_deps_temp_.size()) {
            for (const auto& dep : thread_cross_deps_temp_[c]) {
                if (dep.pred_thread < thread_progress_count_) {
                    thread_resolved_deps_[c].push_back(
                        {&thread_progress_array_[dep.pred_thread].completed_cycle, dep.min_delay,
                         dep.pred_thread});
                }
            }
        }
        // Synthetic dep: cluster cannot advance past lookahead_floor_ + max_lookahead_cycles.
        // num_clusters is its reserved cache slot; blocker diagnostics map it
        // back to SIZE_MAX because it is not a real predecessor cluster.
        // A zero window disables epoch-free execution before progress setup.
        if (config_.max_lookahead_cycles > 0) {
            thread_resolved_deps_[c].push_back(
                {&lookahead_floor_, config_.max_lookahead_cycles, /*pred_id=*/num_clusters});
        }
    }

    thread_unit_ptrs_.resize(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        thread_unit_ptrs_[t].clear();
        for (size_t idx : thread_units_[t]) {
            thread_unit_ptrs_[t].push_back(unit_ptrs_[idx]);
        }
    }

    thread_clusters_.clear();
    thread_clusters_.resize(num_threads);
    for (size_t c = 0; c < num_clusters; ++c) {
        if (c >= cluster_to_thread_.size()) continue;
        size_t thread = cluster_to_thread_[c];
        if (thread < thread_clusters_.size()) {
            thread_clusters_[thread].push_back(c);
        }
    }

    cluster_unit_ptrs_.clear();
    cluster_unit_ptrs_.resize(num_clusters);

    for (size_t c = 0; c < num_clusters; ++c) {
        for (size_t unit_idx : clusters_.clusters[c]) {
            cluster_unit_ptrs_[c].push_back(unit_ptrs_[unit_idx]);
        }
    }

    if (observe_ctx_) {
        observe::log_info<"  Initialized cluster progress sync for {} clusters on {} threads">(
            observe_ctx_, num_clusters, num_threads);
    }

    if (config_.enable_dynamic_rebalance) {
        initDynamicMigrationRuntime_();
    }
}

bool TickSimulation::allMultiProducerPortsHaveProgress_() const noexcept {
    for (const IMultiProducerPort* p : multi_producer_ports_) {
        if (!p->producerProgressFullyResolved()) return false;
    }
    return true;
}

uint64_t TickSimulation::completedCyclesForRun_(uint64_t run_start,
                                                uint64_t run_target) const noexcept {
    uint64_t reached = thread_progress_array_[0].completed_cycle.load(std::memory_order_relaxed);

    if (termination_ctrl_.isTerminationRequested()) {
        const auto& request = termination_ctrl_.getRequest();
        if (request.cycle >= run_start) {
            const uint64_t stop_reached =
                std::min(saturatingCycleAdd(request.cycle, 1), run_target);
            reached = std::max(reached, stop_reached);
        }
    }

    reached = std::min(reached, run_target);
    return reached > run_start ? reached - run_start : 0;
}

bool TickSimulation::crossThreadHeadroomFits_(uint64_t max_lookahead) const noexcept {
    return crossThreadHeadroomLimit_() > max_lookahead;
}

bool TickSimulation::crossThreadHeadroomAllowsEpochFree_() const noexcept {
    if (has_zero_delay_cross_thread_cycle_) return false;
    for (const ConnectionBase* c : connections_) {
        if (c->crossThreadHeadroom() == 0) return false;
    }
    return true;
}

size_t TickSimulation::prepareEpochFreeHeadroom_() {
    if (!config_.enable_epoch_free_lookahead) return 0;
    std::unordered_map<void*, std::vector<ConnectionBase*>> by_port;
    for (auto* conn : connections_) {
        by_port[conn->destPortPtr()].push_back(conn);
    }

    size_t unproven_count = 0;
    for (auto& [port, conns] : by_port) {
        (void)port;
        bool unsafe = false;
        for (auto* conn : conns) {
            if (conn->crossThreadHeadroom() == 0 &&
                !conn->ensureEpochFreeHeadroom(config_.max_lookahead_cycles)) {
                unsafe = true;
                break;
            }
        }
        if (!unsafe) continue;
        unproven_count += conns.size();
    }
    return unproven_count;
}

size_t TickSimulation::crossThreadHeadroomLimit_() const noexcept {
    size_t limit = std::numeric_limits<size_t>::max();
    for (const ConnectionBase* c : connections_) {
        limit = std::min(limit, c->crossThreadHeadroom());
    }
    return limit;
}

uint64_t TickSimulation::executeRunEpochFree_(uint64_t total_cycles) {
    const size_t nthreads = thread_units_.size();
    if (nthreads == 0 || total_cycles == 0) return 0;

    auto token = stop_source_->get_token();
    auto sched = pool_.get_scheduler();

    const uint64_t run_start =
        thread_progress_array_[0].completed_cycle.load(std::memory_order_relaxed);
    const uint64_t run_target = saturatingCycleAdd(run_start, total_cycles);
    auto& obs_mgr = observe::ObservationManager::instance();
    const bool push_periodic_counters = obs_mgr.periodicCounterSnapshotsEnabled();
    const uint64_t counter_period = push_periodic_counters ? obs_mgr.periodicDumpCycles() : 0;

    // Single run-spanning window: no per-epoch barrier. Run-ahead is bounded
    // solely by the lookahead_floor_ + max_lookahead_cycles synthetic dep
    // (installed in initProgressSync), seeded here at run_start and raised
    // monotonically by refreshLookaheadFloor_ on the slow path as the
    // global-min cluster advances. The caller guarantees max_lookahead_cycles>0
    // (so the floor, not an epoch boundary, is the cap) and full per-connection
    // MPSC progress (so no port needs a per-epoch central flush).
    lookahead_floor_.store(run_start, std::memory_order_relaxed);

    SchedulerTimelineTrace::TimePoint run_begin{};
    if (timeline_trace_.traceEpochs()) {
        run_begin = SchedulerTimelineTrace::Clock::now();
    }

    std::exception_ptr captured;
    std::atomic_flag captured_set = ATOMIC_FLAG_INIT;

    auto work =
        stdexec::bulk(stdexec::just(), stdexec::par, nthreads, [&, token](std::size_t thread_idx) {
            // Drive this worker's clusters straight to run_target. The try-catch
            // captures the first exception and requests stop so peers leave their
            // dependency spin-waits; sync_wait below is the sole join.
            try {
                if (push_periodic_counters) {
                    executeThreadRunWithPeriodicCounters_(thread_idx, run_target, run_start,
                                                          counter_period, token);
                } else {
                    executeThreadRun_(thread_idx, run_target, token);
                }
            } catch (...) {
                if (!captured_set.test_and_set(std::memory_order_relaxed)) {
                    captured = std::current_exception();
                }
                stop_source_->request_stop();
            }
        });

    stdexec::sync_wait(stdexec::starts_on(sched, std::move(work)));

    if (captured) std::rethrow_exception(captured);

    if (timeline_trace_.traceEpochs()) {
        auto run_end = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler",
                                       "epoch-free lookahead run", run_start, run_begin, run_end,
                                       "cycles=" + std::to_string(total_cycles));
    }

    for (size_t i = 0; i < units_.size(); ++i) {
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_release);
    }

    return completedCyclesForRun_(run_start, run_target);
}

// ---------------------------------------------------------------------------
// Per-thread run driver
// ---------------------------------------------------------------------------

template <bool PushPeriodicCounters>
void TickSimulation::executeThreadRunImpl_(size_t thread_idx, uint64_t end_cycle,
                                           uint64_t run_start, uint64_t period,
                                           stdexec::inplace_stop_token token) {
    const auto& clusters = thread_clusters_[thread_idx];
    const bool trace_units = timeline_trace_.traceUnits();
    // This cache spans the worker invocation (the entire run in EpochFree mode),
    // allowing all locally-owned clusters to reuse acquired predecessor progress.
    WorkerPredecessorCycleCache predecessor_cache(thread_progress_count_);
    uint64_t* const predecessor_cycles = predecessor_cache.data();
    const bool trace_waits = timeline_trace_.traceWaits();
    const bool stop_on_first_blocker = !trace_waits;
    observe::ThreadContext* counter_producer = nullptr;
    uint64_t next_counter_cycle = UINT64_MAX;
    if constexpr (PushPeriodicCounters) {
        counter_producer = observe::ObservationManager::instance().periodicCounterProducer();
        next_counter_cycle = detail::nextPeriodicCycle(run_start, period);
    }

    while (true) {
        bool all_done = true;
        bool made_progress = false;
        BlockedClusterInfo blocker{};

        for (size_t cluster : clusters) {
            auto& progress = thread_progress_array_[cluster].completed_cycle;
            uint64_t cycle = progress.load(std::memory_order_relaxed);
            if (cycle >= end_cycle) continue;
            all_done = false;

            BlockedClusterInfo candidate{};
            if (!clusterCanAdvance_(cluster, cycle, candidate, predecessor_cycles,
                                    stop_on_first_blocker)) {
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
                // A cross-thread wakeAt() may lower next_active_cycle_ while
                // this worker is in the bulk idle path. Re-sample before
                // publishing completed_cycle so peers never observe an idle
                // hop past an already-visible wake.
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
                executeClusterOneCycle_(thread_idx, cluster, cycle, trace_units);
                progress.store(cycle + 1, std::memory_order_release);
            }
            // Floor not recomputed per advance (that was an O(num_clusters)
            // cross-core scan on the hot path). It is refreshed lazily, only
            // when a cluster is actually blocked — see below and the spin-wait.
            made_progress = true;
        }

        if constexpr (PushPeriodicCounters) {
            // One predictable check per worker sweep, never per counter
            // increment. The first owned cluster is the sampling clock; other
            // clusters may differ by up to the configured lookahead window.
            if (counter_producer && next_counter_cycle <= end_cycle && !clusters.empty()) {
                const uint64_t observed =
                    thread_progress_array_[clusters.front()].completed_cycle.load(
                        std::memory_order_relaxed);
                while (OBSERVE_UNLIKELY(observed >= next_counter_cycle &&
                                        next_counter_cycle <= end_cycle)) {
                    const bool pushed =
                        observe::ObservationManager::instance().pushPeriodicCounterSnapshots(
                            next_counter_cycle, clusters, *counter_producer);
                    if (!pushed) break;
                    const uint64_t prior_cycle = next_counter_cycle;
                    next_counter_cycle = detail::nextPeriodicCycle(next_counter_cycle, period);
                    if (next_counter_cycle <= prior_cycle) break;
                }
            }
        }

        if (all_done || token.stop_requested()) return;
        if (made_progress) {
            // A worker advancing one cluster while another is stuck at the
            // ceiling never reaches the spin-wait; refresh here so the blocked
            // cluster is not pinned to a stale global-floor ceiling.
            // blocker.cluster is set iff some cluster failed clusterCanAdvance_;
            // this fires at most once per executed cycle, so it needs no throttle.
            if (blocker.cluster != SIZE_MAX) {
                refreshLookaheadFloor_();
            }
            continue;
        }

        SchedulerTimelineTrace::TimePoint wait_begin{};
        if (trace_waits) {
            wait_begin = SchedulerTimelineTrace::Clock::now();
        }

        // Refresh the floor as peers advance to lift the ceiling; throttled
        // (kFloorRefreshSpinMask) so the tight spin doesn't hammer the scan.
        uint64_t spin = 0;
        while (!token.stop_requested()) {
            if ((spin++ & kFloorRefreshSpinMask) == 0) {
                refreshLookaheadFloor_();
            }
            bool any_ready = false;
            for (size_t cluster : clusters) {
                uint64_t cycle =
                    thread_progress_array_[cluster].completed_cycle.load(std::memory_order_relaxed);
                if (cycle >= end_cycle) continue;
                BlockedClusterInfo ignored{};
                if (clusterCanAdvance_(cluster, cycle, ignored, predecessor_cycles,
                                       stop_on_first_blocker)) {
                    any_ready = true;
                    break;
                }
            }
            if (any_ready) break;
            cpuPause();
        }

        if (trace_waits && !token.stop_requested()) {
            auto wait_end = SchedulerTimelineTrace::Clock::now();
            // Tag the stall by cause so the timeline distinguishes recoverable
            // lookahead-floor stalls (widen the window) from genuine cross-cluster
            // dependency stalls (critical path, not tunable).
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

void TickSimulation::executeThreadRun_(size_t thread_idx, uint64_t end_cycle,
                                       stdexec::inplace_stop_token token) {
    executeThreadRunImpl_<false>(thread_idx, end_cycle, 0, 0, token);
}

void TickSimulation::executeThreadRunWithPeriodicCounters_(size_t thread_idx, uint64_t end_cycle,
                                                           uint64_t run_start, uint64_t period,
                                                           stdexec::inplace_stop_token token) {
    executeThreadRunImpl_<true>(thread_idx, end_cycle, run_start, period, token);
}

// ---------------------------------------------------------------------------
// Cluster advancement and execution
// ---------------------------------------------------------------------------

void TickSimulation::refreshLookaheadFloor_() {
    // Monotonically raise lookahead_floor_ to the global-min completed cycle.
    // Relaxed throughout — see the memory-order contract on its declaration.
    uint64_t old_floor = lookahead_floor_.load(std::memory_order_relaxed);
    uint64_t global_min = UINT64_MAX;
    for (size_t c = 0; c < thread_progress_count_; ++c) {
        global_min = std::min(
            global_min, thread_progress_array_[c].completed_cycle.load(std::memory_order_relaxed));
    }
    while (global_min > old_floor) {
        if (lookahead_floor_.compare_exchange_weak(old_floor, global_min,
                                                   std::memory_order_relaxed)) {
            break;
        }
    }
}

bool TickSimulation::clusterCanAdvance_(size_t cluster, uint64_t cycle, BlockedClusterInfo& blocker,
                                        uint64_t* predecessor_cache,
                                        bool stop_on_first_blocker) const {
    if (cluster >= thread_resolved_deps_.size()) return true;

    // Treat predecessor dependencies as SIMT lanes. The ballot pass is local
    // and branch-free with respect to cache hit/miss. Only lanes whose cached
    // lower bound is insufficient execute the remote acquire slow path. Normal
    // scheduler fan-in fits one machine-word mask; keep unusual larger sets out
    // of this hot function so their chunking state does not cause spills here.
    constexpr size_t kMinMaskLanes = 4;
    constexpr size_t kMaxMaskLanes = 64;
    const auto& deps = thread_resolved_deps_[cluster];
    if (deps.size() < kMinMaskLanes || deps.size() > kMaxMaskLanes) {
        return clusterCanAdvanceScalarSlow_(cluster, cycle, blocker, predecessor_cache);
    }

    uint64_t refresh_mask = 0;
    for (size_t lane = 0; lane < deps.size(); ++lane) {
        const auto& dep = deps[lane];
        const uint64_t needed = cycle + 1 > dep.min_delay ? cycle + 1 - dep.min_delay : 0;
        const uint64_t cached = predecessor_cache[dep.pred_id];
        refresh_mask |= static_cast<uint64_t>(cached < needed) << lane;
    }

    if (refresh_mask == 0) [[likely]]
        return true;
    return refreshPredecessorMisses_(cluster, cycle, blocker, predecessor_cache, refresh_mask,
                                     stop_on_first_blocker);
}

bool TickSimulation::refreshPredecessorMisses_(size_t cluster, uint64_t cycle,
                                               BlockedClusterInfo& blocker,
                                               uint64_t* predecessor_cache, uint64_t refresh_mask,
                                               bool stop_on_first_blocker) const {
    const auto& deps = thread_resolved_deps_[cluster];
    bool ready = true;
    while (refresh_mask != 0) {
        const unsigned lane = std::countr_zero(refresh_mask);
        refresh_mask &= refresh_mask - 1;

        const auto& dep = deps[lane];
        const uint64_t needed = cycle + 1 > dep.min_delay ? cycle + 1 - dep.min_delay : 0;
        const uint64_t observed = dep.progress_ptr->load(std::memory_order_acquire);
        predecessor_cache[dep.pred_id] = observed;
        if (observed >= needed) continue;

        ready = false;
        const uint64_t deficit = needed - observed;
        if (deficit > blocker.deficit) {
            blocker.cluster = cluster;
            blocker.pred_cluster = dep.pred_id == thread_progress_count_ ? SIZE_MAX : dep.pred_id;
            blocker.needed = needed;
            blocker.observed = observed;
            blocker.delay = dep.min_delay;
            blocker.deficit = deficit;
        }
        if (stop_on_first_blocker) return false;
    }
    return ready;
}

bool TickSimulation::clusterCanAdvanceScalarSlow_(size_t cluster, uint64_t cycle,
                                                  BlockedClusterInfo& blocker,
                                                  uint64_t* predecessor_cache) const {
    bool ready = true;
    for (const auto& dep : thread_resolved_deps_[cluster]) {
        const uint64_t needed = cycle + 1 > dep.min_delay ? cycle + 1 - dep.min_delay : 0;
        const uint64_t observed = observePredecessorCycle_(dep, needed, predecessor_cache);
        if (observed >= needed) continue;

        ready = false;
        const uint64_t deficit = needed - observed;
        if (deficit > blocker.deficit) {
            blocker.cluster = cluster;
            blocker.pred_cluster = dep.pred_id == thread_progress_count_ ? SIZE_MAX : dep.pred_id;
            blocker.needed = needed;
            blocker.observed = observed;
            blocker.delay = dep.min_delay;
            blocker.deficit = deficit;
        }
    }
    return ready;
}

uint64_t TickSimulation::computeIdleClusterTarget_(size_t cluster, uint64_t cycle,
                                                   uint64_t end_cycle,
                                                   uint64_t* predecessor_cache) const {
    if (cluster >= cluster_unit_ptrs_.size()) {
        return cycle;
    }

    uint64_t next_active = end_cycle;
    for (auto* unit : cluster_unit_ptrs_[cluster]) {
        uint64_t unit_next = unit->nextRunnableCycleAtOrAfter(cycle);
        if (unit_next <= cycle) {
            return cycle;
        }
        next_active = std::min(next_active, unit_next);
    }

    uint64_t dep_limit = end_cycle;
    if (cluster < thread_resolved_deps_.size()) {
        for (const auto& dep : thread_resolved_deps_[cluster]) {
            const uint64_t candidate = std::min(next_active, dep_limit);
            const uint64_t needed = candidate > dep.min_delay ? candidate - dep.min_delay : 0;
            uint64_t observed = observePredecessorCycle_(dep, needed, predecessor_cache);
            uint64_t limit =
                observed > UINT64_MAX - dep.min_delay ? UINT64_MAX : observed + dep.min_delay;
            dep_limit = std::min(dep_limit, limit);
        }
    }

    uint64_t target = std::min(next_active, dep_limit);
    if (target <= cycle) {
        return cycle;
    }
    return target;
}

void TickSimulation::advanceClusterIdle_(size_t cluster, uint64_t delta) {
    if (delta == 0 || cluster >= cluster_unit_ptrs_.size()) {
        return;
    }
    for (auto* unit : cluster_unit_ptrs_[cluster]) {
        unit->advanceIdleTick(delta);
    }
}

void TickSimulation::recordClusterIdle_(size_t thread_idx, size_t cluster, uint64_t cycle,
                                        uint64_t delta, SchedulerTimelineTrace::TimePoint begin,
                                        SchedulerTimelineTrace::TimePoint end) {
    if (delta == 0 || cluster >= cluster_unit_ptrs_.size()) {
        return;
    }

    std::string detail = "cycles=" + std::to_string(delta);
    for (auto* unit : cluster_unit_ptrs_[cluster]) {
        timeline_trace_.recordDuration(thread_idx, "unit idle", unit->name(), cycle, begin, end,
                                       detail);
    }
}

std::string TickSimulation::formatBlockerDetail_(const BlockedClusterInfo& blocker) const {
    if (blocker.cluster == SIZE_MAX) {
        return "no-ready-cluster";
    }
    return "cluster=" + std::to_string(blocker.cluster) +
           " pred_cluster=" + std::to_string(blocker.pred_cluster) +
           " needed=" + std::to_string(blocker.needed) +
           " observed=" + std::to_string(blocker.observed) +
           " delay=" + std::to_string(blocker.delay);
}

void TickSimulation::executeClusterOneCycle_(size_t thread_idx, size_t cluster, uint64_t cycle,
                                             bool trace_units, bool sample_unit_activity) {
    auto* const* units = cluster_unit_ptrs_[cluster].data();
    const size_t num_units = cluster_unit_ptrs_[cluster].size();

    auto unit_index = [&](size_t offset) {
        return cluster < clusters_.clusters.size() && offset < clusters_.clusters[cluster].size()
                   ? clusters_.clusters[cluster][offset]
                   : SIZE_MAX;
    };
    auto record_activity = [&](size_t unit, bool active) {
        if (unit >= dynamic_runtime_unit_count_) return;
        if (active) ++dynamic_unit_active_ticks_since_checkpoint_[unit];
        const uint64_t last = dynamic_unit_last_activity_checkpoint_cycle_[unit];
        if (!detail::shouldSampleDynamicTick(cycle, last)) return;
        const uint64_t window_cycles =
            last == detail::kNoDynamicTickSample ? cycle + 1 : cycle - last;
        dynamic_unit_observed_active_ticks_[unit].fetch_add(
            dynamic_unit_active_ticks_since_checkpoint_[unit], std::memory_order_relaxed);
        dynamic_unit_observed_cycles_[unit].fetch_add(window_cycles, std::memory_order_relaxed);
        dynamic_unit_active_ticks_since_checkpoint_[unit] = 0;
        dynamic_unit_last_activity_checkpoint_cycle_[unit] = cycle;
    };
    auto record_cost_sample = [&](size_t unit, bool active, uint64_t elapsed_ns) {
        if (unit >= dynamic_runtime_unit_count_) return;
        auto& last = active ? dynamic_unit_last_active_sample_cycle_[unit]
                            : dynamic_unit_last_inactive_sample_cycle_[unit];
        if (!detail::shouldSampleDynamicTick(cycle, last)) return;
        recordDynamicUnitTickSample_(unit, elapsed_ns, active);
        last = cycle;
    };

    if (trace_units) {
        auto& points = thread_trace_points_[thread_idx];
        if (points.size() < num_units + 1) {
            points.resize(num_units + 1);
        }
        const bool trace_thread_cpu = timeline_trace_.traceThreadCpuTime();
        auto* cpu_points = trace_thread_cpu ? &thread_trace_cpu_points_[thread_idx] : nullptr;
        if (cpu_points && cpu_points->size() < num_units + 1) {
            cpu_points->resize(num_units + 1);
        }
        std::vector<char> active(num_units, 0);
        points[0] = SchedulerTimelineTrace::Clock::now();
        if (cpu_points) {
            (*cpu_points)[0] = threadTraceCpuPoint_();
        }
        for (size_t u = 0; u < num_units; ++u) {
            active[u] = executeUnitCycle_(units[u], cycle);
            points[u + 1] = SchedulerTimelineTrace::Clock::now();
            if (cpu_points) {
                (*cpu_points)[u + 1] = threadTraceCpuPoint_();
            }
        }
        for (size_t u = 0; u < num_units; ++u) {
            recordUnitDuration_(thread_idx, active[u] ? "unit" : "unit idle", units[u]->name(),
                                cycle, points[u], points[u + 1], active[u] ? "" : "cycles=1",
                                trace_thread_cpu,
                                cpu_points ? (*cpu_points)[u] : ThreadTraceCpuPoint{},
                                cpu_points ? (*cpu_points)[u + 1] : ThreadTraceCpuPoint{});
            if (sample_unit_activity) {
                const size_t unit = unit_index(u);
                const uint64_t elapsed_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(points[u + 1] - points[u])
                        .count());
                record_cost_sample(unit, active[u] != 0, elapsed_ns);
                record_activity(unit, active[u] != 0);
            }
        }
    } else {
        for (size_t u = 0; u < num_units; ++u) {
            if (!sample_unit_activity) {
                executeUnitCycle_(units[u], cycle);
                continue;
            }

            const size_t unit = unit_index(u);
            bool expected_active = false;
            bool time_sample = false;
            if (unit < dynamic_runtime_unit_count_) {
                const bool active_due = detail::shouldSampleDynamicTick(
                    cycle, dynamic_unit_last_active_sample_cycle_[unit]);
                const bool inactive_due = detail::shouldSampleDynamicTick(
                    cycle, dynamic_unit_last_inactive_sample_cycle_[unit]);
                if (active_due || inactive_due) {
                    expected_active = units[u]->shouldRunTickAt(cycle);
                    time_sample = expected_active ? active_due : inactive_due;
                }
            }
            SchedulerTimelineTrace::TimePoint begin{};
            if (time_sample) begin = SchedulerTimelineTrace::Clock::now();
            const bool active = executeUnitCycle_(units[u], cycle);
            if (time_sample && active == expected_active) {
                const auto end = SchedulerTimelineTrace::Clock::now();
                const uint64_t elapsed_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
                record_cost_sample(unit, active, elapsed_ns);
            }
            record_activity(unit, active);
        }
    }
}

}  // namespace chronon::sender
