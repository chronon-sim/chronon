// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// TickSimulation parallel runtime: progress-based epoch execution,
/// cross-thread dependency spin-waits, MPSC arbitration helpers,
/// and progress-sync initialization.

#include <barrier>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <limits>
#include <new>
#include <string>
#include <unordered_map>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"
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

// ---------------------------------------------------------------------------
// MPSC producer progress wiring
// ---------------------------------------------------------------------------

void TickSimulation::installMPSCProducerProgress_() {
    if (!thread_progress_array_ || thread_progress_count_ == 0) return;

    std::unordered_map<Unit*, size_t> unit_to_cluster;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        if (i < unit_to_cluster_.size()) {
            unit_to_cluster[static_cast<Unit*>(unit_ptrs_[i])] = unit_to_cluster_[i];
        }
    }

    std::unordered_map<void*, std::vector<size_t>> port_to_src_clusters;
    for (auto* conn : connections_) {
        if (!conn->hasThreadQueueId()) continue;
        Unit* src = conn->source();
        if (!src) continue;
        auto src_it = unit_to_cluster.find(src);
        if (src_it == unit_to_cluster.end()) continue;
        void* port_key = conn->destPortPtr();
        if (!port_key) continue;
        auto& vec = port_to_src_clusters[port_key];
        if (std::find(vec.begin(), vec.end(), src_it->second) == vec.end()) {
            vec.push_back(src_it->second);
        }
    }

    // Per-connection producer progress: each producer Unit -> its cluster's
    // completed_cycle atomic. The InPort uses this to drain each MPSC
    // connection up to its OWN producer's progress (heterogeneous-delay
    // correctness), rather than the min across producers.
    std::unordered_map<Unit*, const std::atomic<uint64_t>*> src_to_progress;
    src_to_progress.reserve(unit_to_cluster.size());
    for (const auto& [unit, cluster] : unit_to_cluster) {
        if (cluster < thread_progress_count_) {
            src_to_progress[unit] = &thread_progress_array_[cluster].completed_cycle;
        }
    }

    for (IArbitratablePort* arb : mpsc_inports_) {
        void* key = arb->arbitratablePortKey();
        auto it = port_to_src_clusters.find(key);
        if (it == port_to_src_clusters.end()) continue;
        std::vector<const std::atomic<uint64_t>*> ptrs;
        ptrs.reserve(it->second.size());
        for (size_t c : it->second) {
            if (c < thread_progress_count_) {
                ptrs.push_back(&thread_progress_array_[c].completed_cycle);
            }
        }
        arb->setArbitrationProgressPointers(std::move(ptrs));
        arb->setArbitrationConnProgress(src_to_progress);
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
        // Skip when max_lookahead_cycles == 0 (no limit — epoch boundary is the only cap).
        if (config_.max_lookahead_cycles > 0) {
            thread_resolved_deps_[c].push_back(
                {&lookahead_floor_, config_.max_lookahead_cycles, /*pred_id=*/SIZE_MAX});
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
    cluster_thread_unit_positions_.clear();
    cluster_thread_unit_positions_.resize(num_clusters);

    std::vector<std::unordered_map<size_t, size_t>> thread_unit_pos(num_threads);
    for (size_t t = 0; t < num_threads; ++t) {
        for (size_t pos = 0; pos < thread_units_[t].size(); ++pos) {
            thread_unit_pos[t][thread_units_[t][pos]] = pos;
        }
    }

    for (size_t c = 0; c < num_clusters; ++c) {
        size_t thread = c < cluster_to_thread_.size() ? cluster_to_thread_[c] : SIZE_MAX;
        for (size_t unit_idx : clusters_.clusters[c]) {
            cluster_unit_ptrs_[c].push_back(unit_ptrs_[unit_idx]);
            size_t pos = 0;
            if (thread < thread_unit_pos.size()) {
                auto it = thread_unit_pos[thread].find(unit_idx);
                if (it != thread_unit_pos[thread].end()) {
                    pos = it->second;
                }
            }
            cluster_thread_unit_positions_[c].push_back(pos);
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

void TickSimulation::initTimelineTraceScratch_() {
    thread_trace_points_.clear();
    if (!timeline_trace_.traceUnits() || thread_units_.empty()) return;

    thread_trace_points_.resize(thread_units_.size());
    for (size_t t = 0; t < thread_units_.size(); ++t) {
        thread_trace_points_[t].resize(thread_units_[t].size() + 1);
    }
}

// ---------------------------------------------------------------------------
// Progress-based parallel execution
// ---------------------------------------------------------------------------

void TickSimulation::executeEpochProgressBased(uint64_t epoch_cycles) {
    uint64_t start_cycle =
        thread_progress_array_[0].completed_cycle.load(std::memory_order_relaxed);
    uint64_t end_cycle = start_cycle + epoch_cycles;
    lookahead_floor_.store(start_cycle, std::memory_order_relaxed);
    const bool trace_epoch = timeline_trace_.traceEpochs();
    SchedulerTimelineTrace::TimePoint epoch_begin{};
    if (trace_epoch) {
        epoch_begin = SchedulerTimelineTrace::Clock::now();
    }

    auto token = stop_source_->get_token();

    auto sched = pool_.get_scheduler();
    auto work = stdexec::bulk(stdexec::just(), stdexec::par, thread_units_.size(),
                              [this, end_cycle, token](std::size_t thread_idx) {
                                  // The try-catch is NOT for exception
                                  // capture (stdexec handles that natively);
                                  // it solely requests stop so other threads
                                  // exit their dependency spin-waits.
                                  try {
                                      executeThreadEpoch_(thread_idx, end_cycle, token);
                                  } catch (...) {
                                      stop_source_->request_stop();
                                      throw;
                                  }
                              });

    auto scheduled_work = stdexec::starts_on(sched, std::move(work));
    stdexec::sync_wait(std::move(scheduled_work));

    // Epoch-end flush (R8 in docs/mpsc-atomic-publish.md). Every thread
    // has published end_cycle-1 just before exiting, so the final cycle's
    // staging entries may not have been arbitrated. Run the main-thread
    // arbiter once unbounded so tail entries are committed.
    if (timeline_trace_.traceArbitration()) {
        auto arb_begin = SchedulerTimelineTrace::Clock::now();
        arbitrateAllMPSCPorts_();
        auto arb_end = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler",
                                       "epoch-end mpsc arbitration", end_cycle, arb_begin, arb_end);
    } else {
        arbitrateAllMPSCPorts_();
    }

    for (size_t i = 0; i < units_.size(); ++i) {
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_release);
    }

    if (trace_epoch) {
        auto epoch_end = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler",
                                       "progress epoch", start_cycle, epoch_begin, epoch_end,
                                       "cycles=" + std::to_string(epoch_cycles));
    }
}

uint64_t TickSimulation::executeRunProgressBased(uint64_t total_cycles) {
    const size_t nthreads = thread_units_.size();
    if (nthreads == 0 || total_cycles == 0) return 0;

    auto token = stop_source_->get_token();
    auto sched = pool_.get_scheduler();

    const uint64_t run_start =
        thread_progress_array_[0].completed_cycle.load(std::memory_order_relaxed);
    const uint64_t run_target = saturatingCycleAdd(run_start, total_cycles);

    // Shared with the barrier completion. Written only in the completion (one
    // thread, peers parked) or before launch; the barrier supplies the
    // happens-before, so no atomics are needed.
    uint64_t epoch_end = run_start + std::min<uint64_t>(config_.epoch_size, total_cycles);
    bool run_done = false;
    std::exception_ptr captured;
    std::atomic_flag captured_set = ATOMIC_FLAG_INIT;

    lookahead_floor_.store(run_start, std::memory_order_relaxed);

    // Serial epoch tail, run once when all workers have arrived. Must be nothrow
    // (std::barrier completion contract). The MPSC flush is ordered by the queue
    // atomics, not the barrier; progress is published per-worker below, so this
    // never reads a peer's plain localCycle.
    auto on_epoch_complete = [&]() noexcept {
        arbitrateAllMPSCPorts_();
        if (epoch_end >= run_target || token.stop_requested()) {
            run_done = true;
            return;
        }
        const uint64_t next_cycles = std::min<uint64_t>(config_.epoch_size, run_target - epoch_end);
        lookahead_floor_.store(epoch_end, std::memory_order_relaxed);
        epoch_end += next_cycles;
    };

    std::barrier sync_point(static_cast<std::ptrdiff_t>(nthreads), on_epoch_complete);

    auto work =
        stdexec::bulk(stdexec::just(), stdexec::par, nthreads, [&, token](std::size_t thread_idx) {
            const std::vector<size_t>& my_units = thread_units_[thread_idx];
            for (;;) {
                const uint64_t end = epoch_end;  // published by init / prior barrier
                try {
                    executeThreadEpoch_(thread_idx, end, token);
                } catch (...) {
                    // Capture once and request stop, but still fall through to the
                    // barrier so no peer deadlocks waiting on this worker.
                    if (!captured_set.test_and_set(std::memory_order_relaxed)) {
                        captured = std::current_exception();
                    }
                    stop_source_->request_stop();
                }
                // Publish only this thread's own units (release): each localCycle is
                // read by the thread that wrote it, so there is no cross-thread plain
                // read in the completion.
                for (size_t unit_idx : my_units) {
                    unit_progress_[unit_idx].store(unit_ptrs_[unit_idx]->localCycle(),
                                                   std::memory_order_release);
                }
                sync_point.arrive_and_wait();  // -> on_epoch_complete (once)
                if (run_done) break;
            }
        });

    stdexec::sync_wait(stdexec::starts_on(sched, std::move(work)));

    if (captured) std::rethrow_exception(captured);

    return completedCyclesForRun_(run_start, run_target);
}

bool TickSimulation::allMPSCPortsHaveConnProgress_() const noexcept {
    for (const IArbitratablePort* p : mpsc_inports_) {
        if (!p->mpscConnProgressFullyResolved()) return false;
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
    for (const ConnectionBase* c : connections_) {
        if (c->crossThreadHeadroom() <= 1) return false;
    }
    return true;
}

size_t TickSimulation::demoteUnsafeEpochFreeQueues_() {
    if (!config_.enable_epoch_free_lookahead) return 0;
    std::unordered_map<void*, std::vector<ConnectionBase*>> by_port;
    for (auto* conn : connections_) {
        by_port[conn->destPortPtr()].push_back(conn);
    }

    size_t demoted = 0;
    for (auto& [port, conns] : by_port) {
        (void)port;
        bool unsafe = false;
        for (auto* conn : conns) {
            if (conn->crossThreadHeadroom() <= 1 &&
                !conn->ensureEpochFreeHeadroom(config_.max_lookahead_cycles)) {
                unsafe = true;
                break;
            }
        }
        if (!unsafe) continue;
        for (auto* conn : conns) {
            conn->optimizeForThreadSafe();
        }
        demoted += conns.size();
    }
    return demoted;
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
                executeThreadEpoch_(thread_idx, run_target, token);
            } catch (...) {
                if (!captured_set.test_and_set(std::memory_order_relaxed)) {
                    captured = std::current_exception();
                }
                stop_source_->request_stop();
            }
        });

    stdexec::sync_wait(stdexec::starts_on(sched, std::move(work)));

    if (captured) std::rethrow_exception(captured);

    // Single final MPSC flush. Every worker has published its final progress and
    // exited, so the consumer-driven drains (which admit up to
    // producer_completed-1) may leave the run's last staged entries
    // unarbitrated; commit them once here. Mirrors the epoch-end flush in
    // executeEpochProgressBased, but runs once per run instead of per epoch.
    arbitrateAllMPSCPorts_();

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
// Per-thread epoch driver
// ---------------------------------------------------------------------------

void TickSimulation::executeThreadEpoch_(size_t thread_idx, uint64_t end_cycle,
                                         stdexec::inplace_stop_token token) {
    const auto& clusters = thread_clusters_[thread_idx];
    const bool trace_units = timeline_trace_.traceUnits();
    const bool trace_waits = timeline_trace_.traceWaits();

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
            if (!clusterCanAdvance_(cluster, cycle, candidate)) {
                if (candidate.deficit > blocker.deficit) {
                    blocker = candidate;
                }
                continue;
            }

            uint64_t idle_target = computeIdleClusterTarget_(cluster, cycle, end_cycle);
            if (idle_target > cycle) {
                const bool cluster_has_mpsc =
                    clusterHasMPSCConnections(cluster_unit_ptrs_[cluster]);
                if (cluster_has_mpsc) {
                    // MPSC-owning idle units drain staging queues during
                    // advanceIdleTick(), so their idle advance has observable
                    // queue/back-pressure side effects and cannot be safely
                    // rolled back after a refreshed wake. Keep those clusters
                    // interruptible by advancing one idle cycle at a time; any
                    // future explicit wakeAt(A) observed during this step is
                    // picked up by the next scheduler iteration before the
                    // cluster can publish progress past A.
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

        if (all_done || token.stop_requested()) return;
        if (made_progress) {
            // A worker advancing one cluster while another is stuck at the
            // ceiling never reaches the spin-wait; refresh here so the blocked
            // cluster isn't pinned to the epoch-start ceiling all epoch.
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
                if (clusterCanAdvance_(cluster, cycle, ignored)) {
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

bool TickSimulation::clusterCanAdvance_(size_t cluster, uint64_t cycle,
                                        BlockedClusterInfo& blocker) const {
    if (cluster >= thread_resolved_deps_.size()) return true;
    bool ready = true;
    for (const auto& dep : thread_resolved_deps_[cluster]) {
        uint64_t needed = (cycle + 1 > dep.min_delay) ? cycle + 1 - dep.min_delay : 0;
        uint64_t observed = dep.progress_ptr->load(std::memory_order_acquire);
        if (observed >= needed) continue;
        ready = false;
        uint64_t deficit = needed - observed;
        if (deficit > blocker.deficit) {
            blocker.cluster = cluster;
            blocker.pred_cluster = dep.pred_id;
            blocker.needed = needed;
            blocker.observed = observed;
            blocker.delay = dep.min_delay;
            blocker.deficit = deficit;
        }
    }
    return ready;
}

uint64_t TickSimulation::computeIdleClusterTarget_(size_t cluster, uint64_t cycle,
                                                   uint64_t end_cycle) const {
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
            uint64_t observed = dep.progress_ptr->load(std::memory_order_acquire);
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
                                             bool trace_units) {
    auto* const* units = cluster_unit_ptrs_[cluster].data();
    const size_t num_units = cluster_unit_ptrs_[cluster].size();

    if (trace_units) {
        auto& points = thread_trace_points_[thread_idx];
        if (points.size() < num_units + 1) {
            points.resize(num_units + 1);
        }
        std::vector<char> active(num_units, 0);
        points[0] = SchedulerTimelineTrace::Clock::now();
        for (size_t u = 0; u < num_units; ++u) {
            const bool sample_tick =
                config_.enable_dynamic_rebalance &&
                !epoch_free_dynamic_runtime_active_.load(std::memory_order_relaxed) &&
                (cycle & 1023u) == 0;
            active[u] = executeUnitCycle_(units[u], cycle);
            points[u + 1] = SchedulerTimelineTrace::Clock::now();

            if (__builtin_expect(sample_tick, 0)) {
                uint64_t elapsed_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(points[u + 1] - points[u])
                        .count());
                recordTickSample_(thread_idx, cluster_thread_unit_positions_[cluster][u],
                                  elapsed_ns, active[u]);
            }
        }
        for (size_t u = 0; u < num_units; ++u) {
            timeline_trace_.recordDuration(thread_idx, active[u] ? "unit" : "unit idle",
                                           units[u]->name(), cycle, points[u], points[u + 1],
                                           active[u] ? "" : "cycles=1");
        }
    } else if (__builtin_expect(
                   config_.enable_dynamic_rebalance &&
                       !epoch_free_dynamic_runtime_active_.load(std::memory_order_relaxed) &&
                       (cycle & 1023u) == 0,
                   0)) {
        for (size_t u = 0; u < num_units; ++u) {
            auto tp0 = std::chrono::steady_clock::now();
            bool active = executeUnitCycle_(units[u], cycle);
            auto tp1 = std::chrono::steady_clock::now();
            uint64_t elapsed_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(tp1 - tp0).count());
            recordTickSample_(thread_idx, cluster_thread_unit_positions_[cluster][u], elapsed_ns,
                              active);
        }
    } else {
        for (size_t u = 0; u < num_units; ++u) {
            executeUnitCycle_(units[u], cycle);
        }
    }
}

// ---------------------------------------------------------------------------
// MPSC InPort collection
// ---------------------------------------------------------------------------

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
