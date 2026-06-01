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

#include <chrono>
#include <cstdlib>
#include <new>
#include <string>
#include <unordered_map>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"

namespace chronon::sender {

namespace {
/// Spin iterations between lazy lookahead-floor refreshes in the slow-path
/// spin-wait (bitmask; refresh fires when (spin & mask) == 0).  256 keeps the
/// O(num_clusters) cross-core scan off the hot interconnect path while still
/// lifting the max_lookahead ceiling far sooner than the slowest cluster could
/// stall on it.
constexpr uint64_t kFloorRefreshSpinMask = 0xFF;
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

            executeClusterOneCycle_(thread_idx, cluster, cycle, trace_units);
            progress.store(cycle + 1, std::memory_order_release);
            // The global lookahead floor is NOT recomputed per advance here.
            // It only gates the synthetic max_lookahead ceiling dep, and a
            // stale floor merely blocks a cluster marginally sooner (never a
            // correctness issue — real data sync rides on the per-edge
            // completed_cycle release/acquire above).  Refreshing it on every
            // advance would be an O(num_clusters) all-to-all scan of atomics
            // written by other cores on the hot path.  It is instead refreshed
            // only when some cluster is actually blocked (below and in the
            // spin-wait), which is the only time a stale floor delays progress.
            made_progress = true;
        }

        if (all_done || token.stop_requested()) return;
        if (made_progress) {
            // A worker can own a cluster blocked only by the synthetic
            // lookahead ceiling alongside another cluster that still advances.
            // The advancing cluster sets made_progress, so this worker never
            // reaches the spin-wait that refreshes the floor; without a refresh
            // here the blocked cluster would stay pinned to the epoch-start
            // ceiling for the whole epoch, serializing it instead of keeping
            // the configured max_lookahead lead.  blocker.cluster is set iff
            // some cluster failed clusterCanAdvance_ this pass.  This runs at
            // most once per executed cycle (the loop is gated by real work, not
            // a tight spin), so unlike the spin-wait it needs no throttle.
            if (blocker.cluster != SIZE_MAX) {
                refreshLookaheadFloor_();
            }
            continue;
        }

        SchedulerTimelineTrace::TimePoint wait_begin{};
        if (trace_waits) {
            wait_begin = SchedulerTimelineTrace::Clock::now();
        }

        // Refresh the global lookahead floor every kFloorRefreshSpinMask+1 spin
        // iterations (and on the first, so a stale ceiling lifts promptly).  The
        // O(num_clusters) cross-core scan is throttled here rather than run on
        // every cpuPause so it does not re-introduce the coherence traffic it
        // replaces — the mask is large enough to keep that traffic low, yet far
        // smaller than the cycles the slowest cluster takes to advance, so the
        // ceiling lifts well before it would actually stall forward progress.
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
            timeline_trace_.recordDuration(
                thread_idx, "wait", "cluster dependency",
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
    // Recompute the global minimum completed cycle (GVT) and monotonically
    // raise lookahead_floor_ to it.  Called only on the slow path (a stalled
    // thread's spin-wait), never per cycle advance.  Loads are relaxed — the
    // floor is a monotone gating hint that carries no data; see the
    // memory-order contract on its declaration in TickSimulation.hpp.
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
        points[0] = SchedulerTimelineTrace::Clock::now();
        for (size_t u = 0; u < num_units; ++u) {
            const bool sample_tick = config_.enable_dynamic_rebalance && (cycle & 1023u) == 0;
            units[u]->executeTick();
            points[u + 1] = SchedulerTimelineTrace::Clock::now();

            if (__builtin_expect(sample_tick, 0)) {
                recordTickSample_(
                    thread_idx, cluster_thread_unit_positions_[cluster][u],
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              points[u + 1] - points[u])
                                              .count()));
            }
        }
        for (size_t u = 0; u < num_units; ++u) {
            timeline_trace_.recordDuration(thread_idx, "unit", units[u]->name(), cycle, points[u],
                                           points[u + 1]);
        }
    } else if (__builtin_expect(config_.enable_dynamic_rebalance && (cycle & 1023u) == 0, 0)) {
        for (size_t u = 0; u < num_units; ++u) {
            auto tp0 = std::chrono::steady_clock::now();
            units[u]->executeTick();
            auto tp1 = std::chrono::steady_clock::now();
            uint64_t elapsed_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(tp1 - tp0).count());
            recordTickSample_(thread_idx, cluster_thread_unit_positions_[cluster][u], elapsed_ns);
        }
    } else {
        for (size_t u = 0; u < num_units; ++u) {
            units[u]->executeTick();
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
