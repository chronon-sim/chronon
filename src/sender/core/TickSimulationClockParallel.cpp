// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <exception>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulationClockRuntime.hpp"

namespace chronon::sender {

void TickSimulation::selectClockExecutionMode_() {
    const auto fallback = [&](std::string reason) {
        parallel_fallback_reason_ = std::move(reason);
        optimizeAllQueuesForSingleThread();
    };
    if (!config_.enable_parallel) return fallback("enable_parallel=false");
    if (config_.num_threads < 2 || unit_ptrs_.size() < 2)
        return fallback("fewer than two clock workers or units");
    if (!config_.enable_lookahead) return fallback("enable_lookahead=false");
    if (!config_.enable_epoch_free_lookahead) return fallback("enable_epoch_free_lookahead=false");
    if (!config_.max_lookahead_cycles) return fallback("max_lookahead_cycles=0");
    unit_costs_ = has_precomputed_costs_ && precomputed_unit_costs_.size() == unit_ptrs_.size()
                      ? precomputed_unit_costs_
                      : std::vector<double>(unit_ptrs_.size(), 1.0);
    const size_t workers = std::min(config_.num_threads, unit_ptrs_.size() + cdc_.size());
    applyClusteredThreadAssignment_(workers, 0.0);
    if (clusters_.numClusters() < 2) return fallback("only one clock cluster");
    // Keep members in the canonical same-domain zero-delay topological order.
    for (auto& cluster : clusters_.clusters) std::sort(cluster.begin(), cluster.end());
    parallel_beneficial_ = true;
    execution_mode_ = ExecutionMode::EpochFree;
}

void TickSimulation::initializeClockParallel_() {
    termination_ctrl_.setOrderedClocks(true);
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) unit_ptrs_[i]->termination_order_ = i;
    clock_parallel_ = std::make_shared<ClockParallelRuntime>();
    auto& runtime = *clock_parallel_;
    runtime.clusters = std::make_unique<ClockParallelRuntime::Cluster[]>(clusters_.numClusters());
    runtime.worker_bridges.resize(thread_units_.size());
    std::vector<size_t> load;
    for (const auto& units : thread_units_) load.push_back(units.size());
    std::unordered_map<Unit*, size_t> cluster_of;
    for (size_t c = 0; c < clusters_.numClusters(); ++c) {
        auto& state = runtime.clusters[c];
        state.clock = &cluster_unit_ptrs_[c].front()->clockDomain();
        for (auto* unit : cluster_unit_ptrs_[c]) {
            if (unit->clockDomainId() != state.clock->id())
                throw std::logic_error("clock cluster spans hardware domains");
            cluster_of.emplace(unit, c);
        }
    }
    for (auto& fifo : cdc_) {
        auto bridge = std::make_unique<ClockParallelRuntime::Bridge>();
        bridge->circuit = fifo.get();
        const std::array<Unit*, 2> owners{fifo->writeOwner(), fifo->readOwner()};
        if (clock_trace_ && clock_trace_->enabled()) {
            // Stable logical producers survive host placement changes. Units
            // record evaluate events; only this bridge writes its commit streams.
            const auto producer = uint64_t{fifo->id()} + 1;
            bridge->trace = {
                clock_trace_->addProducerStream(owners[0]->clockTraceStream(), producer),
                clock_trace_->addProducerStream(owners[1]->clockTraceStream(), producer)};
            fifo->setClockTraceStreams(bridge->trace[0], bridge->trace[1]);
        }
        for (size_t side = 0; side < owners.size(); ++side) {
            auto& endpoint = bridge->endpoints[side];
            endpoint.cluster = cluster_of.at(owners[side]);
            endpoint.clock = &owners[side]->clockDomain();
            runtime.clusters[endpoint.cluster].bridges.push_back(&endpoint.prepared);
        }
        const size_t worker = std::min_element(load.begin(), load.end()) - load.begin();
        ++load[worker];
        runtime.worker_bridges[worker].push_back(runtime.bridges.size());
        runtime.bridges.push_back(std::move(bridge));
    }
    if (config_.enable_dynamic_rebalance) initializeClockMigration_();
}

uint64_t TickSimulation::runClockEpochFree_(uint64_t max_batches, std::optional<SimTime> limit,
                                            bool inclusive) {
    if (!max_batches || clock_calendar_->empty() || wasTerminationRequested()) return 0;
    const auto within_limit = [&](SimTime time) {
        return !limit || time < *limit || (inclusive && time == *limit);
    };
    if (!within_limit(clock_calendar_->nextTime())) return 0;
    ++epoch_free_run_count_;
    auto& runtime = *clock_parallel_;
    auto calendar = *clock_calendar_;
    uint64_t scheduled = 0, completed = 0;
    std::atomic<bool> done{false}, failed{false};
    std::exception_ptr error;
    std::atomic_flag error_set = ATOMIC_FLAG_INIT;
    const auto token = stop_source_->get_token();
    auto* trace = clock_trace_ && clock_trace_->parallelActive() ? clock_trace_.get() : nullptr;
    const bool dynamic = config_.enable_dynamic_rebalance;
    if (dynamic) {
        epoch_free_dynamic_runtime_active_.store(true, std::memory_order_release);
        resetDynamicSchedulerMarkers_();
    }

    // Only worker zero manipulates the calendar. It grants a rolling bounded
    // window, never waits for a whole window, and retires individual completed
    // physical instants. Workers gate on local dependencies inside that window.
    const auto coordinate = [&](bool settling) {
        bool progress = false;
        while (!runtime.pending.empty()) {
            const auto& batch = runtime.pending.front();
            bool ready = true;
            for (const auto& edge : batch.edges) {
                for (size_t c = 0; c < clusters_.numClusters(); ++c) {
                    if (runtime.clusters[c].clock->id() == edge.domain->id() &&
                        thread_progress_array_[c].completed_cycle.load(std::memory_order_acquire) <=
                            edge.cycle)
                        ready = false;
                }
                for (const auto& bridge : runtime.bridges)
                    for (const auto& endpoint : bridge->endpoints)
                        if (endpoint.clock->id() == edge.domain->id() &&
                            endpoint.completed.load(std::memory_order_acquire) <= edge.cycle)
                            ready = false;
            }
            if (!ready) break;
            if (current_cycle_ == UINT64_MAX)
                throw std::overflow_error("scheduler progress overflow");
            (void)clock_calendar_->pop();
            for (const auto& edge : batch.edges)
                clock_runtime_.at(edge.domain->id()).next_cycle = edge.cycle + 1;
            clock_time_ = batch.time;
            ++current_cycle_;
            ++completed;
            runtime.pending.pop_front();
            progress = true;
        }
        if (dynamic && progress)
            runtime.rebalance_cycle.store(clockRebalanceCycle_(clock_time_),
                                          std::memory_order_release);
        // Retirement observes both unit and bridge completion with acquire.
        // Publish without waiting: actors sharing this worker must keep running
        // while the backend drains its bounded observation window.
        if (trace) trace->publishClockProgress(clock_calendar_->nextTime().floorNanoseconds());
        if (!settling) {
            while (runtime.pending.size() < config_.max_lookahead_cycles &&
                   scheduled < max_batches && !calendar.empty() &&
                   within_limit(calendar.nextTime()) && !token.stop_requested()) {
                if (trace && !trace->tryAdmitClockBatch(calendar.nextTime())) break;
                if (scheduled >= UINT64_MAX - (current_cycle_ - completed))
                    throw std::overflow_error("scheduler progress overflow");
                const auto edges = calendar.pop();  // Validates representable successor edges.
                runtime.pending.push_back({edges.front().time, {edges.begin(), edges.end()}});
                for (const auto& edge : edges) {
                    for (size_t c = 0; c < clusters_.numClusters(); ++c)
                        if (runtime.clusters[c].clock->id() == edge.domain->id())
                            runtime.clusters[c].allowed.store(edge.cycle + 1,
                                                              std::memory_order_release);
                }
                ++scheduled;
                progress = true;
            }
        }
        if (runtime.pending.empty() &&
            (settling || scheduled == max_batches || calendar.empty() ||
             !within_limit(calendar.nextTime()) || token.stop_requested()))
            done.store(true, std::memory_order_release);
        return progress;
    };

    const auto bridge_step = [&](size_t index) {
        auto& bridge = *runtime.bridges[index];
        const size_t actor = clusters_.numClusters() + index;
        const uint64_t cycle = dynamic ? bridge.completed.load(std::memory_order_relaxed) : 0;
        if (bridge.edge_count) {
            for (size_t side = 0; side < 2; ++side) {
                const auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side] &&
                    thread_progress_array_[endpoint.cluster].completed_cycle.load(
                        std::memory_order_acquire) <= endpoint.next)
                    return false;
            }
            SchedulerTimelineTrace::TimePoint begin{};
            if (bridge.sample) begin = SchedulerTimelineTrace::Clock::now();
            bridge.circuit->commit();
            for (size_t side = 0; side < 2; ++side) {
                auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side]) {
                    if (trace && bridge.trace[side]) bridge.trace[side]->endEdge();
                    ++endpoint.next;
                    endpoint.completed.store(endpoint.next, std::memory_order_release);
                }
            }
            if (bridge.sample) {
                bridge.sample_ns +=
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              SchedulerTimelineTrace::Clock::now() - begin)
                                              .count());
                cluster_sample_time_ns_[actor].fetch_add(bridge.sample_ns,
                                                         std::memory_order_relaxed);
                cluster_sample_count_[actor].fetch_add(bridge.edge_count,
                                                       std::memory_order_relaxed);
                cluster_active_sample_count_[actor].fetch_add(1, std::memory_order_relaxed);
            }
            bridge.edge_count = 0;
            if (dynamic) bridge.completed.store(cycle + 1, std::memory_order_release);
            return true;
        }
        if (dynamic && dynamicMigrationBlocksCluster_(actor, cycle)) return false;
        const auto write_time = bridge.endpoints[0].clock->edge(bridge.endpoints[0].next);
        const auto read_time = bridge.endpoints[1].clock->edge(bridge.endpoints[1].next);
        const auto time = std::min(write_time, read_time);
        bridge.participating = {write_time == time, read_time == time};
        for (size_t side = 0; side < 2; ++side) {
            const auto& endpoint = bridge.endpoints[side];
            if (bridge.participating[side] && runtime.clusters[endpoint.cluster].allowed.load(
                                                  std::memory_order_acquire) <= endpoint.next)
                return false;
        }
        for (size_t side = 0; side < 2; ++side) {
            const auto& endpoint = bridge.endpoints[side];
            if (bridge.participating[side])
                bridge.edges[bridge.edge_count++] = {endpoint.clock, endpoint.next, time};
        }
        bridge.sample = dynamic && detail::shouldSampleDynamicTick(
                                       cycle, dynamic_cluster_last_tick_sample_cycle_[actor]);
        SchedulerTimelineTrace::TimePoint begin{};
        if (bridge.sample) begin = SchedulerTimelineTrace::Clock::now();
        bridge.circuit->begin(std::span(bridge.edges.data(), bridge.edge_count));
        if (bridge.sample) {
            dynamic_cluster_last_tick_sample_cycle_[actor] = cycle;
            bridge.sample_ns =
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          SchedulerTimelineTrace::Clock::now() - begin)
                                          .count());
        }
        for (size_t side = 0; side < 2; ++side) {
            auto& endpoint = bridge.endpoints[side];
            if (bridge.participating[side])
                endpoint.prepared.store(endpoint.next + 1, std::memory_order_release);
        }
        return true;
    };

    const auto execute = [&](bool settling) {
        auto work =
            stdexec::bulk(stdexec::just(), stdexec::par, thread_units_.size(), [&](size_t worker) {
                try {
                    WorkerPredecessorCycleCache cache(thread_progress_count_);
                    auto owned_clusters = thread_clusters_[worker];
                    auto owned_bridges = runtime.worker_bridges[worker];
                    std::vector<size_t> owned_actors, ownership_scratch;
                    uint64_t seen_generation = 0;
                    const auto refresh = [&] {
                        refreshDynamicOwnedActors_(worker, owned_actors, ownership_scratch,
                                                   seen_generation);
                        owned_clusters.clear();
                        owned_bridges.clear();
                        for (const size_t actor : owned_actors) {
                            if (actor < clusters_.numClusters())
                                owned_clusters.push_back(actor);
                            else
                                owned_bridges.push_back(actor - clusters_.numClusters());
                        }
                    };
                    while (!failed.load(std::memory_order_acquire) &&
                           !done.load(std::memory_order_acquire) &&
                           (settling || !token.stop_requested())) {
                        bool progress = worker == 0 && coordinate(settling);
                        if (dynamic && seen_generation != cluster_assignment_generation_.load(
                                                              std::memory_order_acquire))
                            refresh();
                        for (const auto index : owned_bridges) {
                            if (dynamic &&
                                cluster_runtime_owner_[clusters_.numClusters() + index].load(
                                    std::memory_order_acquire) != worker)
                                continue;
                            progress = bridge_step(index) || progress;
                        }
                        for (const auto c : owned_clusters) {
                            if (dynamic &&
                                cluster_runtime_owner_[c].load(std::memory_order_acquire) != worker)
                                continue;
                            auto& state = runtime.clusters[c];
                            auto& published = thread_progress_array_[c].completed_cycle;
                            const auto cycle = published.load(std::memory_order_relaxed);
                            if (dynamic && dynamicMigrationBlocksCluster_(c, cycle)) continue;
                            if (cycle >= state.allowed.load(std::memory_order_acquire)) continue;
                            bool ready = true;
                            for (const auto* bridge : state.bridges)
                                if (bridge->load(std::memory_order_acquire) <= cycle) ready = false;
                            BlockedClusterInfo blocker;
                            if (!ready || !clusterCanAdvance_(c, cycle, blocker, cache.data()))
                                continue;
                            executeClusterOneCycle_(worker, c, cycle, false, dynamic);
                            published.store(cycle + 1, std::memory_order_release);
                            progress = true;
                        }
                        if (dynamic && !settling) {
                            if (worker == 0)
                                maybeRequestEpochFreeMigration_(dynamicMigrationCycle_());
                            // Only the source can publish an owner change, after
                            // its entire sweep; targets acquire actor-private
                            // circuit, unit sampling and SPSC producer state.
                            serviceEpochFreeMigration_(worker);
                        }
                        if (!progress) cpuPause();
                    }
                } catch (...) {
                    if (!error_set.test_and_set(std::memory_order_relaxed))
                        error = std::current_exception();
                    failed.store(true, std::memory_order_release);
                    stop_source_->request_stop();
                }
            });
        stdexec::sync_wait(stdexec::starts_on(pool_.get_scheduler(), std::move(work)));
    };

    try {
        execute(false);
        finishClockMigrationRun_();
        if (error) std::rethrow_exception(error);
        if (!runtime.pending.empty()) {
            // Termination freezes admission. After joining the workers, settle
            // exactly through the latest edge already begun (including bridge
            // snapshots), not the unused remainder of the lookahead window.
            auto boundary = clock_time_;
            for (size_t c = 0; c < clusters_.numClusters(); ++c) {
                const auto& cluster = runtime.clusters[c];
                // Normal stop never interrupts a tick/cluster: by this join,
                // every started unit edge has also published completion.
                const auto started =
                    thread_progress_array_[c].completed_cycle.load(std::memory_order_relaxed);
                if (started) boundary = std::max(boundary, cluster.clock->edge(started - 1));
            }
            for (const auto& bridge : runtime.bridges)
                for (const auto& endpoint : bridge->endpoints) {
                    const auto prepared = endpoint.prepared.load(std::memory_order_relaxed);
                    if (prepared) boundary = std::max(boundary, endpoint.clock->edge(prepared - 1));
                }
            while (!runtime.pending.empty() && runtime.pending.back().time > boundary)
                runtime.pending.pop_back();
            for (size_t c = 0; c < clusters_.numClusters(); ++c) {
                auto& cluster = runtime.clusters[c];
                auto target = clock_runtime_.at(cluster.clock->id()).next_cycle;
                for (const auto& batch : runtime.pending)
                    for (const auto& edge : batch.edges)
                        if (edge.domain->id() == cluster.clock->id()) target = edge.cycle + 1;
                cluster.allowed.store(target, std::memory_order_relaxed);
            }
            done.store(runtime.pending.empty(), std::memory_order_relaxed);
            execute(true);
            if (error) std::rethrow_exception(error);
        }
    } catch (...) {
        epoch_free_dynamic_runtime_active_.store(false, std::memory_order_release);
        finishClockMigrationRun_();
        for (auto* unit : unit_ptrs_) unit->clock_edge_executing_ = false;
        clock_failed_ = true;
        throw;
    }
    epoch_free_dynamic_runtime_active_.store(false, std::memory_order_release);
    if (dynamic) flushDynamicSchedulerMarkers_();
    for (size_t i = 0; i < unit_ptrs_.size(); ++i)
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_release);
    termination_ctrl_.setSettledTime(clock_time_);
    return completed;
}

}  // namespace chronon::sender
