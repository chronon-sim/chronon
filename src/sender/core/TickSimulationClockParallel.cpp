// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <exception>
#include <thread>

#include "../../chronon/CpuPause.hpp"
#include "DynamicWaitPolicy.hpp"
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
    platform_metrics_ = has_precomputed_costs_ ? precomputed_platform_metrics_ : PlatformMetrics{};
    detail::ClockProfileScope partition_profile(
        config_.profile_clock_scheduler ? &clock_partition_time_ns_ : nullptr);
    applyClusteredThreadAssignment_(workers, has_precomputed_costs_
                                                 ? platform_metrics_.atomic_roundtrip_ns
                                                 : config_.initial_partition_sync_cost_ns);
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
    std::unordered_map<Unit*, size_t> cluster_of;
    for (size_t c = 0; c < clusters_.numClusters(); ++c) {
        auto& state = runtime.clusters[c];
        state.clock = &cluster_unit_ptrs_[c].front()->clockDomain();
        auto& domain = runtime.domains[state.clock->id()];
        domain.clock = state.clock;
        domain.completions.push_back(&thread_progress_array_[c].completed_cycle);
        state.domain = &domain;
        for (auto* unit : cluster_unit_ptrs_[c]) {
            if (unit->clockDomainId() != state.clock->id())
                throw std::logic_error("clock cluster spans hardware domains");
            cluster_of.emplace(unit, c);
        }
    }
    for (const auto& group : clock_bridge_groups_) {
        auto bridge = std::make_unique<ClockParallelRuntime::Bridge>();
        bridge->lanes.reserve(group.size());
        for (const auto f : group) {
            auto& fifo = cdc_[f];
            ClockParallelRuntime::Lane lane;
            lane.circuit = fifo.get();
            const std::array<Unit*, 2> owners{fifo->writeOwner(), fifo->readOwner()};
            if (clock_trace_ && clock_trace_->enabled()) {
                // Scheduling groups do not merge logical producers, lane IDs,
                // ordinals, event budgets or FIFO circuit state.
                const auto producer = uint64_t{fifo->id()} + 1;
                lane.trace[0] =
                    clock_trace_->addProducerStream(owners[0]->clockTraceStream(), producer);
                lane.trace[1] =
                    owners[0] == owners[1]
                        ? lane.trace[0]
                        : clock_trace_->addProducerStream(owners[1]->clockTraceStream(), producer);
                fifo->setClockTraceStreams(lane.trace[0], lane.trace[1]);
            }
            bridge->lanes.push_back(lane);
        }
        const auto* first = bridge->lanes.front().circuit;
        const std::array<Unit*, 2> owners{first->writeOwner(), first->readOwner()};
        for (size_t side = 0; side < owners.size(); ++side) {
            auto& endpoint = bridge->endpoints[side];
            endpoint.cluster = cluster_of.at(owners[side]);
            endpoint.clock = &owners[side]->clockDomain();
            endpoint.next_time = endpoint.clock->edge(0);
            runtime.clusters[endpoint.cluster].bridges.push_back(
                {clusters_.numClusters() + runtime.bridges.size(), &endpoint.prepared});
            runtime.domains.at(endpoint.clock->id()).completions.push_back(&endpoint.completed);
        }
        const size_t worker = clock_bridge_owners_.at(runtime.bridges.size());
        runtime.worker_bridges[worker].push_back(runtime.bridges.size());
        runtime.bridges.push_back(std::move(bridge));
    }
    for (auto& [id, serial] : clock_runtime_) {
        auto& domain = runtime.domains.at(id);
        domain.serial = &serial;
        runtime.indexed_domains.push_back(&domain);
    }
    runtime.pending.resize(config_.max_lookahead_cycles);
    for (auto& batch : runtime.pending) batch.edges.reserve(runtime.indexed_domains.size());
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
    const auto coordinate = [&](bool settling, ClockSchedulerProfile* profile) {
        detail::ClockProfileScope retirement(profile ? &profile->retirement_ns : nullptr);
        bool progress = false;
        if (++runtime.coordinator_sweep == 0) {
            for (auto* domain : runtime.indexed_domains) domain->completion_sweep = 0;
            ++runtime.coordinator_sweep;
        }
        while (runtime.pending_size) {
            const auto& batch = runtime.pendingAt(0);
            bool ready = true;
            for (const auto& edge : batch.edges) {
                auto& domain = *edge.domain;
                if (domain.completion_sweep != runtime.coordinator_sweep) {
                    domain.acquired_completed = UINT64_MAX;
                    for (const auto* completed : domain.completions) {
                        if (profile) ++profile->completion_loads;
                        domain.acquired_completed = std::min(
                            domain.acquired_completed, completed->load(std::memory_order_acquire));
                        // A partial minimum is still conservative if it already
                        // blocks the oldest batch; retry on the next sweep.
                        if (domain.acquired_completed <= edge.cycle) break;
                    }
                    domain.completion_sweep = runtime.coordinator_sweep;
                }
                ready = domain.acquired_completed > edge.cycle;
                if (!ready) break;
            }
            if (!ready) break;
            if (current_cycle_ == UINT64_MAX)
                throw std::overflow_error("scheduler progress overflow");
            (void)clock_calendar_->pop();
            for (const auto& edge : batch.edges) {
                edge.domain->serial->next_cycle = edge.cycle + 1;
                edge.domain->retired.store(edge.cycle + 1, std::memory_order_release);
            }
            clock_time_ = batch.time;
            ++current_cycle_;
            ++completed;
            if (++runtime.pending_head == runtime.pending.size()) runtime.pending_head = 0;
            --runtime.pending_size;
            progress = true;
        }
        if (dynamic && progress)
            runtime.rebalance_cycle.store(clockRebalanceCycle_(clock_time_),
                                          std::memory_order_release);
        // Retirement observes both unit and bridge completion with acquire.
        // Publish without waiting: actors sharing this worker must keep running
        // while the backend drains its bounded observation window.
        if (trace) trace->publishClockProgress(clock_calendar_->nextTime().floorNanoseconds());
        retirement.finish();
        detail::ClockProfileScope admission(profile ? &profile->admission_ns : nullptr);
        if (!settling) {
            while (runtime.pending_size < runtime.pending.size() && scheduled < max_batches &&
                   !calendar.empty() && within_limit(calendar.nextTime()) &&
                   !token.stop_requested()) {
                if (trace && !trace->tryAdmitClockBatch(calendar.nextTime())) break;
                if (scheduled >= UINT64_MAX - (current_cycle_ - completed))
                    throw std::overflow_error("scheduler progress overflow");
                const auto edges = calendar.pop();  // Validates representable successor edges.
                auto& batch = runtime.pendingAt(runtime.pending_size++);
                batch.time = edges.front().time;
                batch.edges.clear();
                for (const auto& edge : edges) {
                    auto* domain = runtime.indexed_domains[edge.calendar_index];
                    batch.edges.push_back({domain, edge.cycle});
                    domain->allowed.store(edge.cycle + 1, std::memory_order_release);
                }
                ++scheduled;
                progress = true;
            }
        }
        if (!runtime.pending_size && (settling || scheduled == max_batches || calendar.empty() ||
                                      !within_limit(calendar.nextTime()) || token.stop_requested()))
            done.store(true, std::memory_order_release);
        return progress;
    };

    const auto bridge_step = [&](size_t index, auto&& blocked, ClockSchedulerProfile* profile) {
        auto& bridge = *runtime.bridges[index];
        const size_t actor = clusters_.numClusters() + index;
        const uint64_t cycle = dynamic ? bridge.completed.load(std::memory_order_relaxed) : 0;
        if (bridge.edge_count) {
            for (size_t side = 0; side < 2; ++side) {
                const auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side] &&
                    thread_progress_array_[endpoint.cluster].completed_cycle.load(
                        std::memory_order_acquire) <= endpoint.next) {
                    blocked(actor, endpoint.cluster, bridge.edges[0].time);
                    return false;
                }
            }
            SchedulerTimelineTrace::TimePoint begin{};
            if (bridge.sample) begin = SchedulerTimelineTrace::Clock::now();
            {
                detail::ClockProfileScope commit_profile(profile ? &profile->bridge_ns : nullptr);
                // Commit EVERY lane before publishing completion or allowing
                // this actor to transfer to another owner at the sweep boundary.
                for (auto& lane : bridge.lanes) lane.circuit->commit();
            }
            if (profile) ++profile->bridge_commits;
            for (size_t side = 0; side < 2; ++side) {
                auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side]) {
                    if (trace)
                        for (auto& lane : bridge.lanes)
                            if (lane.trace[side]) lane.trace[side]->endEdge();
                    ++endpoint.next;
                    endpoint.next_time = endpoint.clock->edge(endpoint.next);
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
        const auto write_time = bridge.endpoints[0].next_time;
        const auto read_time = bridge.endpoints[1].next_time;
        const auto time = std::min(write_time, read_time);
        bridge.participating = {write_time == time, read_time == time};
        for (size_t side = 0; side < 2; ++side) {
            const auto& endpoint = bridge.endpoints[side];
            if (bridge.participating[side] &&
                runtime.clusters[endpoint.cluster].domain->allowed.load(
                    std::memory_order_acquire) <= endpoint.next) {
                blocked(actor, SIZE_MAX, time);
                return false;
            }
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
        {
            detail::ClockProfileScope begin_profile(profile ? &profile->bridge_ns : nullptr);
            // All lanes snapshot old state before either endpoint cluster is
            // released. Coincident edges cannot observe another lane's commit.
            for (auto& lane : bridge.lanes)
                lane.circuit->begin(std::span(bridge.edges.data(), bridge.edge_count));
        }
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
                    uint64_t idle_sweeps = 0;
                    uint64_t wait_sequence = 0;
                    uint64_t profile_sequence = worker;
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
                        auto* profile =
                            config_.profile_clock_scheduler && (profile_sequence++ & 63) == 0
                                ? &clock_scheduler_profile_[worker]
                                : nullptr;
                        if (profile) ++profile->sweeps;
                        bool progress = worker == 0 && coordinate(settling, profile);
                        detail::ClockProfileScope actors_profile(profile ? &profile->actor_ns
                                                                         : nullptr);
                        const bool sample_wait =
                            dynamic && !settling && (wait_sequence++ & 63) == 0;
                        BlockedClusterInfo wait_blocker;
                        SimTime wait_time;
                        const auto blocked = [&](size_t actor, size_t predecessor, SimTime time) {
                            if (sample_wait &&
                                (wait_blocker.cluster == SIZE_MAX || time < wait_time)) {
                                wait_blocker.cluster = actor;
                                wait_blocker.pred_cluster = predecessor;
                                wait_time = time;
                            }
                        };
                        SchedulerTimelineTrace::TimePoint wait_begin{};
                        if (sample_wait) wait_begin = SchedulerTimelineTrace::Clock::now();
                        // Reuse the single-clock stable-sweep protocol. Only
                        // this worker can relinquish its actors, in service()
                        // AFTER the entire sweep. A concurrent peer handoff can
                        // only add an actor; refresh acquires it on the next sweep.
                        const bool stable_sweep =
                            !dynamic || migration_request_.state.load(std::memory_order_acquire) ==
                                            static_cast<uint8_t>(MigrationRequestState::None);
                        if (dynamic && seen_generation != cluster_assignment_generation_.load(
                                                              std::memory_order_acquire))
                            refresh();
                        for (const auto index : owned_bridges) {
                            if (dynamic && !stable_sweep &&
                                cluster_runtime_owner_[clusters_.numClusters() + index].load(
                                    std::memory_order_acquire) != worker)
                                continue;
                            if (profile) ++profile->bridge_polls;
                            progress = bridge_step(index, blocked, profile) || progress;
                        }
                        for (const auto c : owned_clusters) {
                            if (dynamic && !stable_sweep &&
                                cluster_runtime_owner_[c].load(std::memory_order_acquire) != worker)
                                continue;
                            if (profile) ++profile->cluster_polls;
                            auto& state = runtime.clusters[c];
                            auto& published = thread_progress_array_[c].completed_cycle;
                            const auto cycle = published.load(std::memory_order_relaxed);
                            if (dynamic && dynamicMigrationBlocksCluster_(c, cycle)) continue;
                            if (cycle >= state.domain->allowed.load(std::memory_order_acquire)) {
                                if (profile) ++profile->allowance_waits;
                                if (sample_wait) blocked(c, SIZE_MAX, state.clock->edge(cycle));
                                continue;
                            }
                            bool ready = true;
                            for (const auto& bridge : state.bridges) {
                                if (bridge.progress->load(std::memory_order_acquire) <= cycle) {
                                    if (profile) ++profile->dependency_waits;
                                    ready = false;
                                    if (sample_wait)
                                        blocked(c, bridge.actor, state.clock->edge(cycle));
                                    break;
                                }
                            }
                            BlockedClusterInfo blocker;
                            if (!ready) continue;
                            if (!clusterCanAdvance_(c, cycle, blocker, cache.data())) {
                                if (profile) ++profile->dependency_waits;
                                if (sample_wait)
                                    blocked(c, blocker.pred_cluster, state.clock->edge(cycle));
                                continue;
                            }
                            {
                                detail::ClockProfileScope ticks_profile(profile ? &profile->tick_ns
                                                                                : nullptr);
                                executeClusterOneCycle_(worker, c, cycle, false, dynamic,
                                                        state.sample_interval);
                            }
                            if (profile) ++profile->cluster_ticks;
                            published.store(cycle + 1, std::memory_order_release);
                            progress = true;
                        }
                        actors_profile.finish();
                        if (profile && !progress) ++profile->idle_sweeps;
                        if (sample_wait && !progress) {
                            const auto elapsed =
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    SchedulerTimelineTrace::Clock::now() - wait_begin)
                                    .count();
                            recordClockWaitSample_(worker, wait_blocker, wait_time,
                                                   static_cast<uint64_t>(elapsed));
                        }
                        if (dynamic && !settling) {
                            if (worker == 0)
                                maybeRequestEpochFreeMigration_(dynamicMigrationCycle_());
                            // Only the source can publish an owner change, after
                            // its entire sweep; targets acquire actor-private
                            // circuit, unit sampling and SPSC producer state.
                            serviceEpochFreeMigration_(worker);
                        }
                        detail::ClockProfileScope wait_profile(
                            profile && !progress ? &profile->wait_ns : nullptr);
                        if (progress) {
                            idle_sweeps = 0;
                        } else if (detail::shouldYieldDynamicWaitThread(
                                       idle_sweeps++, detail::kFloorWaitThreadYieldSpinMask)) {
                            // A persistent worker must let its peers (including
                            // the trace backend) run on oversubscribed hosts.
                            // Keep short waits on the existing CPU-pause path.
                            std::this_thread::yield();
                        } else {
                            cpuPause();
                        }
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
        if (runtime.pending_size) {
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
            while (runtime.pending_size &&
                   runtime.pendingAt(runtime.pending_size - 1).time > boundary)
                --runtime.pending_size;
            for (auto* domain : runtime.indexed_domains)
                domain->allowed.store(domain->serial->next_cycle, std::memory_order_relaxed);
            for (size_t b = 0; b < runtime.pending_size; ++b)
                for (const auto& edge : runtime.pendingAt(b).edges)
                    edge.domain->allowed.store(edge.cycle + 1, std::memory_order_relaxed);
            done.store(runtime.pending_size == 0, std::memory_order_relaxed);
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
