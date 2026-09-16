// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <exception>

#include "../../chronon/CpuPause.hpp"
#include "TickSimulation.hpp"

namespace chronon::sender {

// A bridge is a scheduled actor, not a dependency between whole clock domains.
// Its begin publication grants its endpoint clusters an edge. Their completion
// publications transfer pending port commands back to the bridge for commit.
// The next begin cannot overtake that commit. Consequently the existing circuit
// can retain its ordinary, non-atomic registers and exact pre-edge sampling.
struct TickSimulation::ClockParallelRuntime {
    struct alignas(64) Cluster {
        const ClockDomain* clock = nullptr;
        std::atomic<uint64_t> allowed{0};
        std::vector<const std::atomic<uint64_t>*> bridges;
    };
    struct alignas(64) Endpoint {
        size_t cluster = 0;
        const ClockDomain* clock = nullptr;
        uint64_t next = 0;  // Bridge-owner private.
        std::atomic<uint64_t> prepared{0};
        std::atomic<uint64_t> completed{0};
    };
    struct Bridge {
        CdcComponent* circuit = nullptr;
        std::array<Endpoint, 2> endpoints;
        std::array<ClockEdge, 2> edges;
        std::array<bool, 2> participating{};
        size_t edge_count = 0;
    };
    struct Batch {
        SimTime time;
        std::vector<ClockEdge> edges;
    };

    std::unique_ptr<Cluster[]> clusters;
    std::vector<std::unique_ptr<Bridge>> bridges;
    std::vector<std::vector<size_t>> worker_bridges;
    std::deque<Batch> pending;
};

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
    // The recorder's serial reservation protocol is not a multi-producer
    // protocol. Keep its existing bounded-memory contract until clock actors
    // have independent recording streams and asynchronous watermarks.
    if (clock_trace_ && clock_trace_->enabled())
        return fallback("clock tracing currently requires serial clock scheduling");

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
        if (!settling) {
            while (runtime.pending.size() < config_.max_lookahead_cycles &&
                   scheduled < max_batches && !calendar.empty() &&
                   within_limit(calendar.nextTime()) && !token.stop_requested()) {
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
        if (runtime.pending.empty()) done.store(true, std::memory_order_release);
        return progress;
    };

    const auto bridge_step = [&](ClockParallelRuntime::Bridge& bridge) {
        if (bridge.edge_count) {
            for (size_t side = 0; side < 2; ++side) {
                const auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side] &&
                    thread_progress_array_[endpoint.cluster].completed_cycle.load(
                        std::memory_order_acquire) <= endpoint.next)
                    return false;
            }
            bridge.circuit->commit();
            for (size_t side = 0; side < 2; ++side) {
                auto& endpoint = bridge.endpoints[side];
                if (bridge.participating[side]) {
                    ++endpoint.next;
                    endpoint.completed.store(endpoint.next, std::memory_order_release);
                }
            }
            bridge.edge_count = 0;
            return true;
        }
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
        bridge.circuit->begin(std::span(bridge.edges.data(), bridge.edge_count));
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
                    while (!failed.load(std::memory_order_acquire) &&
                           !done.load(std::memory_order_acquire) &&
                           (settling || !token.stop_requested())) {
                        bool progress = worker == 0 && coordinate(settling);
                        for (const auto index : runtime.worker_bridges[worker])
                            progress = bridge_step(*runtime.bridges[index]) || progress;
                        for (const auto c : thread_clusters_[worker]) {
                            auto& state = runtime.clusters[c];
                            auto& published = thread_progress_array_[c].completed_cycle;
                            const auto cycle = published.load(std::memory_order_relaxed);
                            if (cycle >= state.allowed.load(std::memory_order_acquire)) continue;
                            bool ready = true;
                            for (const auto* bridge : state.bridges)
                                if (bridge->load(std::memory_order_acquire) <= cycle) ready = false;
                            BlockedClusterInfo blocker;
                            if (!ready || !clusterCanAdvance_(c, cycle, blocker, cache.data()))
                                continue;
                            for (auto* unit : cluster_unit_ptrs_[c]) {
                                unit->clock_edge_executing_ = true;
                                executeUnitCycle_(unit, cycle);
                                unit->clock_edge_executing_ = false;
                            }
                            published.store(cycle + 1, std::memory_order_release);
                            progress = true;
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
        for (auto* unit : unit_ptrs_) unit->clock_edge_executing_ = false;
        clock_failed_ = true;
        throw;
    }
    for (size_t i = 0; i < unit_ptrs_.size(); ++i)
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_release);
    termination_ctrl_.setSettledTime(clock_time_);
    return completed;
}

}  // namespace chronon::sender
