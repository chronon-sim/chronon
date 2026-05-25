// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// High-performance tick-based simulation driver. Uses stdexec for parallel
/// execution; units are state machines (tick()) rather than coroutines.

#pragma once

#include "../../observe/ObservationManager.hpp"
#include "../port/Connection.hpp"
#include "../port/Port.hpp"
#include "../schedule/CostProfileCache.hpp"
#include "../schedule/CycleAnalyzer.hpp"
#include "../schedule/DependencyGraph.hpp"
#include "../schedule/PlatformBenchmark.hpp"
#include "../schedule/SchedulerTimelineTrace.hpp"
#include "../schedule/SimulatedAnnealingPartitioner.hpp"
#include "../schedule/TickCostProfiler.hpp"
#include "../schedule/WeightedPartitioner.hpp"
#include "TerminationRequest.hpp"
#include "TickSimulationConfig.hpp"
#include "TickableUnit.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wextra-semi"
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>
#include <stdexec/stop_token.hpp>
#pragma GCC diagnostic pop

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace chronon::sender {

/**
 * High-performance simulation using stdexec.
 *
 * Execution model: for each epoch, compute safe boundaries (lookahead),
 * dispatch parallel work via stdexec::bulk + starts_on, then sync_wait at
 * the epoch boundary. No per-cycle sync overhead.
 *
 * @code
 * TickSimulation sim;
 * auto* fetch = sim.createUnit<FetchUnit>();
 * auto* decode = sim.createUnit<DecodeUnit>();
 * sim.connect(fetch->out, decode->in, 1);
 * sim.initialize();
 * sim.run(1'000'000);
 * @endcode
 */
class TickSimulation {
public:
    static size_t normalizeThreadCount(size_t requested) noexcept {
        if (requested == 0) {
            requested = std::thread::hardware_concurrency();
        }
        return requested == 0 ? 1 : requested;
    }

    explicit TickSimulation(const TickSimulationConfig& config = {})
        : config_(config),
          current_cycle_(0),
          initialized_(false),
          pool_(static_cast<uint32_t>(normalizeThreadCount(config.num_threads))) {
        config_.num_threads = normalizeThreadCount(config_.num_threads);
        timeline_trace_.configure(config_.timeline_trace);
        resolveSolver_();
    }

    ~TickSimulation() { freeThreadProgressArray(); }

    TickSimulation(const TickSimulation&) = delete;
    TickSimulation& operator=(const TickSimulation&) = delete;

    size_t assignedThread(Unit* unit) const {
        if (!unit) return SIZE_MAX;
        for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
            if (static_cast<Unit*>(unit_ptrs_[i]) != unit) continue;
            if (i < unit_to_cluster_.size()) {
                size_t cluster = unit_to_cluster_[i];
                if (cluster < cluster_to_thread_.size()) {
                    return cluster_to_thread_[cluster];
                }
            }
            if (i < cluster_to_thread_.size()) {
                return cluster_to_thread_[i];
            }
            return SIZE_MAX;
        }
        return SIZE_MAX;
    }

    uint64_t rebalanceCount() const { return rebalance_count_; }

    template <typename UnitT, typename... Args>
    UnitT* createUnit(Args&&... args) {
        static_assert(std::is_base_of_v<TickableUnit, UnitT>,
                      "UnitT must derive from TickableUnit");

        auto unit = std::make_unique<UnitT>(std::forward<Args>(args)...);
        auto* ptr = unit.get();

        ptr->setId(static_cast<uint32_t>(units_.size()));

        units_.push_back(std::move(unit));
        unit_ptrs_.push_back(ptr);

        return ptr;
    }

    template <typename T>
    void connect(OutPort<T>& from, InPort<T>& to, uint32_t delay = 1) {
        auto* conn = from.connect(&to, delay);
        conn->setConnId(static_cast<uint32_t>(connections_.size()));
        connections_.push_back(conn);
    }

    /// For YAML-driven builders that create connections via type-erased port
    /// handles rather than the templated connect() above.
    void registerConnection(ConnectionBase* conn) {
        if (conn) {
            conn->setConnId(static_cast<uint32_t>(connections_.size()));
            connections_.push_back(conn);
        }
    }

    void initialize();

    /// Resolve the producer-cluster completed_cycle atomics for each MPSC
    /// InPort. Lookahead-only; under Sequential/Barrier the producer-
    /// progress set stays empty and arbitrateMPSCConsumerDriven() degrades
    /// to an unbounded drain (correct because those modes only call the
    /// consumer-driven hook after a global sync point).
    void installMPSCProducerProgress_();

    /// Run for the specified cycles. Internally dispatches to parallel or
    /// sequential execution based on cluster analysis.
    uint64_t run(uint64_t num_cycles);

    /// Run until `should_stop()` returns true or max_cycles reached.
    template <typename Predicate>
    uint64_t runUntil(Predicate&& should_stop, uint64_t max_cycles = UINT64_MAX) {
        if (!initialized_) {
            initialize();
        }

        uint64_t executed = 0;
        while (executed < max_cycles && !should_stop()) {
            uint64_t batch = std::min(config_.epoch_size, max_cycles - executed);
            executed += run(batch);
        }
        return executed;
    }

    uint64_t runUntilComplete(uint64_t max_cycles = UINT64_MAX) {
        return runUntil(
            [this]() {
                for (auto& unit : units_) {
                    if (!unit->isCompleted()) return false;
                }
                return true;
            },
            max_cycles);
    }

    /**
     * Run until termination is requested or max_cycles is reached.
     *
     * Termination is checked at epoch boundaries (~64 cycles) with very low
     * overhead. If max_cycles is reached without a request, the controller
     * is updated with MaxCyclesReached.
     */
    uint64_t runUntilTermination(uint64_t max_cycles = UINT64_MAX);

    /// Only valid after runUntilTermination() returns.
    const TerminationRequest& terminationRequest() const noexcept {
        return termination_ctrl_.getRequest();
    }

    bool wasTerminationRequested() const noexcept {
        return termination_ctrl_.isTerminationRequested();
    }

    /// Externally-driven termination, e.g. signal handlers or API calls.
    void requestTermination(TerminationReason reason, int32_t exit_code = 0,
                            std::string_view message = "") {
        termination_ctrl_.requestTermination(reason, exit_code, current_cycle_, "external",
                                             message);
    }

    /// Reconstructs the stop_source (inplace_stop_source is non-resettable).
    void resetTermination() noexcept {
        termination_ctrl_.reset();
        stop_source_.emplace();
        termination_ctrl_.setStopSource(&*stop_source_);
    }

    /// Token reflects both unit-initiated termination and exception-driven
    /// abort. Used by external coordination (e.g. observation backends).
    stdexec::inplace_stop_token get_stop_token() const noexcept {
        return stop_source_->get_token();
    }

    uint64_t currentCycle() const noexcept { return current_cycle_; }
    uint64_t tickFrequencyHz() const noexcept { return config_.tick_frequency_hz; }
    size_t unitCount() const noexcept { return units_.size(); }
    bool isInitialized() const noexcept { return initialized_; }
    bool timelineTraceEnabled() const noexcept { return timeline_trace_.enabled(); }
    const std::string& timelineTraceFile() const noexcept { return timeline_trace_.file(); }
    void writeTimelineTrace() { timeline_trace_.write(); }

    /// Used by observation async I/O.
    exec::static_thread_pool& pool() noexcept { return pool_; }
    bool hasTightConnectionsInGraph() const noexcept { return has_tight_connections_; }
    bool isParallelBeneficial() const noexcept { return parallel_beneficial_; }
    bool useParallelExecution() const noexcept {
        return config_.enable_parallel && units_.size() > 1 && parallel_beneficial_;
    }

    TickableUnit* getUnit(const std::string& name) {
        for (auto& unit : units_) {
            if (unit->name() == name) {
                return unit.get();
            }
        }
        return nullptr;
    }

    template <typename UnitT>
    UnitT* getUnit(const std::string& name) {
        return dynamic_cast<UnitT*>(getUnit(name));
    }

    const std::vector<double>& getProfiledUnitCosts() const { return profiled_unit_costs_; }
    const PlatformMetrics& getPlatformMetrics() const { return platform_metrics_; }

    /// When set, initialize() skips internal profiling and uses these costs.
    void setPrecomputedProfilingData(std::vector<double> unit_costs, PlatformMetrics metrics) {
        precomputed_unit_costs_ = std::move(unit_costs);
        precomputed_platform_metrics_ = metrics;
        has_precomputed_costs_ = true;
    }

private:
    enum class ExecutionMode {
        Sequential,
        Parallel,
    };

    bool shouldUseParallelExecution_() const noexcept {
        return execution_mode_ == ExecutionMode::Parallel;
    }

    bool parallelBeneficialFromThreadAssignment_() const noexcept;

    std::string buildUnitNameList_(const std::vector<size_t>& unit_indices) const;

    void resolveSolver_();
    PartitionResult runPartitionSolver_(const PartitionInput& input);
    PartitionResult runRebalanceSolver_(const PartitionInput& input);

    void selectExecutionMode_();
    void traceExecutionMode_();

    uint64_t runSequential(uint64_t num_cycles);
    uint64_t runSequentialEpoch(uint64_t epoch_cycles);
    uint64_t runParallelEpoch(uint64_t epoch_cycles, uint64_t executed_offset);
    uint64_t runParallel(uint64_t num_cycles);

    /**
     * Barrier scheduler: per-cycle parallel advancement across clusters.
     *
     * Every cluster advances exactly one cycle, then all threads rejoin at
     * sync_wait. Units inside a cluster always run sequentially on the same
     * thread, preserving order for intra-cluster tight (delay=0) edges.
     */
    template <typename Scheduler>
    void executeEpochBarrier(Scheduler& sched, uint64_t epoch_cycles) {
        const size_t num_threads = thread_units_.size();
        if (num_threads == 0) {
            for (uint64_t c = 0; c < epoch_cycles; ++c) {
                auto work =
                    stdexec::bulk(stdexec::just(), stdexec::par, units_.size(),
                                  [this](std::size_t idx) { unit_ptrs_[idx]->executeTick(); });
                auto scheduled = stdexec::starts_on(sched, std::move(work));
                stdexec::sync_wait(std::move(scheduled));
                arbitrateAllMPSCPorts_();
            }
            return;
        }

        for (uint64_t c = 0; c < epoch_cycles; ++c) {
            const uint64_t cycle = current_cycle_ + c;
            auto work = stdexec::bulk(
                stdexec::just(), stdexec::par, num_threads, [this, cycle](std::size_t thread_idx) {
                    if (timeline_trace_.traceUnits()) {
                        auto& points = thread_trace_points_[thread_idx];
                        points[0] = SchedulerTimelineTrace::Clock::now();
                        for (size_t pos = 0; pos < thread_units_[thread_idx].size(); ++pos) {
                            size_t unit_idx = thread_units_[thread_idx][pos];
                            unit_ptrs_[unit_idx]->executeTick();
                            points[pos + 1] = SchedulerTimelineTrace::Clock::now();
                        }
                        for (size_t pos = 0; pos < thread_units_[thread_idx].size(); ++pos) {
                            size_t unit_idx = thread_units_[thread_idx][pos];
                            timeline_trace_.recordDuration(thread_idx, "unit",
                                                           unit_ptrs_[unit_idx]->name(), cycle,
                                                           points[pos], points[pos + 1]);
                        }
                    } else {
                        for (size_t unit_idx : thread_units_[thread_idx]) {
                            unit_ptrs_[unit_idx]->executeTick();
                        }
                    }
                });
            auto scheduled = stdexec::starts_on(sched, std::move(work));
            stdexec::sync_wait(std::move(scheduled));
            if (timeline_trace_.traceArbitration()) {
                auto begin = SchedulerTimelineTrace::Clock::now();
                arbitrateAllMPSCPorts_();
                auto end = SchedulerTimelineTrace::Clock::now();
                timeline_trace_.recordDuration(timeline_trace_.schedulerStream(), "scheduler",
                                               "mpsc arbitration", cycle, begin, end);
            } else {
                arbitrateAllMPSCPorts_();
            }
        }
    }

    void executeUnitToTarget(size_t unit_idx, uint64_t target_cycle);

    /**
     * Compute safe execution boundary for a unit using DIRECT predecessors
     * only — transitive constraints are already enforced by intermediate
     * units. Floyd-Warshall distances would over-constrain the schedule.
     */
    uint64_t computeSafeBoundary(size_t unit_idx, uint64_t epoch_end) const;

    void buildDependencyGraph();
    void buildPredecessorCache();

    /**
     * Topologically reorder unit_ptrs_ via SCC condensation. Units within
     * the same SCC are kept in creation order; SCCs are topo-sorted so
     * producers tick before consumers. Result is stable for a given graph
     * but not fully canonical (Tarjan traversal order plus builder ids).
     * Must be followed by buildDependencyGraph() to rebuild graph indices.
     */
    void reorderUnitsTopologically_();

    bool hasTightConnections() const;

    /**
     * Aggregate connections between unit pairs into a single edge with
     * count and minimum delay. Directed edges only (see implementation
     * for rationale).
     */
    void buildPartitionAdjacency_(const std::unordered_map<Unit*, size_t>& unit_ptr_to_idx,
                                  PartitionInput& input) const;

    void profileAndAssignThreadsClustered_();
    void assignThreadsDeterministic_();
    void assignThreadsFromPrecomputedCosts_();
    void applyClusteredThreadAssignment_(size_t num_threads);

    /// Parallel is beneficial when the bottleneck thread cost (with 10%
    /// sync margin) is less than total sequential cost.
    bool parallelBeneficialWeighted_() const noexcept;

    bool shouldRebalance_() const;
    bool performRebalance_();
    void recordTickSample_(size_t thread_idx, size_t unit_local_idx, uint64_t ticks);

    /**
     * Topology-only cluster-aware placement (no cost profiling). Used as
     * the legacy path when weighted partitioning is disabled but tight
     * connections exist.
     */
    void buildClusterAffinity();
    bool hasTightInterClusterConnections() const;

    void optimizeAllQueuesForSingleThread();
    void optimizeConnectionQueuesForThreads();

    /**
     * Pick queue adapters that remain valid after runtime cluster
     * migration. Dynamic rebalance can move endpoints at epoch boundaries,
     * so queue type cannot depend on initial thread placement, and
     * reconfiguring adapters mid-run would drop future-cycle messages.
     */
    void optimizeConnectionQueuesForDynamicRebalance_();

    void buildThreadAssignment();
    void buildCrossThreadDependencies();

    void freeThreadProgressArray();
    void initProgressSync();
    void initTimelineTraceScratch_();

    /**
     * Lookahead scheduler (progress-atomic backend).
     *
     * Each thread runs its assigned clusters cycle-by-cycle and publishes
     * its progress to a cache-line-aligned atomic; peers spin on those
     * atomics. No per-iteration sync_wait. The unified stop_source_
     * prevents spin-wait deadlock and propagates unit-initiated
     * termination into mid-epoch waits.
     */
    void executeEpochProgressBased(uint64_t epoch_cycles);

    /// Per-thread epoch body. Spin-waits on cross-thread progress atomics
    /// and exits via stop_token on termination or exception.
    void executeThreadEpoch_(size_t thread_idx, uint64_t end_cycle,
                             stdexec::inplace_stop_token token);

    bool clusterCanAdvance_(size_t cluster, uint64_t cycle, BlockedClusterInfo& blocker) const;
    std::string formatBlockerDetail_(const BlockedClusterInfo& blocker) const;
    void executeClusterOneCycle_(size_t thread_idx, size_t cluster, uint64_t cycle,
                                 bool trace_units);

    /**
     * Drive cycle-boundary MPSC arbitration on every registered InPort.
     *
     * Called after every scheduler sync point. The InPort arbiter drains
     * each Connection's staging deque in topology-stable conn_id order,
     * making admission independent of wall-clock push ordering across
     * worker threads. `mpsc_inports_` preserves port-registration order.
     */
    [[gnu::always_inline]] inline void arbitrateAllMPSCPorts_() noexcept {
        for (auto* p : mpsc_inports_) {
            p->arbitrateMPSC();
        }
    }

    /**
     * Collect MPSC-mode InPorts from `connections_`. Iterates in
     * registration order, filters by MPSC flag, registers each on its
     * InPort (which sorts by conn_id internally), and maintains
     * `mpsc_inports_` deduplicated in first-seen order.
     */
    void collectMPSCInPorts_();

    /// Rethrow the current exception as TickException. Wraps non-
    /// TickException throwables with unknown unit context.
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] static void throwTickException();

    TickSimulationConfig config_;
    uint64_t current_cycle_;
    bool initialized_;
    ExecutionMode execution_mode_ = ExecutionMode::Sequential;
    bool has_tight_connections_ = false;
    bool has_tight_inter_cluster_ = false;
    bool parallel_beneficial_ = false;

    TerminationController termination_ctrl_;

    ::exec::static_thread_pool pool_;

    std::vector<std::unique_ptr<TickableUnit>> units_;
    std::vector<TickableUnit*> unit_ptrs_;

    std::unique_ptr<std::atomic<uint64_t>[]> unit_progress_;

    /// Pointers only — Connections are owned by their OutPort.
    std::vector<ConnectionBase*> connections_;

    /// Flat, deduplicated list of InPorts with at least one MPSC
    /// connection, populated after optimizeConnectionQueuesForThreads() in
    /// initialize(). Walked at every cycle boundary.
    std::vector<IArbitratablePort*> mpsc_inports_;

    DependencyGraph dep_graph_;
    AnalysisResult analysis_;

    /// predecessor_cache_[unit_idx] = [(pred_idx, delay), ...]
    std::vector<std::vector<std::pair<size_t, uint32_t>>> predecessor_cache_;

    TightCouplingResult clusters_;
    std::vector<size_t> unit_to_cluster_;
    std::vector<size_t> cluster_to_thread_;
    std::vector<std::vector<size_t>> thread_units_;

    std::vector<std::vector<ThreadCrossDep>> thread_cross_deps_temp_;
    ThreadProgress* thread_progress_array_ = nullptr;
    size_t thread_progress_count_ = 0;
    std::vector<std::vector<ResolvedDep>> thread_resolved_deps_;
    std::vector<std::vector<TickableUnit*>> thread_unit_ptrs_;
    std::vector<std::vector<size_t>> thread_clusters_;
    std::vector<std::vector<TickableUnit*>> cluster_unit_ptrs_;
    std::vector<std::vector<size_t>> cluster_thread_unit_positions_;
    std::vector<std::vector<SchedulerTimelineTrace::TimePoint>> thread_trace_points_;

    /// Unified stop source: prevents spin-wait deadlock AND propagates
    /// unit-initiated termination into worker threads. Wrapped in optional
    /// because inplace_stop_source is non-resettable; resetTermination()
    /// reconstructs via emplace().
    std::optional<stdexec::inplace_stop_source> stop_source_{std::in_place};

    bool has_thread_assignment_ = false;

    observe::ObservationContext* observe_ctx_ = nullptr;
    SchedulerTimelineTrace timeline_trace_;
    std::string last_rebalance_detail_;

    PartitionSolver partition_solver_ = &WeightedPartitioner::partition;
    PartitionSolver rebalance_solver_ = &WeightedPartitioner::partition;
    PlatformMetrics platform_metrics_{};
    std::vector<double> profiled_unit_costs_;

    std::vector<double> precomputed_unit_costs_;
    PlatformMetrics precomputed_platform_metrics_{};
    bool has_precomputed_costs_ = false;

    struct alignas(64) ThreadSamplingState {
        std::vector<uint64_t> tick_times;
        size_t write_idx = 0;
        size_t sample_count = 0;
        static constexpr size_t RING_SIZE = 128;
    };
    std::vector<ThreadSamplingState> thread_sampling_;
    uint64_t cycles_since_last_rebalance_ = 0;
    uint64_t cycles_since_last_actual_rebalance_ = 0;
    uint64_t rebalance_count_ = 0;
};

}  // namespace chronon::sender
