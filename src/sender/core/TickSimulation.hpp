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
#include "../schedule/CycleAnalyzer.hpp"
#include "../schedule/DependencyGraph.hpp"
#include "../schedule/PlatformBenchmark.hpp"
#include "../schedule/SchedulerTimelineTrace.hpp"
#include "../schedule/SimulatedAnnealingPartitioner.hpp"
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
#include <array>
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
 * Execution model: Chronon selects sequential, barrier, per-epoch lookahead,
 * or epoch-free lookahead at runtime. In lookahead mode, tight clusters advance
 * on dependency progress atomics; the epoch-free path is the default when the
 * safety gate can prove that no per-epoch flush is required.
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
     * Termination is observed at scheduler boundaries with very low overhead;
     * parallel lookahead paths also propagate the stop token into spin waits.
     * If max_cycles is reached without a request, the controller is updated
     * with MaxCyclesReached.
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

    /**
     * @brief Write the scheduler execution timeline at end of run.
     *
     * Always written as a standalone file (scheduler_timeline.pftrace) separate
     * from the simulation's timeline.pftrace.  When the observation backend is
     * running, the file is placed in its timestamped output directory; otherwise
     * it is written relative to cwd.
     */
    void writeTimelineTrace() {
        if (!timeline_trace_.enabled()) {
            return;
        }
        auto& obs = observe::ObservationManager::instance();
        if (obs.isBackendRunning() && obs.backend()) {
            timeline_trace_.write(obs.backend()->outputDir());
        } else {
            timeline_trace_.write();
        }
    }

    /// Number of runParallel() invocations that took the epoch-free path.
    /// Test/bench introspection for the enable_epoch_free_lookahead A/B knob.
    uint64_t epochFreeRunCount() const noexcept { return epoch_free_run_count_; }

    /// Total dropped staged pushes across all MPSC InPorts (full physical ring).
    /// Nonzero means the lookahead window outran the staging capacity — a
    /// correctness failure. The epoch-free A/B watchdog (see
    /// enable_epoch_free_lookahead). Always 0 for the barrier/sequential paths.
    uint64_t totalStagingOverflowEvents() const noexcept {
        uint64_t total = 0;
        for (const IArbitratablePort* p : mpsc_inports_) total += p->stagingOverflowEvents();
        return total;
    }

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

    const DependencyGraph& dependencyGraph() const { return dep_graph_; }

    const std::vector<double>& getUnitCosts() const { return unit_costs_; }
    const PlatformMetrics& getPlatformMetrics() const { return platform_metrics_; }

    /// Provide explicit per-unit costs instead of using uniform-cost initial partitioning.
    void setPrecomputedUnitCosts(std::vector<double> unit_costs, PlatformMetrics metrics) {
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
    bool persistentLookaheadEligible_() const;
    bool epochFreeLookaheadEligible_() const;

    bool executeUnitCycle_(TickableUnit* unit, uint64_t cycle) {
        if (!unit->usesActivityScheduling()) {
            unit->executeTickAlwaysActive();
            return true;
        }
        if (unit->shouldRunTickAt(cycle)) {
            unit->executeTick();
            return true;
        } else {
            unit->advanceIdleTick(1);
            return false;
        }
    }

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
                auto work = stdexec::bulk(stdexec::just(), stdexec::par, units_.size(),
                                          [this](std::size_t idx) {
                                              auto* unit = unit_ptrs_[idx];
                                              executeUnitCycle_(unit, unit->localCycle());
                                          });
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
                        std::vector<char> active(thread_units_[thread_idx].size(), 0);
                        points[0] = SchedulerTimelineTrace::Clock::now();
                        for (size_t pos = 0; pos < thread_units_[thread_idx].size(); ++pos) {
                            size_t unit_idx = thread_units_[thread_idx][pos];
                            active[pos] = executeUnitCycle_(unit_ptrs_[unit_idx],
                                                            unit_ptrs_[unit_idx]->localCycle());
                            points[pos + 1] = SchedulerTimelineTrace::Clock::now();
                        }
                        for (size_t pos = 0; pos < thread_units_[thread_idx].size(); ++pos) {
                            size_t unit_idx = thread_units_[thread_idx][pos];
                            timeline_trace_.recordDuration(
                                thread_idx, active[pos] ? "unit" : "unit idle",
                                unit_ptrs_[unit_idx]->name(), cycle, points[pos], points[pos + 1],
                                active[pos] ? "" : "cycles=1");
                        }
                    } else {
                        for (size_t unit_idx : thread_units_[thread_idx]) {
                            executeUnitCycle_(unit_ptrs_[unit_idx],
                                              unit_ptrs_[unit_idx]->localCycle());
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
    void validateNoZeroDelayCycles_() const;

    /**
     * Topologically reorder unit_ptrs_ via SCC condensation. Within a full
     * SCC, members are ordered by their acyclic zero-delay subgraph so
     * same-cycle producers tick before consumers; creation order is the
     * tie-breaker. Result is stable for a given graph but not fully canonical
     * (Tarjan traversal order plus builder ids).
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
    PartitionInput buildUnitPartitionInput_(double sync_cost_ns) const;

    void assignThreadsDeterministic_();
    void assignThreadsFromPrecomputedCosts_();
    void applyClusteredThreadAssignment_(size_t num_threads, double partition_sync_cost_ns);

    /// Parallel is beneficial when the bottleneck thread cost (with 10%
    /// sync margin) is less than total sequential cost.
    bool parallelBeneficialWeighted_() const;

    bool shouldRebalance_() const;
    bool performRebalance_();
    void maybeRebalanceAfterEpoch_(uint64_t epoch_executed);
    void recordTickSample_(size_t thread_idx, size_t unit_local_idx, uint64_t ticks, bool active);
    void recordClusterTickSample_(size_t cluster, uint64_t ticks, bool active);

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
     * migration. Dynamic rebalance can move endpoints at scheduler fence points,
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

    /**
     * Persistent-worker variant of executeEpochProgressBased: one bulk launch for
     * the whole run, epoch boundaries crossed via a reusable std::barrier, so the
     * stdexec thread pool no longer heap-allocates a bulk op-state per epoch. Used
     * by runParallel() as the fixed-layout fallback when the epoch-free gate is
     * unavailable. Returns cycles executed.
     */
    uint64_t executeRunProgressBased(uint64_t total_cycles);

    /**
     * Epoch-free variant of executeRunProgressBased: one bulk launch, one
     * run-spanning window, NO per-epoch barrier. Each worker drives its
     * clusters straight to run_target; run-ahead is bounded only by the
     * lookahead_floor_ + max_lookahead_cycles synthetic dep (refreshed lazily
     * on the slow path), and MPSC delivery is the per-connection consumer-driven
     * drain plus a single arbitrateAllMPSCPorts_() flush after the workers join.
     * Selected by runParallel() only when allMPSCPortsHaveConnProgress_() and
     * max_lookahead_cycles > 0. Returns cycles actually executed.
     */
    uint64_t executeRunEpochFree_(uint64_t total_cycles);
    uint64_t executeRunEpochFreeDynamic_(uint64_t total_cycles);

    /// Completed cycles reported by a run-spanning worker launch. When a unit
    /// requests termination on a nonzero cluster, cluster 0 may lag behind the
    /// terminating tick; account for the request cycle so runUntilTermination()
    /// advances current_cycle_ far enough to cover the observed stop point.
    uint64_t completedCyclesForRun_(uint64_t run_start, uint64_t run_target) const noexcept;

    /// True iff every MPSC InPort has fully resolved per-connection producer
    /// progress, so the consumer-driven drain needs no per-epoch central flush.
    /// Precondition for executeRunEpochFree_ (see enable_epoch_free_lookahead).
    bool allMPSCPortsHaveConnProgress_() const noexcept;

    /// Grow lock-free queues with no positive headroom, or switch the port to
    /// thread-safe queueing if bounded capacity makes that impossible.
    size_t demoteUnsafeEpochFreeQueues_();

    /// Minimum cross-thread queue headroom over all connections. SIZE_MAX means
    /// no bounded cross-thread ring constrains epoch-free run-ahead.
    size_t crossThreadHeadroomLimit_() const noexcept;

    /// True when every remaining finite cross-thread queue has at least one
    /// cycle of safe run-ahead after unsafe ports have been demoted.
    bool crossThreadHeadroomAllowsEpochFree_() const noexcept;

    /// Compatibility helper used by logs/tests.
    bool crossThreadHeadroomFits_(uint64_t max_lookahead) const noexcept;

    /// Per-thread epoch body. Spin-waits on cross-thread progress atomics
    /// and exits via stop_token on termination or exception.
    void executeThreadEpoch_(size_t thread_idx, uint64_t end_cycle,
                             stdexec::inplace_stop_token token);
    void executeThreadEpochDynamic_(size_t thread_idx, uint64_t end_cycle,
                                    stdexec::inplace_stop_token token);

    /// Monotonically raise lookahead_floor_ to the global-min completed cycle.
    /// Slow-path only (a stalled worker); see lookahead_floor_ for the contract.
    void refreshLookaheadFloor_();

    /// Test-only white-box access for test_lookahead_floor_progress.
    friend struct LookaheadFloorTestAccess;

    bool clusterCanAdvance_(size_t cluster, uint64_t cycle, BlockedClusterInfo& blocker) const;
    uint64_t computeIdleClusterTarget_(size_t cluster, uint64_t cycle, uint64_t end_cycle) const;
    void advanceClusterIdle_(size_t cluster, uint64_t delta);
    void recordClusterIdle_(size_t thread_idx, size_t cluster, uint64_t cycle, uint64_t delta,
                            SchedulerTimelineTrace::TimePoint begin,
                            SchedulerTimelineTrace::TimePoint end);
    std::string formatBlockerDetail_(const BlockedClusterInfo& blocker) const;
    void executeClusterOneCycle_(size_t thread_idx, size_t cluster, uint64_t cycle,
                                 bool trace_units);

    void initDynamicMigrationRuntime_();
    void rebuildThreadUnitsFromClusterOwners_();
    bool maybeRequestEpochFreeMigration_(uint64_t cycle);
    void serviceEpochFreeMigration_();
    void clearDynamicMigrationRequest_();
    void recordDynamicWaitSample_(size_t thread_idx, const BlockedClusterInfo& blocker,
                                  uint64_t wait_ns);
    void resetDynamicSchedulerMarkers_();
    void recordDynamicSchedulerMarker_(std::string name, uint64_t cycle, std::string detail);
    void flushDynamicSchedulerMarkers_();

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

    /// Count of runParallel() invocations dispatched to executeRunEpochFree_.
    /// Introspection only (see epochFreeRunCount()).
    uint64_t epoch_free_run_count_ = 0;

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

    /// Per-epoch floor for max_lookahead_cycles enforcement: clusters gate at
    /// floor + max_lookahead_cycles.  Memory-order contract: relaxed monotone
    /// CAS write (refreshLookaheadFloor_), acquire read (clusterCanAdvance_).
    /// A gating HINT only — never carries data, and a stale value is always
    /// safe (blocks marginally sooner); real data sync uses ThreadProgress.
    alignas(64) std::atomic<uint64_t> lookahead_floor_{0};
    std::vector<std::vector<TickableUnit*>> thread_unit_ptrs_;
    std::vector<std::vector<size_t>> thread_clusters_;
    std::vector<std::vector<TickableUnit*>> cluster_unit_ptrs_;
    std::vector<std::vector<size_t>> cluster_thread_unit_positions_;
    std::vector<std::vector<SchedulerTimelineTrace::TimePoint>> thread_trace_points_;

    enum class MigrationRequestState : uint8_t {
        None = 0,
        Requested,
        Quiescing,
        ReadyToCommit,
        Committed,
    };

    struct RuntimeMigrationRequest {
        std::atomic<uint8_t> state{static_cast<uint8_t>(MigrationRequestState::None)};
        std::atomic<size_t> cluster{SIZE_MAX};
        std::atomic<size_t> source_thread{SIZE_MAX};
        std::atomic<size_t> target_thread{SIZE_MAX};
        std::atomic<uint64_t> fence_cycle{0};
    };

    RuntimeMigrationRequest migration_request_;
    // unique_ptr owns variable-size arrays; atomic elements publish runtime
    // ownership/progress between worker threads during epoch-free rebalance.
    std::unique_ptr<std::atomic<size_t>[]> cluster_runtime_owner_;
    std::unique_ptr<std::atomic<size_t>[]> cluster_execution_owner_;
    std::unique_ptr<std::atomic<uint8_t>[]> cluster_migration_pending_;
    std::unique_ptr<std::atomic<uint64_t>[]> cluster_sample_time_ns_;
    std::unique_ptr<std::atomic<uint64_t>[]> cluster_sample_count_;
    std::unique_ptr<std::atomic<uint64_t>[]> cluster_active_sample_count_;
    std::unique_ptr<std::atomic<uint64_t>[]> dynamic_thread_floor_wait_ns_;
    std::unique_ptr<std::atomic<uint64_t>[]> dynamic_thread_dep_wait_ns_;
    std::unique_ptr<std::atomic<uint64_t>[]> dynamic_thread_no_ready_wait_ns_;
    std::unique_ptr<std::atomic<uint64_t>[]> dynamic_cluster_blocked_wait_ns_;
    std::unique_ptr<std::atomic<uint64_t>[]> dynamic_cluster_blocker_wait_ns_;
    std::vector<uint64_t> dynamic_cluster_last_migration_cycle_;
    std::vector<size_t> dynamic_cluster_last_source_thread_;
    std::vector<size_t> dynamic_cluster_last_target_thread_;
    std::atomic<uint64_t> cluster_assignment_generation_{0};
    size_t dynamic_runtime_cluster_count_ = 0;
    size_t dynamic_runtime_thread_count_ = 0;
    alignas(64) std::atomic<uint64_t> next_dynamic_rebalance_check_cycle_{0};
    alignas(64) std::atomic<bool> epoch_free_dynamic_runtime_active_{false};

    struct DynamicSchedulerMarker {
        SchedulerTimelineTrace::TimePoint time{};
        uint64_t cycle = 0;
        std::string name;
        std::string detail;
    };
    static constexpr size_t kDynamicSchedulerMarkerCapacity = 1024;
    std::array<DynamicSchedulerMarker, kDynamicSchedulerMarkerCapacity> dynamic_scheduler_markers_;
    std::atomic<size_t> dynamic_scheduler_marker_count_{0};
    std::atomic<uint64_t> dynamic_scheduler_marker_drops_{0};

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
    std::vector<double> unit_costs_;

    std::vector<double> precomputed_unit_costs_;
    PlatformMetrics precomputed_platform_metrics_{};
    bool has_precomputed_costs_ = false;

    struct alignas(64) ThreadSamplingState {
        std::vector<uint64_t> tick_times;
        std::vector<uint8_t> active_samples;
        std::vector<size_t> unit_write_idx;
        std::vector<size_t> unit_sample_count;
        size_t sample_count = 0;
        static constexpr size_t RING_SIZE = 128;
    };
    std::vector<ThreadSamplingState> thread_sampling_;
    uint64_t cycles_since_last_rebalance_ = 0;
    uint64_t cycles_since_last_actual_rebalance_ = 0;
    uint64_t rebalance_count_ = 0;
};

}  // namespace chronon::sender
