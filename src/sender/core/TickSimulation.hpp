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
#include "../schedule/DependencyGraph.hpp"
#include "../schedule/PlatformBenchmark.hpp"
#include "../schedule/SchedulerTimelineTrace.hpp"
#include "../schedule/SimulatedAnnealingPartitioner.hpp"
#include "../schedule/WeightedPartitioner.hpp"
#include "TerminationRequest.hpp"
#include "TickSimulationConfig.hpp"
#include "TickSimulationCycleUtils.hpp"
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
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace chronon::sender {

/**
 * High-performance simulation using stdexec.
 *
 * Execution model: Chronon selects sequential or epoch-free lookahead at
 * initialization. Tight clusters advance on dependency-progress atomics when
 * the epoch-free safety gate proves that no barrier or per-epoch flush is
 * required; otherwise the simulation uses the sequential reference path.
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
        ptr->bindActivitySchedulingState_(&any_activity_scheduling_);

        units_.push_back(std::move(unit));
        unit_ptrs_.push_back(ptr);

        return ptr;
    }

    template <typename T>
    Connection<T>* connect(OutPort<T>& from, InPort<T>& to, uint32_t delay = 1) {
        auto* conn = from.connect(&to, delay);
        conn->setConnId(static_cast<uint32_t>(connections_.size()));
        connections_.push_back(conn);
        return conn;
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

    /// Resolve one producer-cluster completed-cycle atomic for each direct
    /// MPSC lane. Complete coverage is required by epoch-free lookahead.
    void installMultiProducerProgress_();

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
            const uint64_t polling_interval = std::max<uint64_t>(1, config_.epoch_size);
            uint64_t batch = std::min(polling_interval, max_cycles - executed);
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
    size_t transparentBroadcastConnectionCount() const noexcept {
        return transparent_broadcast_connection_count_;
    }
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

    /// Number of epoch-free scheduler invocations. Sequential fallback leaves
    /// this unchanged, making the counter useful for safety-gate coverage.
    uint64_t epochFreeRunCount() const noexcept { return epoch_free_run_count_; }

    /// Total direct-lane pushes rejected by a full physical ring.
    /// Nonzero means the lookahead window outran transport capacity — a
    /// correctness failure. Always 0 for sequential execution.
    uint64_t totalTransportOverflowEvents() const noexcept {
        uint64_t total = 0;
        for (const IMultiProducerPort* p : multi_producer_ports_) {
            total += p->transportOverflowEvents();
        }
        return total;
    }

    uint64_t totalStagingOverflowEvents() const noexcept { return totalTransportOverflowEvents(); }

    /// Used by observation async I/O.
    exec::static_thread_pool& pool() noexcept { return pool_; }
    bool hasTightConnectionsInGraph() const noexcept { return has_tight_connections_; }
    bool isParallelBeneficial() const noexcept { return parallel_beneficial_; }
    bool useParallelExecution() const noexcept { return shouldUseParallelExecution_(); }

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

    /// Preserve topology-stable multi-producer admission even for a fixed
    /// thread assignment. Configure before initialize() when an out-of-band
    /// transport replaces physical connection fanout.
    void forceStableConnectionQueues() noexcept { force_stable_connection_queues_ = true; }

private:
    enum class ExecutionMode {
        Sequential,
        EpochFree,
    };

    bool shouldUseParallelExecution_() const noexcept {
        return execution_mode_ == ExecutionMode::EpochFree;
    }

    bool parallelBeneficialFromThreadAssignment_() const noexcept;

    std::string buildUnitNameList_(const std::vector<size_t>& unit_indices) const;

    void resolveSolver_();
    PartitionResult runPartitionSolver_(const PartitionInput& input);

    void selectExecutionMode_();
    void traceExecutionMode_();
    void initClusterActivityScheduling_();

    uint64_t runSequential(uint64_t num_cycles);
    template <bool PushPeriodicCounters>
    uint64_t runSequentialImpl_(uint64_t num_cycles);
    uint64_t runEpochFree(uint64_t num_cycles);
    bool epochFreeLookaheadEligible_() const;
    std::string epochFreeVetoReason_() const;
    void warnParallelFallbackIfNeeded_();

    bool executeUnitCycle_(TickableUnit* unit, uint64_t cycle) {
        if (!any_activity_scheduling_.enabled.load(std::memory_order_acquire)) {
            unit->executeTickAlwaysActive();
            return true;
        }
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

    void buildDependencyGraph();
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
    size_t optimizeTransparentBroadcasts_();

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
     * Epoch-free scheduler: one bulk launch and one run-spanning window. Each worker drives its
     * clusters straight to run_target; run-ahead is bounded by the
     * lookahead_floor_ + max_lookahead_cycles synthetic dependency (refreshed
     * lazily on the slow path). Direct MPSC lanes need no epoch-end flush.
     * Selected only when allMultiProducerPortsHaveProgress_() and
     * max_lookahead_cycles > 0; otherwise initialization selects Sequential.
     */
    uint64_t executeRunEpochFree_(uint64_t total_cycles);
    uint64_t executeRunEpochFreeDynamic_(uint64_t total_cycles);

    /// Completed cycles reported by a run-spanning worker launch. When a unit
    /// requests termination on a nonzero cluster, cluster 0 may lag behind the
    /// terminating tick; account for the request cycle so runUntilTermination()
    /// advances current_cycle_ far enough to cover the observed stop point.
    uint64_t completedCyclesForRun_(uint64_t run_start, uint64_t run_target) const noexcept;

    /// True iff every direct MPSC lane has a resolved producer-progress source.
    /// Precondition for executeRunEpochFree_ (see enable_epoch_free_lookahead).
    bool allMultiProducerPortsHaveProgress_() const noexcept;

    /// Grow registered lock-free buffers where the model declares enough edge
    /// rate to prove epoch-free headroom. Returns the number of connections
    /// whose reverse space dependency cannot be proven, which vetoes
    /// epoch-free instead of changing queue semantics.
    size_t prepareEpochFreeHeadroom_();

    /// Minimum cross-thread queue headroom over all connections. SIZE_MAX means
    /// no bounded cross-thread ring constrains epoch-free run-ahead.
    size_t crossThreadHeadroomLimit_() const noexcept;

    /// True when every finite cross-thread queue has a provable capacity
    /// dependency after headroom preparation, and those dependencies do not
    /// introduce a zero-delay cluster cycle. Zero-slack cycles fall back to
    /// sequential execution because epoch-free lookahead has no cluster that
    /// can legally make the first tick.
    bool crossThreadHeadroomAllowsEpochFree_() const noexcept;

    /// Compatibility helper used by logs/tests.
    bool crossThreadHeadroomFits_(uint64_t max_lookahead) const noexcept;

    /// Per-thread run body. Spin-waits on cross-thread progress atomics
    /// and exits via stop_token on termination or exception.
    void executeThreadRun_(size_t thread_idx, uint64_t end_cycle,
                           stdexec::inplace_stop_token token);
    void executeThreadRunWithPeriodicCounters_(size_t thread_idx, uint64_t end_cycle,
                                               uint64_t run_start, uint64_t period,
                                               stdexec::inplace_stop_token token);
    template <bool PushPeriodicCounters>
    void executeThreadRunImpl_(size_t thread_idx, uint64_t end_cycle, uint64_t run_start,
                               uint64_t period, stdexec::inplace_stop_token token);
    void executeThreadRunDynamic_(size_t thread_idx, uint64_t end_cycle,
                                  stdexec::inplace_stop_token token);
    void executeThreadRunDynamicWithPeriodicCounters_(size_t thread_idx, uint64_t end_cycle,
                                                      uint64_t run_start, uint64_t period,
                                                      stdexec::inplace_stop_token token);
    template <bool PushPeriodicCounters>
    void executeThreadRunDynamicImpl_(size_t thread_idx, uint64_t end_cycle, uint64_t run_start,
                                      uint64_t period, stdexec::inplace_stop_token token);

    /// Monotonically raise lookahead_floor_ to the global-min completed cycle.
    /// Slow-path only (a stalled worker); see lookahead_floor_ for the contract.
    void refreshLookaheadFloor_();

    /// Test-only white-box access for test_lookahead_floor_progress.
    friend struct LookaheadFloorTestAccess;
    /// Test-only white-box access for test_predecessor_cycle_cache.
    friend struct PredecessorCycleCacheTestAccess;
    /// Test-only white-box access for transitive dependency pruning.
    friend struct TransitiveDependencyPruneTestAccess;
    /// Test-only safe-point migration used by the epoch-free differential
    /// harness. The seam is intentionally outside the worker hot path.
    friend struct EpochFreeDifferentialTestAccess;
    /// Test-only source-owner migration commit validation.
    friend struct DynamicMigrationTestAccess;
    /// Test-only transparent-broadcast fusion selection inspection.
    friend struct TransparentBroadcastFusionTestAccess;

    /// Move the cluster containing @p unit while all persistent workers are
    /// stopped between run() spans. Returns false when the request is invalid
    /// or the simulation is not at a legal epoch-free dynamic safe point.
    bool forceEpochFreeMigrationAtBoundary_(Unit* unit, size_t target_thread);

    /// Worker-private lower bounds for predecessor progress. Each cache lives
    /// for one worker invocation and needs no atomic synchronization of its own.
    /// ThreadProgress is release-published and never decreases, so a value read
    /// with acquire remains a valid lower bound for all later dependency checks
    /// on that worker. The extra slot is reserved for the synthetic lookahead
    /// floor dependency (pred_id == thread_progress_count_).
    struct alignas(64) WorkerPredecessorCycleCache {
        explicit WorkerPredecessorCycleCache(size_t num_clusters)
            : observed_cycles(num_clusters + 1, 0) {}

        uint64_t* data() noexcept { return observed_cycles.data(); }

        std::vector<uint64_t> observed_cycles;
    };

    /// Return a predecessor-progress lower bound sufficient for `needed` when
    /// possible. A cache hit deliberately does not load the remote atomic. On a
    /// miss, a real cluster dependency's acquire pairs with the predecessor's
    /// release publication and replaces the worker-local lower bound. The floor
    /// slot is only a gating hint and carries no model data. Cluster progress is
    /// monotonic, so dynamic ownership migration needs no cache invalidation.
    static uint64_t observePredecessorCycle_(const ResolvedDep& dep, uint64_t needed,
                                             uint64_t* observed_cycles) noexcept {
        uint64_t observed = observed_cycles[dep.pred_id];
        if (observed >= needed) return observed;

        observed = dep.progress_ptr->load(std::memory_order_acquire);
        observed_cycles[dep.pred_id] = observed;
        return observed;
    }

    bool clusterCanAdvance_(size_t cluster, uint64_t cycle, BlockedClusterInfo& blocker,
                            uint64_t* predecessor_cache) const;
    [[gnu::noinline]] bool refreshPredecessorMisses_(size_t cluster, uint64_t cycle,
                                                     BlockedClusterInfo& blocker,
                                                     uint64_t* predecessor_cache,
                                                     uint64_t refresh_mask) const;
    [[gnu::noinline]] bool clusterCanAdvanceScalarSlow_(size_t cluster, uint64_t cycle,
                                                        BlockedClusterInfo& blocker,
                                                        uint64_t* predecessor_cache) const;
    uint64_t computeIdleClusterTarget_(size_t cluster, uint64_t cycle, uint64_t end_cycle,
                                       uint64_t* predecessor_cache) const;
    [[gnu::noinline]] uint64_t computeIdleClusterTargetIfEnabled_(
        size_t cluster, uint64_t cycle, uint64_t end_cycle, uint64_t* predecessor_cache) const;
    void advanceClusterIdle_(size_t cluster, uint64_t delta);
    void recordClusterIdle_(size_t thread_idx, size_t cluster, uint64_t cycle, uint64_t delta,
                            SchedulerTimelineTrace::TimePoint begin,
                            SchedulerTimelineTrace::TimePoint end);
    std::string formatBlockerDetail_(const BlockedClusterInfo& blocker) const;
    struct ThreadTraceCpuPoint {
        uint64_t cpu_time_ns = 0;
        uint32_t tid = 0;
        int cpu = -1;
    };
    static ThreadTraceCpuPoint threadTraceCpuPoint_() noexcept;
    void recordUnitDuration_(size_t thread_idx, std::string_view category, std::string_view name,
                             uint64_t cycle, SchedulerTimelineTrace::TimePoint begin,
                             SchedulerTimelineTrace::TimePoint end, std::string_view detail,
                             bool include_thread_cpu_time, ThreadTraceCpuPoint cpu_begin,
                             ThreadTraceCpuPoint cpu_end);
    void executeClusterOneCycle_(size_t thread_idx, size_t cluster, uint64_t cycle,
                                 bool trace_units);

    void initDynamicMigrationRuntime_();
    void rebuildThreadUnitsFromClusterOwners_();
    bool maybeRequestEpochFreeMigration_(uint64_t cycle);
    void serviceEpochFreeMigration_(size_t worker_thread);
    void clearDynamicMigrationRequest_();
    void recordDynamicWaitSample_(size_t thread_idx, const BlockedClusterInfo& blocker,
                                  uint64_t wait_ns);
    void resetDynamicSchedulerMarkers_();
    void recordDynamicSchedulerMarker_(std::string name, uint64_t cycle, std::string detail);
    void flushDynamicSchedulerMarkers_();

    /**
     * Collect MPSC-mode InPorts from `connections_`. Iterates in
     * registration order, filters by MPSC flag, registers each on its
     * InPort (which sorts by conn_id internally), and maintains
     * `multi_producer_ports_` deduplicated in first-seen order.
     */
    void collectMultiProducerPorts_();

    /// Rethrow the current exception as TickException. Wraps non-
    /// TickException throwables with unknown unit context.
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] static void throwTickException();

    TickSimulationConfig config_;
    uint64_t current_cycle_;
    bool initialized_;
    ExecutionMode execution_mode_ = ExecutionMode::Sequential;
    bool has_tight_connections_ = false;
    bool has_tight_inter_cluster_ = false;
    bool has_zero_delay_cross_thread_cycle_ = false;
    bool parallel_beneficial_ = false;
    bool parallel_fallback_warned_ = false;
    std::string parallel_fallback_reason_;

    /// Count of runEpochFree() invocations.
    /// Introspection only (see epochFreeRunCount()).
    uint64_t epoch_free_run_count_ = 0;

    TerminationController termination_ctrl_;

    ::exec::static_thread_pool pool_;

    std::vector<std::unique_ptr<TickableUnit>> units_;
    std::vector<TickableUnit*> unit_ptrs_;

    std::unique_ptr<std::atomic<uint64_t>[]> unit_progress_;

    /// Pointers only — Connections are owned by their OutPort.
    std::vector<ConnectionBase*> connections_;
    size_t transparent_broadcast_fusion_connection_count_ = 0;

    /// Flat, deduplicated list of InPorts with at least one MPSC ingress lane.
    /// Used for initialization-time progress coverage and overflow diagnostics;
    /// bounded FIFO preparation is registered directly on the owning Unit.
    std::vector<IMultiProducerPort*> multi_producer_ports_;

    DependencyGraph dep_graph_;
    TightCouplingResult clusters_;
    std::vector<size_t> unit_to_cluster_;
    std::vector<size_t> cluster_to_thread_;
    std::vector<size_t> counter_owner_ids_;
    std::vector<std::vector<size_t>> thread_units_;

    std::vector<std::vector<ThreadCrossDep>> thread_cross_deps_temp_;
    /// Directed predecessor -> dependent graph used by runtime rebalance.
    /// Includes retained progress and finite-headroom scheduling relationships.
    std::vector<std::vector<PartitionInput::EdgeInfo>> dynamic_rebalance_adjacency_;
    ThreadProgress* thread_progress_array_ = nullptr;
    size_t thread_progress_count_ = 0;
    std::vector<std::vector<ResolvedDep>> thread_resolved_deps_;

    /// Root of the activity-summary hierarchy. A false acquire load proves
    /// that every Unit is still always active; runtime opt-in publishes true.
    detail::ActivitySchedulingState any_activity_scheduling_;

    /// Cache-line-isolated, monotone per-cluster idle-scan hints. A stale false
    /// only disables the bulk-idle optimization and is behavior-safe.
    std::unique_ptr<detail::ActivitySchedulingState[]> cluster_activity_scheduling_;

    /// Global floor for max_lookahead_cycles enforcement: clusters gate at
    /// floor + max_lookahead_cycles.  Memory-order contract: relaxed monotone
    /// CAS write (refreshLookaheadFloor_), acquire read (clusterCanAdvance_).
    /// A gating HINT only — never carries data, and a stale value is always
    /// safe (blocks marginally sooner); real data sync uses ThreadProgress.
    alignas(64) std::atomic<uint64_t> lookahead_floor_{0};
    std::vector<std::vector<TickableUnit*>> thread_unit_ptrs_;
    std::vector<std::vector<size_t>> thread_clusters_;
    std::vector<std::vector<TickableUnit*>> cluster_unit_ptrs_;
    std::vector<std::vector<SchedulerTimelineTrace::TimePoint>> thread_trace_points_;
    std::vector<std::vector<ThreadTraceCpuPoint>> thread_trace_cpu_points_;

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
    std::vector<uint64_t> dynamic_cluster_last_tick_sample_cycle_;
    std::vector<uint64_t> dynamic_cluster_last_migration_cycle_;
    std::vector<size_t> dynamic_cluster_last_source_thread_;
    std::vector<size_t> dynamic_cluster_last_target_thread_;
    std::atomic<uint64_t> cluster_assignment_generation_{0};
    size_t dynamic_runtime_cluster_count_ = 0;
    size_t dynamic_runtime_thread_count_ = 0;
    alignas(64) std::atomic<uint64_t> next_dynamic_rebalance_check_cycle_{0};
    alignas(64) std::atomic<bool> epoch_free_dynamic_runtime_active_{false};
    alignas(64) std::atomic_flag dynamic_planner_busy_ = ATOMIC_FLAG_INIT;

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
    PlatformMetrics platform_metrics_{};
    std::vector<double> unit_costs_;

    std::vector<double> precomputed_unit_costs_;
    PlatformMetrics precomputed_platform_metrics_{};
    bool has_precomputed_costs_ = false;
    bool force_stable_connection_queues_ = false;
    size_t transparent_broadcast_connection_count_ = 0;

    uint64_t cycles_since_last_actual_rebalance_ = 0;
    uint64_t rebalance_count_ = 0;
};

}  // namespace chronon::sender
