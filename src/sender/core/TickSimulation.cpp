// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Core TickSimulation methods: initialization, run entry points,
/// sequential/parallel dispatch, dependency analysis, and utilities.

#include "TickSimulation.hpp"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

namespace chronon::sender {

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

void TickSimulation::initialize() {
    if (initialized_) return;

    buildDependencyGraph();
    validateNoZeroDelayCycles_();

    // Topologically reorder unit_ptrs_ so zero-delay producers tick before
    // same-cycle consumers in the per-cycle loop. Full-graph SCC condensation
    // handles registered feedback; creation order is a tie-break only.
    reorderUnitsTopologically_();
    buildDependencyGraph();

    // Remap pre-computed costs to the new index order.
    // setPrecomputedUnitCosts() stores costs in creation order (by unit
    // id), so we permute the cost vector to match the reordered indices.
    if (has_precomputed_costs_ && precomputed_unit_costs_.size() == unit_ptrs_.size()) {
        std::vector<double> remapped(unit_ptrs_.size());
        for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
            uint32_t orig_idx = unit_ptrs_[i]->id();
            if (orig_idx < precomputed_unit_costs_.size()) {
                remapped[i] = precomputed_unit_costs_[orig_idx];
            }
        }
        precomputed_unit_costs_ = std::move(remapped);
    }

    if (config_.enable_lookahead && !unit_ptrs_.empty()) {
        analysis_ = CycleAnalyzer::analyze(dep_graph_);
    }

    // With tight (delay=0) connections, lookahead degenerates to per-cycle
    // and barrier mode has less sync overhead.
    has_tight_connections_ = hasTightConnections();

    buildPredecessorCache();

    auto& obs_mgr = observe::ObservationManager::instance();
    if (obs_mgr.isEnabled()) {
        observe_ctx_ =
            obs_mgr.createContextForUnit("simulation", [this]() { return current_cycle_; }, 0);
        if (observe_ctx_) {
            observe_ctx_->enableCategory(observe::category::LOG_INFO);
        }
    }

    termination_ctrl_.setStopSource(&*stop_source_);

    if (obs_mgr.isEnabled()) {
        auto* backend = obs_mgr.backend();
        if (backend) {
            backend->setStopToken(stop_source_->get_token());
        }
    }

    // Initialize units BEFORE selectExecutionMode_() so tick() is callable
    // during cost profiling.
    for (auto& unit : units_) {
        unit->setTerminationController(&termination_ctrl_);
        unit->initialize();
    }

    selectExecutionMode_();
    traceExecutionMode_();

    // Any connection still carrying a thread_queue_id after queue
    // optimization is genuinely MPSC; record its InPort for the per-cycle
    // arbiter.
    collectMPSCInPorts_();

    const size_t progress_size = units_.size();
    unit_progress_ = std::make_unique<std::atomic<uint64_t>[]>(progress_size);
    for (size_t i = 0; i < progress_size; ++i) {
        unit_progress_[i].store(unit_ptrs_[i]->localCycle(), std::memory_order_relaxed);
    }

    // Progress-based sync is Lookahead-only; Sequential / Barrier ignore
    // thread_progress_array_.
    if (config_.enable_lookahead && shouldUseParallelExecution_() && has_thread_assignment_ &&
        !thread_units_.empty()) {
        initProgressSync();
        installMPSCProducerProgress_();
    }

    timeline_trace_.start(thread_units_, unit_ptrs_);
    initTimelineTraceScratch_();

    initialized_ = true;
}

// ---------------------------------------------------------------------------
// Run entry points
// ---------------------------------------------------------------------------

uint64_t TickSimulation::run(uint64_t num_cycles) {
    if (!initialized_) {
        initialize();
    }

    if (units_.empty()) {
        return 0;
    }

    uint64_t executed = 0;

    if (shouldUseParallelExecution_()) {
        executed = runParallel(num_cycles);
    } else {
        executed = runSequential(num_cycles);
    }

    current_cycle_ += executed;

    // Periodic counter dumps are intentionally NOT done here because run()
    // executes the whole batch, with no observable intermediate boundary.
    // Use runUntilTermination() for periodic dumps.
    return executed;
}

uint64_t TickSimulation::runUntilTermination(uint64_t max_cycles) {
    if (!initialized_) {
        initialize();
    }

    if (units_.empty()) {
        return 0;
    }

    uint64_t executed = 0;

    const bool use_parallel = shouldUseParallelExecution_();

    auto& obs_mgr = observe::ObservationManager::instance();
    const bool do_periodic_dump = obs_mgr.isEnabled() && obs_mgr.periodicDumpCycles() > 0;
    const uint64_t dump_period = do_periodic_dump ? obs_mgr.periodicDumpCycles() : 0;
    uint64_t next_dump_cycle =
        do_periodic_dump ? ((current_cycle_ / dump_period) + 1) * dump_period : UINT64_MAX;

    while (executed < max_cycles) {
        if (termination_ctrl_.isTerminationRequested()) {
            break;
        }

        const bool use_epoch_free_chunk = use_parallel && epochFreeLookaheadEligible_();
        uint64_t step_cycles = max_cycles - executed;
        if (do_periodic_dump && next_dump_cycle > current_cycle_) {
            step_cycles = std::min(step_cycles, next_dump_cycle - current_cycle_);
        }
        if (!use_epoch_free_chunk) {
            step_cycles = std::min(step_cycles, config_.epoch_size);
        }
        uint64_t epoch_executed = 0;

        if (use_parallel) {
            if (config_.enable_dynamic_rebalance && !use_epoch_free_chunk) {
                // Dynamic rebalance is epoch-boundary based, so keep the
                // explicit epoch driver when it is enabled.
                epoch_executed = runParallelEpoch(step_cycles, 0);
            } else {
                epoch_executed = runParallel(step_cycles);
            }
        } else {
            epoch_executed = runSequentialEpoch(step_cycles);
        }

        executed += epoch_executed;
        current_cycle_ += epoch_executed;

        while (do_periodic_dump && next_dump_cycle <= current_cycle_) {
            obs_mgr.dumpCounterSnapshots(next_dump_cycle);
            next_dump_cycle += dump_period;
        }

        if (config_.enable_dynamic_rebalance && use_parallel && !use_epoch_free_chunk) {
            cycles_since_last_rebalance_ += epoch_executed;
            cycles_since_last_actual_rebalance_ += epoch_executed;
            if (cycles_since_last_rebalance_ >= config_.rebalance_check_interval_cycles) {
                cycles_since_last_rebalance_ = 0;
                bool cooldown_ok =
                    (config_.rebalance_cooldown_cycles == 0) ||
                    (cycles_since_last_actual_rebalance_ >= config_.rebalance_cooldown_cycles);
                if (cooldown_ok && shouldRebalance_()) {
                    SchedulerTimelineTrace::TimePoint rebalance_begin{};
                    if (timeline_trace_.enabled()) {
                        rebalance_begin = SchedulerTimelineTrace::Clock::now();
                    }
                    if (performRebalance_()) {
                        cycles_since_last_actual_rebalance_ = 0;
                        if (timeline_trace_.enabled()) {
                            auto rebalance_end = SchedulerTimelineTrace::Clock::now();
                            timeline_trace_.recordDuration(timeline_trace_.schedulerStream(),
                                                           "scheduler", "dynamic rebalance",
                                                           current_cycle_, rebalance_begin,
                                                           rebalance_end, last_rebalance_detail_);
                        }
                    }
                }
            }
        }

        if (epoch_executed < step_cycles) {
            break;
        }
    }

    if (!termination_ctrl_.isTerminationRequested() && executed >= max_cycles) {
        termination_ctrl_.requestTermination(TerminationReason::MaxCyclesReached, 0, current_cycle_,
                                             "", "Maximum cycle limit reached");
    }

    return executed;
}

// ---------------------------------------------------------------------------
// Execution mode selection
// ---------------------------------------------------------------------------

void TickSimulation::resolveSolver_() {
    switch (config_.partition_solver) {
        case TickSimulationConfig::PartitionSolverType::Weighted:
            partition_solver_ = &WeightedPartitioner::partition;
            break;
        case TickSimulationConfig::PartitionSolverType::SA:
            partition_solver_ = &SimulatedAnnealingPartitioner::partition;
            break;
    }
    // SA v1 is validated for initial partitioning only; dynamic rebalance
    // stays on Weighted until SA has end-to-end rebalance coverage.
    rebalance_solver_ = &WeightedPartitioner::partition;
}

void TickSimulation::selectExecutionMode_() {
    execution_mode_ = ExecutionMode::Sequential;
    parallel_beneficial_ = false;

    if (!(config_.enable_parallel && units_.size() > 1)) {
        optimizeAllQueuesForSingleThread();
        return;
    }

    if (config_.enable_weighted_partitioning && units_.size() >= 4) {
        if (has_precomputed_costs_) {
            assignThreadsFromPrecomputedCosts_();
        } else {
            assignThreadsDeterministic_();
        }
        parallel_beneficial_ = parallelBeneficialWeighted_();
    } else if (has_tight_connections_) {
        buildClusterAffinity();
    } else {
        buildThreadAssignment();
        parallel_beneficial_ = parallelBeneficialFromThreadAssignment_();
    }

    if (parallel_beneficial_) {
        execution_mode_ = ExecutionMode::Parallel;
    } else {
        // Even when parallel was requested, fall back to the single-thread
        // queue/counter optimizations.
        optimizeAllQueuesForSingleThread();
    }
}

void TickSimulation::traceExecutionMode_() {
    if (!observe_ctx_) {
        return;
    }
    observe::log_info<
        "Execution policy: mode={} (parallel_requested={}, tight_connections={}, "
        "parallel_beneficial={})">(
        observe_ctx_, shouldUseParallelExecution_() ? "parallel" : "sequential",
        config_.enable_parallel ? "yes" : "no", has_tight_connections_ ? "yes" : "no",
        parallel_beneficial_ ? "yes" : "no");
}

// ---------------------------------------------------------------------------
// Sequential / parallel dispatch
// ---------------------------------------------------------------------------

uint64_t TickSimulation::runSequential(uint64_t num_cycles) {
    // In single-thread mode, per-cycle execution avoids lookahead
    // bookkeeping overhead without changing execution semantics.
    try {
        for (uint64_t i = 0; i < num_cycles; ++i) {
            for (auto* unit : unit_ptrs_) {
                executeUnitCycle_(unit, unit->localCycle());
            }
            arbitrateAllMPSCPorts_();
        }
    } catch (...) {
        throwTickException();
    }
    return num_cycles;
}

uint64_t TickSimulation::runSequentialEpoch(uint64_t epoch_cycles) {
    try {
        for (uint64_t i = 0; i < epoch_cycles; ++i) {
            for (auto* unit : unit_ptrs_) {
                executeUnitCycle_(unit, unit->localCycle());
            }
            arbitrateAllMPSCPorts_();
        }
    } catch (...) {
        throwTickException();
    }

    return epoch_cycles;
}

uint64_t TickSimulation::runParallelEpoch(uint64_t epoch_cycles, uint64_t /*executed_offset*/) {
    // Lookahead is usable as long as no tight edges cross cluster
    // boundaries. Intra-cluster tight edges are safe because units inside
    // a cluster run sequentially on the same thread.
    const bool use_lookahead = config_.enable_lookahead && !has_tight_inter_cluster_;

    if (use_lookahead && thread_progress_count_ > 0) {
        executeEpochProgressBased(epoch_cycles);
    } else {
        auto sched = pool_.get_scheduler();
        executeEpochBarrier(sched, epoch_cycles);
    }

    return epoch_cycles;
}

bool TickSimulation::persistentLookaheadEligible_() const {
    const bool use_lookahead = config_.enable_lookahead && !has_tight_inter_cluster_;
    return use_lookahead && thread_progress_count_ > 0 &&
           thread_units_.size() <= pool_.available_parallelism();
}

bool TickSimulation::epochFreeLookaheadEligible_() const {
    return persistentLookaheadEligible_() && config_.enable_epoch_free_lookahead &&
           config_.max_lookahead_cycles > 0 && allMPSCPortsHaveConnProgress_() &&
           crossThreadHeadroomFits_(config_.max_lookahead_cycles);
}

uint64_t TickSimulation::runParallel(uint64_t num_cycles) {
    // Persistent-worker fast path (see executeRunProgressBased): only safe with a
    // fixed thread layout. runUntilTermination() reaches this path for chunks
    // that do not need dynamic-rebalance epoch boundaries.
    const bool can_persist = persistentLookaheadEligible_();
    if (can_persist) {
        // Epoch-free (A/B): drop the per-epoch barrier when the lookahead floor
        // can be the sole run-ahead cap (max_lookahead_cycles > 0) and every
        // MPSC port is fully covered by per-connection progress (so no port
        // needs the per-epoch central flush). Otherwise keep the barrier path.
        const bool epoch_free = epochFreeLookaheadEligible_();
        if (config_.enable_epoch_free_lookahead && !epoch_free && observe_ctx_) {
            observe::log_info<
                "epoch-free lookahead requested but vetoed (max_lookahead={}, "
                "mpsc_progress_full={}, headroom_fits={}); "
                "using barrier path">(observe_ctx_, config_.max_lookahead_cycles,
                                      allMPSCPortsHaveConnProgress_(),
                                      crossThreadHeadroomFits_(config_.max_lookahead_cycles));
        }
        if (epoch_free) {
            ++epoch_free_run_count_;
            return executeRunEpochFree_(num_cycles);
        }
        if (!config_.enable_dynamic_rebalance) {
            return executeRunProgressBased(num_cycles);
        }
    }

    uint64_t executed = 0;
    while (executed < num_cycles) {
        uint64_t epoch_cycles = std::min(config_.epoch_size, num_cycles - executed);
        runParallelEpoch(epoch_cycles, executed);
        executed += epoch_cycles;
    }
    return executed;
}

void TickSimulation::executeUnitToTarget(size_t unit_idx, uint64_t target_cycle) {
    auto* unit = unit_ptrs_[unit_idx];
    while (unit->localCycle() < target_cycle) {
        executeUnitCycle_(unit, unit->localCycle());
    }
    // No try-catch: stdexec propagates exceptions natively in parallel
    // paths; sequential paths use throwTickException.
}

uint64_t TickSimulation::computeSafeBoundary(size_t unit_idx, uint64_t epoch_end) const {
    if (!config_.enable_lookahead) {
        return epoch_end;
    }

    uint64_t min_safe = epoch_end;

    // DIRECT predecessors only; transitive constraints are already enforced
    // by intermediate units. Using Floyd-Warshall distances would
    // over-constrain the schedule.
    for (const auto& [pred_idx, delay] : predecessor_cache_[unit_idx]) {
        uint64_t pred_cycle = unit_progress_[pred_idx].load(std::memory_order_acquire);
        uint64_t safe = pred_cycle + delay;
        min_safe = std::min(min_safe, safe);
    }

    uint64_t current = unit_ptrs_[unit_idx]->localCycle();
    return std::min(min_safe, current + config_.max_lookahead_cycles);
}

// ---------------------------------------------------------------------------
// Dependency analysis and topological ordering
// ---------------------------------------------------------------------------

void TickSimulation::buildDependencyGraph() {
    std::vector<Unit*> unit_as_base;
    unit_as_base.reserve(unit_ptrs_.size());
    for (auto* unit : unit_ptrs_) {
        unit_as_base.push_back(static_cast<Unit*>(unit));
    }

    dep_graph_.build(unit_as_base, connections_);
}

void TickSimulation::buildPredecessorCache() {
    predecessor_cache_.clear();
    predecessor_cache_.resize(units_.size());

    std::unordered_map<Unit*, size_t> unit_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        Unit* unit = static_cast<Unit*>(unit_ptrs_[i]);
        for (const auto& [pred, delay] : dep_graph_.predecessors(unit)) {
            auto it = unit_to_idx.find(pred);
            if (it != unit_to_idx.end()) {
                predecessor_cache_[i].emplace_back(it->second, delay);
            }
        }
    }
}

void TickSimulation::reorderUnitsTopologically_() {
    const auto* graph = dep_graph_.graph();
    if (!graph || graph->numNodes() <= 1) {
        return;
    }

    const size_t n = graph->numNodes();

    auto sccs = tarjanSCC(*graph);

    size_t num_sccs = sccs.numComponents();
    DirectedGraph condensed(num_sccs);
    for (size_t u = 0; u < n; ++u) {
        for (const auto& e : graph->neighbors(u)) {
            size_t src_scc = sccs.component[u];
            size_t dst_scc = sccs.component[e.to];
            if (src_scc != dst_scc) {
                condensed.addEdge(src_scc, dst_scc, e.weight);
            }
        }
    }

    auto topo = topologicalSort(condensed);

    std::vector<TickableUnit*> new_order;
    new_order.reserve(n);

    for (size_t scc_idx : topo.order) {
        auto& members = sccs.components[scc_idx];
        std::sort(members.begin(), members.end(),
                  [this](size_t a, size_t b) { return unit_ptrs_[a]->id() < unit_ptrs_[b]->id(); });

        // Full-graph SCCs can contain registered feedback, e.g. A->B delay=0
        // and B->A delay=1. Preserve the same-cycle direction by ordering the
        // SCC with its acyclic zero-delay subgraph, then use creation order as
        // the stable tie-break for unrelated members.
        if (members.size() > 1) {
            std::vector<size_t> local_index(n, SIZE_MAX);
            for (size_t i = 0; i < members.size(); ++i) {
                local_index[members[i]] = i;
            }

            DirectedGraph zero_delay_subgraph(members.size());
            for (size_t i = 0; i < members.size(); ++i) {
                for (const auto& e : graph->neighbors(members[i])) {
                    if (e.weight == 0 && e.to < local_index.size() &&
                        local_index[e.to] != SIZE_MAX) {
                        zero_delay_subgraph.addEdge(i, local_index[e.to], 0);
                    }
                }
            }

            auto zero_topo = topologicalSort(zero_delay_subgraph);
            if (!zero_topo.has_cycle) {
                std::vector<size_t> ordered_members;
                ordered_members.reserve(members.size());
                for (size_t local : zero_topo.order) {
                    ordered_members.push_back(members[local]);
                }
                members = std::move(ordered_members);
            }
        }

        for (size_t node_idx : members) {
            new_order.push_back(unit_ptrs_[node_idx]);
        }
    }

    unit_ptrs_ = std::move(new_order);
}

bool TickSimulation::hasTightConnections() const {
    for (const auto* conn : connections_) {
        if (conn->delay() == 0) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

void TickSimulation::throwTickException() {
    try {
        throw;  // Inspect the current exception's type.
    } catch (const TickException&) {
        throw;
    } catch (const std::exception& e) {
        throw TickException("unknown", 0, e.what());
    } catch (...) {
        throw TickException("unknown", 0, "unknown exception");
    }
}

std::string TickSimulation::buildUnitNameList_(const std::vector<size_t>& unit_indices) const {
    std::string result;
    for (size_t i = 0; i < unit_indices.size(); ++i) {
        if (i > 0) result += ", ";
        result += unit_ptrs_[unit_indices[i]]->name();
    }
    return result;
}

bool TickSimulation::parallelBeneficialFromThreadAssignment_() const noexcept {
    size_t max_thread_units = 0;
    size_t active_threads = 0;
    for (const auto& tu : thread_units_) {
        max_thread_units = std::max(max_thread_units, tu.size());
        if (!tu.empty()) {
            active_threads++;
        }
    }

    const bool balanced = (max_thread_units * 2 <= units_.size());
    const bool enough_work_per_active_thread =
        active_threads > 0 && (units_.size() >= active_threads * 3);
    return balanced && enough_work_per_active_thread;
}

PartitionResult TickSimulation::runPartitionSolver_(const PartitionInput& input) {
    return partition_solver_(input);
}

PartitionResult TickSimulation::runRebalanceSolver_(const PartitionInput& input) {
    return rebalance_solver_(input);
}

// ---------------------------------------------------------------------------
// Queue optimization
// ---------------------------------------------------------------------------

void TickSimulation::optimizeAllQueuesForSingleThread() {
    // Detect multi-producer fan-in even in single-thread execution -- ports
    // fed by multiple Connections still need MPSC-style staging +
    // arbitration so admission order is deterministic and matches the
    // multi-thread scheduler.
    std::unordered_map<void*, std::vector<ConnectionBase*>> port_to_connections;
    for (auto* conn : connections_) {
        void* key = conn->destPortPtr();
        port_to_connections[key].push_back(conn);
    }

    for (auto& [dst_port_key, conns] : port_to_connections) {
        (void)dst_port_key;
        if (conns.empty()) continue;
        if (conns.size() == 1) {
            conns[0]->optimizeForSameThread();
            continue;
        }
        // Multi-producer fan-in: route through MPSC + arbiter so the
        // cycle-boundary admission order is determined by conn_id, not by
        // tick-loop producer ordering.
        for (auto* conn : conns) {
            conn->optimizeForMPSC();
        }
        // Synthesize a pseudo thread_id per connection (its conn_id + 1)
        // so each connection gets its own per-thread ring, matching the
        // multi-thread case.
        for (auto* conn : conns) {
            const size_t pseudo_thread_id = static_cast<size_t>(conn->connId()) + 1;
            const size_t queue_id = conn->registerProducerThread(pseudo_thread_id);
            if (queue_id != SIZE_MAX) {
                conn->setThreadQueueId(queue_id);
            }
        }
    }

    // Safe because localCycle() is single-threaded in this mode;
    // cross-thread visibility for lookahead is handled by unit_progress_.
    for (auto* unit : unit_ptrs_) {
        unit->useFastCycleCounter();
    }

    if (observe_ctx_) {
        observe::log_info<"Optimized {} connections and {} units for single-thread execution">(
            observe_ctx_, connections_.size(), unit_ptrs_.size());
    }
}

void TickSimulation::optimizeConnectionQueuesForThreads() {
    if (clusters_.numClusters() == 0) {
        return;
    }

    if (config_.enable_dynamic_rebalance) {
        optimizeConnectionQueuesForDynamicRebalance_();
        return;
    }

    std::unordered_map<Unit*, size_t> unit_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    auto getThreadForUnit = [&](Unit* unit) -> size_t {
        auto it = unit_to_idx.find(unit);
        if (it == unit_to_idx.end()) return SIZE_MAX;
        size_t cluster = unit_to_cluster_[it->second];
        if (cluster >= cluster_to_thread_.size()) return SIZE_MAX;
        return cluster_to_thread_[cluster];
    };

    // Group by destination PORT (not unit): each InPort is analyzed
    // separately to determine SPSC vs MPSC.
    std::unordered_map<void*, std::vector<ConnectionBase*>> port_to_connections;
    for (auto* conn : connections_) {
        void* key = conn->destPortPtr();
        port_to_connections[key].push_back(conn);
    }

    size_t same_thread_count = 0;
    size_t spsc_count = 0;
    size_t mpsc_count = 0;

    for (auto& [dst_port_key, conns] : port_to_connections) {
        (void)dst_port_key;
        if (conns.empty()) continue;

        Unit* dst = conns[0]->destination();
        size_t dst_thread = getThreadForUnit(dst);
        if (dst_thread == SIZE_MAX) continue;

        std::set<size_t> src_threads;
        for (auto* conn : conns) {
            Unit* src = conn->source();
            size_t src_thread = getThreadForUnit(src);
            if (src_thread != SIZE_MAX) {
                src_threads.insert(src_thread);
            }
        }

        if (src_threads.empty()) continue;

        bool all_same_thread = (src_threads.size() == 1 && *src_threads.begin() == dst_thread);
        bool single_source_thread = (src_threads.size() == 1);

        if (all_same_thread) {
            if (conns.size() == 1) {
                conns[0]->optimizeForSameThread();
                same_thread_count += 1;
            } else {
                // Multi-producer fan-in (even on one thread): route through
                // MPSC so the cycle-boundary arbiter produces a topology-
                // stable admission order matching the multi-thread case
                // byte-for-byte.
                for (auto* conn : conns) {
                    conn->optimizeForMPSC();
                }
                for (auto* conn : conns) {
                    const size_t pseudo_thread_id = static_cast<size_t>(conn->connId()) + 1;
                    const size_t queue_id = conn->registerProducerThread(pseudo_thread_id);
                    if (queue_id != SIZE_MAX) {
                        conn->setThreadQueueId(queue_id);
                    }
                }
                mpsc_count += conns.size();
            }
        } else if (single_source_thread) {
            if (conns.size() == 1) {
                conns[0]->optimizeForSPSC();
                spsc_count += 1;
            } else {
                // Cross-thread, single producer thread, multiple
                // Connections: producer units race for the queue, so use
                // MPSC + one queue per Connection.
                for (auto* conn : conns) {
                    conn->optimizeForMPSC();
                }
                for (auto* conn : conns) {
                    const size_t pseudo_thread_id = static_cast<size_t>(conn->connId()) + 1;
                    const size_t queue_id = conn->registerProducerThread(pseudo_thread_id);
                    if (queue_id != SIZE_MAX) {
                        conn->setThreadQueueId(queue_id);
                    }
                }
                mpsc_count += conns.size();
            }
        } else {
            // Cross-thread MPSC: keying each Connection on a distinct
            // pseudo thread id (conn_id + 1) makes admission order a
            // function of topology, not runtime thread->queue placement.
            for (auto* conn : conns) {
                conn->optimizeForMPSC();
            }
            for (auto* conn : conns) {
                const size_t pseudo_thread_id = static_cast<size_t>(conn->connId()) + 1;
                const size_t queue_id = conn->registerProducerThread(pseudo_thread_id);
                if (queue_id != SIZE_MAX) {
                    conn->setThreadQueueId(queue_id);
                }
            }
            mpsc_count += conns.size();
        }
    }

    if (observe_ctx_) {
        observe::log_info<"Queue optimization: {} same-thread, {} SPSC, {} MPSC connections">(
            observe_ctx_, same_thread_count, spsc_count, mpsc_count);
    }
}

void TickSimulation::optimizeConnectionQueuesForDynamicRebalance_() {
    std::unordered_map<void*, std::vector<ConnectionBase*>> port_to_connections;
    for (auto* conn : connections_) {
        void* key = conn->destPortPtr();
        port_to_connections[key].push_back(conn);
    }

    size_t spsc_count = 0;
    size_t mpsc_count = 0;

    for (auto& [dst_port_key, conns] : port_to_connections) {
        (void)dst_port_key;
        if (conns.empty()) continue;

        if (conns.size() == 1) {
            conns[0]->optimizeForSPSC();
            spsc_count += 1;
            continue;
        }

        for (auto* conn : conns) {
            conn->optimizeForMPSC();
        }
        for (auto* conn : conns) {
            const size_t pseudo_thread_id = static_cast<size_t>(conn->connId()) + 1;
            const size_t queue_id = conn->registerProducerThread(pseudo_thread_id);
            if (queue_id != SIZE_MAX) {
                conn->setThreadQueueId(queue_id);
            }
        }
        mpsc_count += conns.size();
    }

    if (observe_ctx_) {
        observe::log_info<"Queue optimization: dynamic-stable, {} SPSC, {} MPSC connections">(
            observe_ctx_, spsc_count, mpsc_count);
    }
}

// ---------------------------------------------------------------------------
// Thread assignment (topology-aware, no cost profiling)
// ---------------------------------------------------------------------------

void TickSimulation::buildThreadAssignment() {
    size_t num_threads = normalizeThreadCount(config_.num_threads);
    config_.num_threads = num_threads;

    clusters_.cluster_id.resize(unit_ptrs_.size());
    clusters_.clusters.resize(unit_ptrs_.size());
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        clusters_.cluster_id[i] = i;
        clusters_.clusters[i].assign(1, i);
    }
    unit_to_cluster_ = clusters_.cluster_id;

    // Topology-aware thread assignment minimizes cross-thread edges by
    // weighting adjacency as 1/delay (higher = tighter coupling).
    cluster_to_thread_.resize(unit_ptrs_.size());
    thread_units_.resize(num_threads);

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    std::vector<std::vector<std::pair<size_t, double>>> adj(unit_ptrs_.size());
    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;
        auto si = unit_ptr_to_idx.find(src);
        auto di = unit_ptr_to_idx.find(dst);
        if (si == unit_ptr_to_idx.end() || di == unit_ptr_to_idx.end()) continue;
        double weight = 1.0 / std::max(conn->delay(), 1u);
        adj[si->second].push_back({di->second, weight});
        adj[di->second].push_back({si->second, weight});
    }

    std::vector<bool> assigned(unit_ptrs_.size(), false);
    std::vector<size_t> thread_load(num_threads, 0);
    size_t max_per_thread = (unit_ptrs_.size() + num_threads - 1) / num_threads;

    for (size_t round = 0; round < unit_ptrs_.size(); ++round) {
        size_t best_unit = SIZE_MAX;
        size_t best_thread = 0;
        double best_score = -1.0;

        for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
            if (assigned[i]) continue;

            std::vector<double> thread_score(num_threads, 0.0);
            for (auto& [neighbor, weight] : adj[i]) {
                if (assigned[neighbor]) {
                    thread_score[cluster_to_thread_[neighbor]] += weight;
                }
            }

            for (size_t t = 0; t < num_threads; ++t) {
                if (thread_load[t] >= max_per_thread) continue;
                double score = thread_score[t];
                // 0.001*free-slots tiebreak favours less-loaded threads.
                score += 0.001 * (max_per_thread - thread_load[t]);
                if (score > best_score) {
                    best_score = score;
                    best_unit = i;
                    best_thread = t;
                }
            }
        }

        if (best_unit == SIZE_MAX) break;

        assigned[best_unit] = true;
        cluster_to_thread_[best_unit] = best_thread;
        thread_units_[best_thread].push_back(best_unit);
        thread_load[best_thread]++;
    }

    optimizeConnectionQueuesForThreads();

    // Safe in spin-barrier mode: all units complete cycle N before any
    // starts N+1, so localCycle() is only read between barriers.
    for (auto* unit : unit_ptrs_) {
        unit->useFastCycleCounter();
    }

    has_thread_assignment_ = true;

    buildCrossThreadDependencies();

    if (observe_ctx_) {
        observe::log_info<"Thread assignment: {} units across {} threads">(
            observe_ctx_, unit_ptrs_.size(), num_threads);
        for (size_t t = 0; t < num_threads; ++t) {
            std::string names = buildUnitNameList_(thread_units_[t]);
            observe::log_info<"  Thread {}: {} units [{}]">(observe_ctx_, t,
                                                            thread_units_[t].size(), names.c_str());
        }

        size_t total_deps = 0;
        uint32_t global_min_delay = UINT32_MAX;
        for (size_t t = 0; t < thread_cross_deps_temp_.size(); ++t) {
            total_deps += thread_cross_deps_temp_[t].size();
            for (const auto& dep : thread_cross_deps_temp_[t]) {
                global_min_delay = std::min(global_min_delay, dep.min_delay);
            }
        }
        observe::log_info<"  Cross-thread deps: {}, min delay: {} (progress-based sync)">(
            observe_ctx_, total_deps, global_min_delay == UINT32_MAX ? 0u : global_min_delay);

        for (auto* conn : connections_) {
            Unit* src = conn->source();
            Unit* dst = conn->destination();
            if (!src || !dst) continue;

            auto src_it = unit_ptr_to_idx.find(src);
            auto dst_it = unit_ptr_to_idx.find(dst);
            if (src_it == unit_ptr_to_idx.end() || dst_it == unit_ptr_to_idx.end()) continue;

            size_t src_cluster = unit_to_cluster_[src_it->second];
            size_t dst_cluster = unit_to_cluster_[dst_it->second];
            size_t src_thread = cluster_to_thread_[src_cluster];
            size_t dst_thread = cluster_to_thread_[dst_cluster];
            if (src_thread == dst_thread) continue;

            observe::log_info<"    {} -> {} (delay={}, C{}:T{} -> C{}:T{})">(
                observe_ctx_, src->name().c_str(), dst->name().c_str(), conn->delay(), src_cluster,
                src_thread, dst_cluster, dst_thread);
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-thread dependency analysis
// ---------------------------------------------------------------------------

void TickSimulation::buildCrossThreadDependencies() {
    const size_t num_clusters = clusters_.numClusters();
    thread_cross_deps_temp_.clear();
    thread_cross_deps_temp_.resize(num_clusters);
    if (num_clusters == 0) return;

    std::unordered_map<Unit*, size_t> unit_ptr_to_idx;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        unit_ptr_to_idx[static_cast<Unit*>(unit_ptrs_[i])] = i;
    }

    std::vector<std::unordered_map<size_t, uint32_t>> min_delay(num_clusters);

    for (auto* conn : connections_) {
        Unit* src = conn->source();
        Unit* dst = conn->destination();
        if (!src || !dst) continue;

        auto src_it = unit_ptr_to_idx.find(src);
        auto dst_it = unit_ptr_to_idx.find(dst);
        if (src_it == unit_ptr_to_idx.end() || dst_it == unit_ptr_to_idx.end()) continue;

        size_t src_cluster = unit_to_cluster_[src_it->second];
        size_t dst_cluster = unit_to_cluster_[dst_it->second];

        if (src_cluster == dst_cluster) continue;

        uint32_t delay = conn->delay();
        auto& dm = min_delay[dst_cluster];
        auto it = dm.find(src_cluster);
        if (it == dm.end() || delay < it->second) {
            dm[src_cluster] = delay;
        }
    }

    for (size_t c = 0; c < num_clusters; ++c) {
        for (auto& [src_cluster, delay] : min_delay[c]) {
            thread_cross_deps_temp_[c].push_back({src_cluster, delay});
        }
    }
}

}  // namespace chronon::sender
