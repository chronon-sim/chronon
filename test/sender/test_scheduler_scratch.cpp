// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <iostream>

#include "../../benchmark/SchedulerInvocationModel.hpp"
#include "ClockMigrationTestAccess.hpp"
#include "clock_reference.hpp"

namespace chronon::sender {
struct SchedulerScratchTestAccess {
    static void assertInitializedGraph(const TickSimulation& sim) {
        const std::vector<Unit*> units(sim.unit_ptrs_.begin(), sim.unit_ptrs_.end());
        DependencyGraph oracle;
        oracle.build(units, sim.connections_);
        assert(sim.dependencyGraph().units() == units);
        assert(sim.dependencyGraph().distances() == oracle.distances());
        assert(sim.dependencyGraph().graph()->numEdges() == oracle.graph()->numEdges());
    }
    static std::vector<const void*> storage(const TickSimulation& sim) {
        std::vector<const void*> result;
        if (!sim.thread_progress_array_) return result;
        result.push_back(sim.schedulerScratch_().admission_calendar.get());
        for (const auto& worker : sim.schedulerScratch_().workers) {
            result.push_back(worker.predecessor.observed_cycles.data());
            result.push_back(worker.ready_through.data());
        }
        return result;
    }
    static void assertOwnershipLists(const TickSimulation& sim) {
        if (!sim.cluster_runtime_owner_) return;
        std::vector<size_t> seen_clusters(sim.clusters_.numClusters());
        std::vector<size_t> seen_units(sim.unit_ptrs_.size());
        for (size_t worker = 0; worker < sim.thread_units_.size(); ++worker) {
            for (const size_t cluster : sim.thread_clusters_[worker]) {
                assert(sim.cluster_runtime_owner_[cluster].load() == worker);
                assert(++seen_clusters[cluster] == 1);
            }
            for (const size_t unit : sim.thread_units_[worker]) {
                assert(sim.cluster_runtime_owner_[sim.unit_to_cluster_[unit]].load() == worker);
                assert(++seen_units[unit] == 1);
            }
        }
        assert(
            std::all_of(seen_clusters.begin(), seen_clusters.end(), [](auto n) { return n == 1; }));
        assert(std::all_of(seen_units.begin(), seen_units.end(), [](auto n) { return n == 1; }));
        if (sim.clock_parallel_) {
            std::vector<size_t> seen(sim.clock_parallel_->bridges.size());
            for (size_t worker = 0; worker < sim.clock_parallel_->worker_bridges.size(); ++worker)
                for (const size_t bridge : sim.clock_parallel_->worker_bridges[worker]) {
                    assert(
                        sim.cluster_runtime_owner_[sim.clusters_.numClusters() + bridge].load() ==
                        worker);
                    assert(++seen[bridge] == 1);
                }
            assert(std::all_of(seen.begin(), seen.end(), [](auto n) { return n == 1; }));
        }
    }
    static void assertLocalProgress(const TickSimulation& sim) {
        if (!sim.thread_progress_array_ || sim.clock_parallel_) return;
        for (size_t worker = 0; worker < sim.thread_clusters_.size(); ++worker) {
            const auto& observed =
                sim.schedulerScratch_().workers[worker].predecessor.observed_cycles;
            // Small dynamic invocations use stack slots, which do not survive
            // the join. Larger graphs and static workers retain their storage.
            if (observed.empty()) continue;
            for (size_t cluster : sim.thread_clusters_[worker])
                assert(observed[cluster] ==
                       sim.thread_progress_array_[cluster].completed_cycle.load());
        }
    }
    static void poison(TickSimulation& sim) {
        if (!sim.thread_progress_array_) return;
        if (auto& calendar = sim.schedulerScratch_().admission_calendar)
            for (size_t i = 0; i < 11; ++i) (void)calendar->pop();
        for (auto& worker : sim.schedulerScratch_().workers) {
            std::fill(worker.predecessor.observed_cycles.begin(),
                      worker.predecessor.observed_cycles.end(), UINT64_MAX);
            std::fill(worker.ready_through.begin(), worker.ready_through.end(), UINT64_MAX);
            // If a previous invocation's view survives, these are invalid IDs.
            for (auto* owners : {&worker.owned_clusters, &worker.owned_bridges,
                                 &worker.owned_actors, &worker.ownership})
                std::fill(owners->begin(), owners->end(), SIZE_MAX);
        }
    }
};
}  // namespace chronon::sender

using namespace chronon;
using namespace chronon::benchmark;
using Scratch = sender::SchedulerScratchTestAccess;
using Migration = sender::DynamicMigrationTestAccess;

static std::vector<uint64_t> exercise(bool clock, bool dynamic, size_t workers, uint64_t interval,
                                      size_t pairs) {
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    config.max_lookahead_cycles = 32;
    config.epoch_size = interval;
    TickSimulation sim(config);
    const auto units = invocationModel(sim, clock, pairs, 8, 4);
    sim.initialize();
    Scratch::assertInitializedGraph(sim);
    assert(sim.useParallelExecution() == (workers > 1));
    const auto advance = [&](uint64_t count) {
        return clock ? sim.runClockEvents(count) : sim.run(count);
    };
    assert(advance(128) == 128);
    Scratch::assertOwnershipLists(sim);
    Scratch::assertLocalProgress(sim);
    const auto storage = Scratch::storage(sim);
    if (dynamic && workers > 1) {
        const auto actor = clock ? Migration::bridge(sim, 0) : Migration::cluster(sim, units[0]);
        assert(Migration::request(sim, actor));
    }
    uint64_t predicate_calls = 0;
    const auto completed = sim.runUntil(
        [&] {
            ++predicate_calls;
            Scratch::poison(sim);  // Predicates only run after all worker tasks join.
            return (clock ? sim.schedulerSteps() : sim.currentCycle()) >= 256;
        },
        128);
    assert(completed == 128);
    assert(predicate_calls == (128 + interval - 1) / interval);
    assert(storage == Scratch::storage(sim));
    Scratch::assertOwnershipLists(sim);
    if (dynamic && workers > 1) Migration::assertIdle(sim);
    assert(sim.totalTransportOverflowEvents() == 0);
    // Continue with a fresh limit after the poisoned predicate boundary.
    assert(advance(19) == 19);
    Scratch::assertOwnershipLists(sim);
    Scratch::assertLocalProgress(sim);
    return invocationState(units);
}

static void exerciseCoincidentCdcLists() {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation sim(config);
    const std::array<ClockDomainId, 3> ids{11, 29, 3};
    for (size_t i = 0; i < ids.size(); ++i)
        sim.addClockDomain(ClockDomain(ids[i], "clock-" + std::to_string(i), SimTime(i ? 3 : 2),
                                       SimTime(i ? 1 : 0)));
    std::vector<InvocationUnit*> writers, readers;
    std::vector<AsyncFifo<uint64_t>*> fifos;
    std::array<clock_reference::Fifo, 3> oracle{
        clock_reference::Fifo(16, 2), clock_reference::Fifo(16, 2), clock_reference::Fifo(16, 2)};
    const std::array<uint64_t, 3> fifo_ids{31, 7, 19};
    for (size_t i = 0; i < ids.size(); ++i) {
        writers.push_back(sim.createUnitInDomain<InvocationUnit>(
            ids[i], "writer-" + std::to_string(i), true, true, 0));
        readers.push_back(sim.createUnitInDomain<InvocationUnit>(
            ids[(i + 1) % ids.size()], "reader-" + std::to_string(i), true, false, 0));
        fifos.push_back(sim.connectAsyncFifo(fifo_ids[i], writers[i]->async_out,
                                             readers[i]->async_in, {16, 2}));
    }
    std::array<uint64_t, 3> cycles{};
    // Two coincident domains have both shared and unique lanes. Other batches
    // select one or all three domains, interleaving the merge and general paths.
    for (size_t step = 0; step < 200; ++step) {
        auto time = sim.clockDomain(ids.front()).edge(cycles.front());
        for (size_t i = 1; i < ids.size(); ++i)
            time = std::min(time, sim.clockDomain(ids[i]).edge(cycles[i]));
        std::array<bool, 3> active;
        for (size_t i = 0; i < ids.size(); ++i)
            active[i] = sim.clockDomain(ids[i]).edge(cycles[i]) == time;
        assert(sim.runClockEvents(1) == 1);
        assert(sim.lastCommittedTime() == time);
        for (size_t i = 0; i < ids.size(); ++i) {
            const auto next = (i + 1) % ids.size();
            oracle[i].step(active[i], active[next], true, true, cycles[i], cycles[next]);
            const auto state = fifos[i]->diagnostics();
            assert(state.write_binary == oracle[i].writes % 32);
            assert(state.read_binary == oracle[i].reads % 32);
            assert(state.full == oracle[i].full && state.empty == oracle[i].empty);
            assert(state.output_valid == bool(oracle[i].output));
            assert(state.ram_occupancy == oracle[i].memory.size());
        }
        for (size_t i = 0; i < ids.size(); ++i) {
            cycles[i] += active[i];
            assert(sim.domainCycleCount(ids[i]) == cycles[i]);
        }
        assert(sim.totalTransportOverflowEvents() == 0);
    }
}

int main() {
    exerciseCoincidentCdcLists();
    // A one-unit ordinary graph retains its original index order.
    {
        TickSimulationConfig config;
        config.enable_parallel = false;
        config.num_threads = 1;
        TickSimulation sim(config);
        auto* unit = sim.createUnit<InvocationUnit>("only", false, false, 0);
        sim.initialize();
        Scratch::assertInitializedGraph(sim);
        assert(sim.dependencyGraph().lookahead(unit, unit) == 0);
    }
    for (bool clock : {false, true}) {
        for (size_t pairs : {4, 12}) {
            const auto serial = exercise(clock, false, 1, 1, pairs);
            for (bool dynamic : {false, true})
                for (uint64_t interval : {1, 7, 64})
                    assert(exercise(clock, dynamic, 4, interval, pairs) == serial);
        }
    }
    std::cout << "Repeated runs reset scratch and preserve state across migration\n";
}
