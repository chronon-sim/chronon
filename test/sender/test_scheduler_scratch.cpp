// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <iostream>

#include "../../benchmark/SchedulerInvocationModel.hpp"
#include "ClockMigrationTestAccess.hpp"

namespace chronon::sender {
struct SchedulerScratchTestAccess {
    static std::vector<const void*> storage(const TickSimulation& sim) {
        std::vector<const void*> result;
        if (!sim.scheduler_scratch_) return result;
        for (const auto& worker : sim.scheduler_scratch_->workers) {
            result.push_back(worker.predecessor.observed_cycles.data());
            result.push_back(worker.ready_through.data());
        }
        return result;
    }
    static void poison(TickSimulation& sim) {
        if (!sim.scheduler_scratch_) return;
        for (auto& worker : sim.scheduler_scratch_->workers) {
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
    assert(sim.useParallelExecution() == (workers > 1));
    const auto advance = [&](uint64_t count) {
        return clock ? sim.runClockEvents(count) : sim.run(count);
    };
    assert(advance(128) == 128);
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
    if (dynamic && workers > 1) Migration::assertIdle(sim);
    assert(sim.totalTransportOverflowEvents() == 0);
    // Continue with a fresh limit after the poisoned predicate boundary.
    assert(advance(19) == 19);
    return invocationState(units);
}

int main() {
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
