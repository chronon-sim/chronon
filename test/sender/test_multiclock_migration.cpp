// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <functional>
#include <iostream>

#include "ClockMigrationTestAccess.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using Access = sender::DynamicMigrationTestAccess;

struct Endpoint : TickableUnit {
    using TickableUnit::requestTermination;
    AsyncWritePort<uint64_t> out{this, "out"};
    AsyncReadPort<uint64_t> in{this, "in"};
    std::function<void()> callback;
    uint64_t checksum = 0, ticks = 0;
    bool writer;
    unsigned work;
    Endpoint(std::string name, bool writer, unsigned work = 0)
        : TickableUnit(std::move(name)), writer(writer), work(work) {}
    void tick() override {
        for (unsigned i = 0; i < work; ++i) {
            checksum = checksum * 6364136223846793005ULL + i + 1;
            asm volatile("" : "+r"(checksum));
        }
        if (writer)
            out.send({localCycle() + 1, localCycle()});
        else {
            if (auto value = in.take()) checksum ^= value->data;
            in.requestRead();
        }
        ++ticks;
        if (callback) callback();
    }
};

TickSimulationConfig config() {
    TickSimulationConfig result;
    result.num_threads = 3;
    result.enable_dynamic_rebalance = true;
    result.rebalance_check_interval_cycles = 128;
    result.rebalance_cooldown_cycles = 64;
    return result;
}

std::vector<Endpoint*> populate(TickSimulation& sim, unsigned work = 0) {
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 2'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 100'000'000, 1, SimTime::picoseconds(137)));
    std::vector<Endpoint*> units;
    for (size_t i = 0; i < 3; ++i) {
        auto* writer =
            sim.createUnitInDomain<Endpoint>(1, "writer" + std::to_string(i), true, work);
        auto* reader =
            sim.createUnitInDomain<Endpoint>(2, "reader" + std::to_string(i), false, work);
        sim.connectAsyncFifo(i, writer->out, reader->in, {2, 2});
        units.push_back(writer);
        units.push_back(reader);
    }
    sim.initialize();
    assert(sim.useParallelExecution());
    return units;
}

void autonomousMigration() {
    TickSimulation sim(config());
    const auto units = populate(sim, 3000);
    Access::placeAllOnWorkerZero(sim);
    sim.runUntilTime(SimTime::nanoseconds(6000));
    assert(sim.rebalanceCount() > 0);
    bool moved = false;
    for (auto* unit : units) moved |= sim.assignedThread(unit) != 0;
    assert(moved);
    for (auto* unit : units) assert(unit->ticks == sim.domainCycleCount(unit->clockDomainId()));
    Access::assertIdle(sim);
    const auto before = sim.schedulerSteps();
    assert(sim.runClockEvents(79) == 79);
    assert(sim.schedulerSteps() == before + 79);
    Access::assertIdle(sim);
    std::cout << "autonomous clock migrations: " << sim.rebalanceCount() << '\n';
}

void bridgePlanner() {
    TickSimulation sim(config());
    populate(sim);
    const auto actor = Access::planBridge(sim);
    assert(Access::owner(sim, actor) == 0);
    assert(sim.runClockEvents(100) == 100);
    assert(Access::owner(sim, actor) != 0);
    assert(sim.rebalanceCount() == 1);
    Access::assertIdle(sim);
}

void stopWithPending(bool fail) {
    auto cfg = config();
    cfg.rebalance_check_interval_cycles = UINT64_MAX;
    TickSimulation sim(cfg);
    const auto units = populate(sim);
    units[0]->callback = [&] {
        if (units[0]->localCycle() != 5) return;
        assert(Access::request(sim, Access::bridge(sim, 0), UINT64_MAX));
        if (fail) throw std::runtime_error("migration stop test");
        units[0]->requestTermination(TerminationReason::Completed, 0, "migration stop");
    };
    bool caught = false;
    try {
        sim.runClockEvents(1000);
    } catch (const std::exception&) {
        caught = true;
    }
    assert(caught == fail);
    Access::assertIdle(sim);
    assert(!sim.rebalanceCount());
    if (fail) {
        caught = false;
        try {
            sim.runClockEvents(1);
        } catch (const std::logic_error&) {
            caught = true;
        }
        assert(caught);
    } else {
        const auto& request = sim.terminationRequest();
        assert(request.physical_time <= request.settled_time);
        sim.resetTermination();
        assert(sim.runClockEvents(73) == 73);
        Access::assertIdle(sim);
    }
}

void pendingAtRunBoundary() {
    auto cfg = config();
    cfg.rebalance_check_interval_cycles = UINT64_MAX;
    TickSimulation sim(cfg);
    const auto units = populate(sim);
    units[0]->callback = [&] {
        if (units[0]->localCycle() == 5)
            assert(Access::request(sim, Access::cluster(sim, units[0]), UINT64_MAX));
    };
    assert(sim.runClockEvents(37) == 37);
    Access::assertIdle(sim);
    assert(sim.runClockEvents(73) == 73);
    Access::assertIdle(sim);
    assert(!sim.rebalanceCount());
}

int main() {
    {
        auto cfg = config();
        cfg.tick_frequency_hz = 250'000'000;
        TickSimulation sim(cfg);
        populate(sim);
        Access::verifyRates(sim);
        Access::verifySafePoints(sim);
    }
    autonomousMigration();
    bridgePlanner();
    stopWithPending(false);
    stopWithPending(true);
    pendingAtRunBoundary();
    std::cout << "clock migration: physical costs, safe handoff, planner, stop/resume passed\n";
}
