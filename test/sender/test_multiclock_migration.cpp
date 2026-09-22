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

void physicalSampleCadence() {
    auto cfg = config();
    cfg.rebalance_check_interval_cycles = UINT64_MAX;
    TickSimulation sim(cfg);
    populate(sim);
    sim.runUntilTime(SimTime::nanoseconds(1000));
    Access::assertCostsReady(sim, false);
    sim.runUntilTime(SimTime::nanoseconds(1200));
    Access::assertCostsReady(sim, true);  // Both 2 GHz and 100 MHz, not 1024 local edges.
}

// Inject a reported service duration larger than either sampled interval. This
// makes the expected net sample exactly zero, without host-speed thresholds or
// sleeps. Calls still pass through the normal registration and the real FIFO.
struct AccountedService : HostService {
    size_t poll(size_t) noexcept override {
        HostServiceRegistration::thread_service_ns += uint64_t{1} << 40;
        return 1;
    }
};

struct AssistedBridge : sender::CdcComponent {
    TickSimulation& sim;
    sender::CdcComponent*& slot;
    sender::CdcComponent& fifo;
    AccountedService service;
    HostServiceRegistration registration{service};
    AssistedBridge(TickSimulation& sim)
        : sim(sim), slot(Access::bridgeCircuit(sim, 0)), fifo(*slot) {
        slot = this;
    }
    ~AssistedBridge() override { slot = &fifo; }
    uint32_t id() const noexcept override { return fifo.id(); }
    Unit* writeOwner() const noexcept override { return fifo.writeOwner(); }
    Unit* readOwner() const noexcept override { return fifo.readOwner(); }
    void setClockTraceStreams(observe::ClockTraceStream* write,
                              observe::ClockTraceStream* read) noexcept override {
        fifo.setClockTraceStreams(write, read);
    }
    bool endpointEdgesOnly() const noexcept override { return fifo.endpointEdgesOnly(); }
    bool drained() const noexcept override { return fifo.drained(); }
    void begin(std::span<const sender::ClockEdge> edges) override {
        fifo.begin(edges);
        registration.poll(true);
    }
    void commit() override {
        Access::assertBridgeBeginServiceExcluded(sim, 0);
        fifo.commit();
        registration.poll(true);
    }
};

void bridgeServiceAccounting() {
    for (bool profile : {false, true}) {
        auto cfg = config();
        cfg.profile_clock_scheduler = profile;
        cfg.rebalance_check_interval_cycles = UINT64_MAX;
        TickSimulation sim(cfg);
        const auto units = populate(sim);
        AssistedBridge bridge(sim);
        assert(sim.runClockEvents(1200) == 1200);
        Access::assertBridgeServiceExcluded(sim, 0);
        assert(bridge.registration.stats().calls == 2400);  // Both phases, all batches.
        for (auto* unit : units) assert(unit->ticks == sim.domainCycleCount(unit->clockDomainId()));
        Access::assertIdle(sim);
    }
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

void benefitPolicy() {
    using Benefit = sender::detail::MigrationBenefit;
    Benefit::Confidence confidence;
    assert(!confidence.observe(100, 3, false));
    assert(!confidence.observe(100, 4, true));
    assert(!confidence.observe(100, 7, true));
    assert(confidence.observe(101, 8, true));
    assert(!confidence.observe(1000, 12, true));
    assert(!confidence.observe(100, 2, true));  // Sample reset cannot inherit confidence.

    Benefit::Window window;
    window.observe({400, 4, 40, 4, 100, 20});
    assert(window.ready && window.cost == 28);  // Conditional activity, not raw timing mean.
    window.observe({40400, 8, 80, 8, 200, 40});
    assert(window.cost == 2008);  // Fresh phase is not diluted by historical samples.
    window.observe({40400, 8, 120, 12, 300, 40});
    assert(window.cost == 10);  // Entirely inactive window needs no active sample.

    Benefit policy;
    assert(!policy.profitable(100, -100, 100, 1000, 10000, 100, 20, 8, 1000, 0.01));
    assert(!policy.profitable(100, 0, 100, 1000, 1, 100, 20, 8, 1000, 0.01));
    assert(!policy.profitable(1, 1000, 10, 100, 100000, 100, 20, 8, 1000, 0.01));
    assert(policy.profitable(1000, -100, 1000, 10000, 10000, 100, 20, 8, 1000, 0.01));
    assert(!policy.profitable(1000, 0, 1000, 10000, 10000, 100, 20, 8, 0, 0.01));
    assert(!policy.profitable(17, -2.25, 17, 139, 100000, 100, 20, 12, 300, 0.01));
    assert(policy.profitable(3000, -5, 3000, 27000, 10000, 100, 20, 48, 27000, 0.01));
    for (double horizon : {1.0, 100.0, 10000.0}) {
        assert(policy.profitable(1000, -100, 1000, 10000, horizon, 100, 20, 8, 10000, 0.01) ==
               policy.profitable(500, -50, 1000, 5000, horizon * 2, 100, 20, 8, 5000, 0.01));
    }  // Changing reference frequency cannot change an otherwise identical decision.
    assert(Benefit::add(UINT64_MAX - 1, 100) == UINT64_MAX);
    assert(Benefit::multiply(UINT64_MAX, 4) == UINT64_MAX);
    policy.startRun(100, 1000);
    policy.before_ns_per_cycle = 10;
    policy.pending_generation = 1;
    policy.pending_cycle = 100;
    policy.pending = true;
    assert(!policy.feedback(200, 2000, 1, 10));  // Handoff hasn't committed.
    assert(!policy.feedback(120, 2000, 2, 10));  // Too little physical progress.
    assert(policy.feedback(200, 2100, 2, 10));   // Negative feedback increases cooldown.
    assert(policy.feedback_bad == 1 && policy.backoff == 2);
    for (unsigned i = 0; i < 10; ++i) policy.defer();
    assert(policy.backoff == 32);
    policy.pending = true;
    policy.startRun(200, 1000000);  // A stopped host interval is excluded.
    assert(!policy.pending && policy.rate(300, 1000500) == 5);
}

int main() {
    benefitPolicy();
    {
        auto cfg = config();
        cfg.tick_frequency_hz = 250'000'000;
        cfg.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
        TickSimulation sim(cfg);
        populate(sim);
        Access::verifyPlacementAndWaits(sim);
        Access::verifyRates(sim);
        Access::verifySafePoints(sim);
        Access::verifyBudget(sim);
    }
    autonomousMigration();
    physicalSampleCadence();
    bridgeServiceAccounting();
    {
        auto cfg = config();
        cfg.partition_solver = TickSimulationConfig::PartitionSolverType::SA;
        TickSimulation sim(cfg);
        populate(sim);
        Access::verifyPlacementAndWaits(sim);
        assert(sim.runClockEvents(100) == 100);
    }
    bridgePlanner();
    stopWithPending(false);
    stopWithPending(true);
    pendingAtRunBoundary();
    std::cout << "clock migration: physical costs, safe handoff, planner, stop/resume passed\n";
}
