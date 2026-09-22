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

std::vector<Endpoint*> populate(TickSimulation& sim, unsigned work = 0, bool coincident = false) {
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 2'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", coincident ? 2'000'000'000 : 100'000'000, 1,
                                           coincident ? SimTime{} : SimTime::picoseconds(137)));
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
    // Sanitizers and host scheduling can make every proposed move unprofitable.
    // Live sampling must still progress, preserve state, and account for any
    // committed ownership change. Positive admission is tested with controlled
    // cost windows below, not a host-speed requirement on this short run.
    Access::assertLiveSampling(sim);
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

void actorPlanner(bool bridge, bool finite_coincident = false) {
    TickSimulation sim(config());
    populate(sim, 0, finite_coincident);
    const auto actor = Access::planActor(sim, bridge, finite_coincident);
    assert(Access::owner(sim, actor) == 0);
    assert(sim.runClockEvents(100) == 100);
    assert(Access::owner(sim, actor) != 0);
    assert(sim.rebalanceCount() == 1);
    Access::assertIdle(sim);
}

void batchHorizon() {
    const auto fast = ClockDomain::fromHz(1, "fast", 1'000'000'000);
    const std::array peers{
        ClockDomain::fromHz(2, "identical", 1'000'000'000),
        ClockDomain::fromHz(2, "harmonic", 500'000'000),
        ClockDomain::fromHz(2, "partial", 2'000'000'000, 3),
        ClockDomain::fromHz(2, "phased", 1'000'000'000, 1, SimTime::picoseconds(500)),
        ClockDomain::fromHz(2, "delayed", 2'000'000'000, 1, SimTime::nanoseconds(10'000))};
    for (const auto& peer : peers) {
        const std::array clocks{&fast, &peer};
        Access::verifyBatchHorizon(clocks);
        Access::verifyBatchHorizon(clocks, 50'000);  // Resume away from the phase origin.
        Access::verifyBatchHorizon(clocks, 0, 250'000'000);
    }
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

void freshCostWindows() {
    using Benefit = sender::detail::MigrationBenefit;
    using Sample = Benefit::Sample;
    Benefit::Window window;
    Benefit::Confidence confidence;
    const auto observe = [&](Sample sample) {
        window.observe(sample);
        return confidence.observe(window.cost, window.samples, window.ready);
    };
    assert(!observe({400, 4, 0, 0, 100, 100}));
    assert(observe({800, 8, 0, 0, 200, 200}));
    assert(!observe({800, 8, 0, 0, 200, 200}));  // No new progress is not a fresh window.
    assert(!window.ready);
    assert(!confidence.observe(window.cost, window.samples, true));  // Cannot reuse the count.
    assert(!observe({800, 8, 0, 0, 203, 203}));                      // Too few fresh cycles.
    assert(window.previous.cycles == 200);
    assert(observe({1200, 12, 0, 0, 300, 300}));
    assert(observe({1600, 16, 0, 0, 400, 400}));

    assert(!observe({1600, 16, 30, 3, 500, 400}));  // Active -> inactive, partial timing.
    assert(!window.ready);
    assert(!observe({1600, 16, 30, 3, 600, 400}));  // Preserve the baseline while sparse.
    assert(window.previous.cycles == 400);
    assert(!observe({1600, 16, 40, 4, 700, 400}));
    assert(window.ready && window.cost == 10);
    assert(observe({1600, 16, 80, 8, 800, 400}));
    assert(!observe({1900, 19, 80, 8, 900, 500}));  // Inactive -> active, partial timing.
    assert(!window.ready);
    assert(!observe({2000, 20, 80, 8, 1000, 600}));
    assert(window.ready && window.cost == 100);
    assert(observe({2400, 24, 80, 8, 1100, 700}));

    assert(!observe({2800, 28, 120, 12, 1200, 850}));  // Inconsistent activity snapshot.
    assert(!window.ready && window.previous.cycles == 1100);
    assert(observe({2800, 28, 120, 12, 1300, 850}));
    assert(window.ready && window.cost == 77.5);
    assert(observe({3200, 32, 160, 16, 1500, 1000}));

    window = {};
    confidence = {};
    assert(!observe({400, 4, 40, 4, 100, 50}));
    assert(observe({800, 8, 80, 8, 200, 100}));
    assert(!observe({1100, 11, 120, 12, 300, 150}));  // Mixed phase lacks active samples.
    assert(!window.ready);
    assert(observe({1200, 12, 120, 12, 400, 200}));
    assert(window.cost == 55);
    assert(!observe({1600, 16, 150, 15, 500, 250}));  // Mixed phase lacks inactive samples.
    assert(!window.ready);
    assert(observe({1600, 16, 160, 16, 600, 300}));

    // Slow actors always have incomplete checks between complete windows. They
    // can still regain confidence; a changed cost needs a compatible new pair.
    window = {};
    confidence = {};
    Sample slow;
    for (unsigned round = 0; round < 4; ++round) {
        for (unsigned sample = 1; sample <= 4; ++sample) {
            slow.active_ns += round < 2 ? 100 : 1000;
            ++slow.active;
            ++slow.cycles;
            ++slow.active_cycles;
            assert(observe(slow) == (sample == 4 && round % 2 == 1));
            assert(window.ready == (sample == 4));
        }
    }

    // Every cumulative field may reset/wrap. Rebase without subtracting across
    // that discontinuity, then allow two new consistent windows to recover.
    for (auto field : {&Sample::active_ns, &Sample::active, &Sample::inactive_ns, &Sample::inactive,
                       &Sample::cycles, &Sample::active_cycles}) {
        window = {};
        confidence = {};
        assert(!observe({400, 4, 40, 4, 100, 50}));
        Sample current{800, 8, 80, 8, 200, 100};
        assert(observe(current));
        --(current.*field);
        assert(!observe(current));
        assert(!window.ready && window.samples == 0 && window.previous.*field == current.*field);
        for (unsigned round = 0; round < 2; ++round) {
            current.active_ns += 400;
            current.active += 4;
            current.inactive_ns += 40;
            current.inactive += 4;
            current.cycles += 100;
            current.active_cycles += 50;
            assert(observe(current) == (round == 1));
            assert(window.ready && window.cost == 55);
        }
    }
}

void benefitPolicy() {
    using Benefit = sender::detail::MigrationBenefit;
    Benefit::Confidence confidence;
    assert(!confidence.observe(100, 3, false));
    assert(!confidence.observe(100, 4, true));
    assert(!confidence.observe(100, 7, true));
    assert(confidence.observe(101, 8, true));
    assert(!confidence.observe(101, 9, true));  // Stable does not bypass the fresh-sample count.
    assert(!confidence.observe(1000, 12, true));
    assert(!confidence.observe(100, 2, true));  // Sample reset cannot inherit confidence.
    assert(!confidence.observe(std::numeric_limits<double>::quiet_NaN(), 6, true));
    assert(confidence.samples == 0);
    assert(!confidence.observe(100, 4, true));
    assert(confidence.observe(100, 8, true));

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
    freshCostWindows();
    benefitPolicy();
    batchHorizon();
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
    actorPlanner(false);
    actorPlanner(true);
    actorPlanner(false, true);
    actorPlanner(true, true);
    stopWithPending(false);
    stopWithPending(true);
    pendingAtRunBoundary();
    std::cout << "clock migration: physical costs, safe handoff, planner, stop/resume passed\n";
}
