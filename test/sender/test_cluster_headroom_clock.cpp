// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>

#include "ClockMigrationTestAccess.hpp"
#include "EpochFreeDifferentialHarness.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender::test;
using Access = chronon::sender::DynamicMigrationTestAccess;

namespace {

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class Source final : public TickableUnit {
public:
    OutPort<uint64_t> out{this, "out", 4};
    UnitEventLog log{0};
    uint64_t sent = 0;
    Source() : TickableUnit("source") {}
    void tick() override {
        const bool accepted = out.send(sent);
        log.record(localCycle(), ModelEventKind::SendResult, sent, accepted);
        if (accepted) ++sent;
    }
};

class Writer final : public TickableUnit {
public:
    InPort<uint64_t> in{this, "in", 1};
    AsyncWritePort<uint64_t> out{this, "out"};
    UnitEventLog log{1};
    std::optional<uint64_t> pending;
    std::function<void(uint64_t)> after_tick;
    Writer() : TickableUnit("writer") {}
    void tick() override {
        if (!pending) {
            pending = in.tryReceive(localCycle());
            if (pending) log.record(localCycle(), ModelEventKind::Receive, *pending);
        }
        if (pending) {
            const bool sent = out.send({*pending + 1, *pending});
            log.record(localCycle(), ModelEventKind::SendResult, *pending, sent);
            if (sent) pending.reset();
        }
        if (after_tick) after_tick(localCycle());
    }
};

class Reader final : public TickableUnit {
public:
    AsyncReadPort<uint64_t> in{this, "in"};
    UnitEventLog log{2};
    Reader() : TickableUnit("reader") {}
    void tick() override {
        if (localCycle() % 13 < 7) {
            if (auto value = in.take())
                log.record(localCycle(), ModelEventKind::Receive, value->data);
            log.record(localCycle(), ModelEventKind::State, in.requestRead());
        }
    }
};

struct Result {
    std::vector<CanonicalEvent> events;
    std::vector<uint64_t> state;
    friend bool operator==(const Result&, const Result&) = default;
};

Result run(size_t workers, bool dynamic, bool segmented, bool migrate) {
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.initial_partition_sync_cost_ns = 0;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 2'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 700'000'000, 1, SimTime::picoseconds(137)));
    auto* source = sim.createUnitInDomain<Source>(1);
    auto* writer = sim.createUnitInDomain<Writer>(1);
    auto* reader = sim.createUnitInDomain<Reader>(2);
    const auto* connection = sim.connect(source->out, writer->in, 1);
    const auto* fifo = sim.connectAsyncFifo(1, writer->out, reader->in, {2, 2});
    sim.initialize();
    require(sim.useParallelExecution() == (workers > 1),
            "clock headroom disabled parallel execution");
    if (workers > 1) {
        require(Access::cluster(sim, source) == Access::cluster(sim, writer),
                "clock pair was split");
        require(Access::cluster(sim, reader) != Access::cluster(sim, writer), "CDC domains merged");
    }
    require(connection->delay() == 1 && writer->in.configuredCapacity() == 1,
            "clock clustering changed port semantics");
    size_t requests = 0;
    if (migrate) {
        writer->after_tick = [&](uint64_t cycle) {
            if (requests < 3 && cycle >= 11 + 64 * requests &&
                Access::request(sim, Access::cluster(sim, writer)))
                ++requests;
        };
    }
    if (segmented) {
        for (size_t i = 0; i < 5; ++i)
            require(sim.runClockEvents(37) == 37, "clock segment stopped");
    } else {
        require(sim.runClockEvents(185) == 185, "clock run stopped");
    }
    sim.runUntilTime(SimTime::nanoseconds(500));
    sim.runDomainCycles(2, 19);
    require(sim.totalTransportOverflowEvents() == 0, "clock transport overflowed");
    if (workers > 1) require(sim.epochFreeRunCount() > 0, "clock run fell back to sequential");
    if (migrate) {
        require(requests == 3 && sim.rebalanceCount() == 3, "clock migrations did not finish");
        require(sim.assignedThread(source) == sim.assignedThread(writer),
                "clock migration split cluster");
        Access::assertIdle(sim);
    }
    const std::array<UnitEventLog*, 3> logs{&source->log, &writer->log, &reader->log};
    const auto state = fifo->diagnostics();
    return {canonicalizeEvents(logs),
            {source->sent, writer->pending.value_or(UINT64_MAX), sim.domainCycleCount(1),
             sim.domainCycleCount(2), state.write_binary, state.read_binary, state.write_sync,
             state.read_sync, state.full, state.empty, state.output_valid, state.ram_occupancy}};
}

}  // namespace

int main() {
    try {
        const auto reference = run(1, false, false, false);
        require(run(1, false, true, false) == reference, "segmented clock reference differs");
        for (size_t workers : {2, 4}) {
            for (bool dynamic : {false, true}) {
                for (bool segmented : {false, true}) {
                    require(run(workers, dynamic, segmented, dynamic) == reference,
                            "clustered clock/CDC cycle-visible trace differs");
                }
            }
        }
        std::cout << "Clock-domain cluster headroom: PASSED\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
