// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>

#include "ClockMigrationTestAccess.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using Access = sender::DynamicMigrationTestAccess;

struct LaneUnit : TickableUnit {
    std::vector<std::unique_ptr<AsyncWritePort<uint64_t>>> outputs;
    std::vector<std::unique_ptr<AsyncReadPort<uint64_t>>> inputs;
    std::vector<uint64_t> sent, received, events;
    std::function<void()> callback;
    explicit LaneUnit(std::string name) : TickableUnit(std::move(name)) {}
    auto& output() {
        outputs.push_back(std::make_unique<AsyncWritePort<uint64_t>>(
            this, "out" + std::to_string(outputs.size())));
        sent.push_back(0);
        return *outputs.back();
    }
    auto& input() {
        inputs.push_back(
            std::make_unique<AsyncReadPort<uint64_t>>(this, "in" + std::to_string(inputs.size())));
        received.push_back(0);
        return *inputs.back();
    }
    void tick() override {
        for (size_t i = 0; i < inputs.size(); ++i) {
            uint64_t taken = 0;
            // Different lane stalls exercise independent RAM/output registers.
            if ((localCycle() + i) % 13 < 5) {
                if (auto packet = inputs[i]->take()) {
                    taken = packet->data;
                    assert(taken == ++received[i]);
                }
                inputs[i]->requestRead();
            }
            events.insert(events.end(), {localCycle(), i, taken, inputs[i]->outputValid()});
        }
        for (size_t i = 0; i < outputs.size(); ++i) {
            const bool full = !outputs[i]->canSend();
            if ((localCycle() + i) % 7 && outputs[i]->send({sent[i] + 1, sent[i] + 1})) ++sent[i];
            events.insert(events.end(), {localCycle(), i, sent[i], full});
        }
        if (callback) callback();
    }
};

// This deliberately observes every physical batch. Its native delegate would
// permit skipping unrelated domains, but the wrapper does not opt in.
struct BatchProbe : sender::CdcComponent {
    std::unique_ptr<sender::CdcComponent> delegate;
    uint64_t begins = 0, commits = 0;
    explicit BatchProbe(std::unique_ptr<sender::CdcComponent> fifo) : delegate(std::move(fifo)) {}
    uint32_t id() const noexcept override { return delegate->id(); }
    sender::Unit* writeOwner() const noexcept override { return delegate->writeOwner(); }
    sender::Unit* readOwner() const noexcept override { return delegate->readOwner(); }
    void setClockTraceStreams(observe::ClockTraceStream* w,
                              observe::ClockTraceStream* r) noexcept override {
        delegate->setClockTraceStreams(w, r);
    }
    void begin(std::span<const sender::ClockEdge> edges) override {
        ++begins;
        delegate->begin(edges);
    }
    void commit() override {
        ++commits;
        delegate->commit();
    }
    bool drained() const noexcept override { return delegate->drained(); }
};

namespace chronon::sender {
struct ClockScalingTestAccess {
    static BatchProbe* probe(TickSimulation& sim) {
        auto probe = std::make_unique<BatchProbe>(std::move(sim.cdc_.front()));
        auto* result = probe.get();
        sim.cdc_.front() = std::move(probe);
        return result;
    }
};
}  // namespace chronon::sender

struct Result {
    std::vector<std::vector<uint64_t>> state;
    std::vector<std::string> trace;
    bool operator==(const Result&) const = default;
};

Result run(size_t workers, bool dynamic, bool segmented, bool coincident, bool custom, bool tracing,
           bool migrating = false) {
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    config.max_lookahead_cycles = 7;
    config.profile_clock_scheduler = true;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(3, "write", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(97, "read", 500'000'000, 1,
                                           SimTime::picoseconds(coincident ? 0 : 137)));
    sim.addClockDomain(ClockDomain::fromHz(4093, "unrelated", 2'000'000'000));
    auto* w = sim.createUnitInDomain<LaneUnit>(3, "writer");
    auto* r = sim.createUnitInDomain<LaneUnit>(97, "reader");
    auto* w2 = sim.createUnitInDomain<LaneUnit>(3, "other_writer");
    auto* r2 = sim.createUnitInDomain<LaneUnit>(97, "other_reader");
    sim.createUnitInDomain<LaneUnit>(4093, "unrelated");
    std::vector<AsyncFifo<uint64_t>*> fifos;
    const auto connect = [&](LaneUnit* a, LaneUnit* b, size_t depth, size_t stages) {
        // Deliberately insert IDs out of order; runtime order must be stable.
        fifos.push_back(
            sim.connectAsyncFifo(100 - fifos.size(), a->output(), b->input(), {depth, stages}));
    };
    for (size_t lane = 0; lane < 8; ++lane) connect(w, r, 2ULL << (lane % 3), 2 + lane % 3);
    connect(w2, r2, 2, 2);  // Same clock pair, different ordered endpoint pair.
    connect(r, w, 4, 3);    // Reverse direction must remain independent.
    connect(w, w, 2, 2);    // Coincident sides of a self-loop.
    auto* probe = custom ? sender::ClockScalingTestAccess::probe(sim) : nullptr;
    const auto output =
        std::filesystem::temp_directory_path() / ("chronon-scaling-" + std::to_string(w->id()));
    if (tracing) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.stream_capacity = 2;
        trace.drain_batch = 1;
        trace.perfetto_options.clock_buffer_records = 1024;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    assert(sim.useParallelExecution() == (workers > 1));
    size_t requests = 0;
    if (migrating) {
        w->callback = [&] {
            if (requests < 3 && w->localCycle() >= 11 + 47 * requests &&
                Access::request(sim, Access::bridge(sim, 0)))
                ++requests;
        };
    }
    if (segmented) {
        for (unsigned i = 0; i < 10; ++i) assert(sim.runClockEvents(37) == 37);
    } else {
        assert(sim.runClockEvents(370) == 370);
    }
    sim.runUntilTime(SimTime::nanoseconds(400));
    sim.runDomainCycles(97, 19);
    if (migrating) {
        assert(requests == 3 && sim.rebalanceCount() == 3);
        Access::assertIdle(sim);
    }
    if (probe) assert(probe->begins == sim.schedulerSteps() && probe->commits == probe->begins);
    assert(!sim.totalTransportOverflowEvents());
    Result result;
    for (auto* unit : {w, r, w2, r2}) result.state.push_back(unit->events);
    for (const auto* fifo : fifos) {
        const auto state = fifo->diagnostics();
        result.state.push_back({state.write_binary, state.read_binary, state.write_sync,
                                state.read_sync, state.full, state.empty, state.output_valid,
                                state.ram_occupancy, state.writes, state.reads});
    }
    result.state.push_back({sim.schedulerSteps(), sim.domainCycleCount(3), sim.domainCycleCount(97),
                            sim.domainCycleCount(4093), sim.lastCommittedTime().numerator(),
                            sim.lastCommittedTime().denominator()});
    sim.closeClockTrace();
    if (tracing) {
        const auto stats = sim.clockTraceRecorder()->stats();
        assert(stats.events && !stats.dropped);
        for (const auto& entry : std::filesystem::directory_iterator(output)) {
            if (!entry.path().filename().string().starts_with("text-domain-")) continue;
            std::ifstream file(entry.path());
            std::string line;
            while (std::getline(file, line)) {
                if (line.starts_with('#')) continue;
                std::istringstream row(line);
                uint64_t cycle, id;
                row >> cycle >> id;
                std::string name;
                for (auto* u : {w, r, w2, r2})
                    if (u->id() == id) name = u->name();
                assert(!name.empty());
                std::string rest;
                std::getline(row, rest);
                result.trace.push_back(name + " " + std::to_string(cycle) + rest);
            }
        }
        std::sort(result.trace.begin(), result.trace.end());
        std::filesystem::remove_all(output);
    }
    return result;
}

int main() {
    for (bool coincident : {false, true}) {
        const auto reference = run(1, false, false, coincident, false, true);
        assert(run(1, false, true, coincident, true, true) == reference);
        for (size_t workers : {2, 4, 8})
            for (bool dynamic : {false, true})
                assert(run(workers, dynamic, true, coincident, false, true, dynamic) == reference);
    }
    std::cout
        << "shared lanes, sparse domains, fallback callbacks, segmentation and migration passed\n";
}
