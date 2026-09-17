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
std::filesystem::path fixture_root;
bool export_fixtures = false;

struct LaneUnit : TickableUnit {
    using TickableUnit::requestTermination;
    OutPort<int> order_out{this, "order_out"};
    InPort<int> order_in{this, "order_in"};
    std::vector<std::unique_ptr<AsyncWritePort<uint64_t>>> outputs;
    std::vector<std::unique_ptr<AsyncReadPort<uint64_t>>> inputs;
    std::vector<uint64_t> sent, received, events, transaction_prefix;
    std::function<void()> callback;
    explicit LaneUnit(std::string name) : TickableUnit(std::move(name)) {}
    auto& output() {
        outputs.push_back(std::make_unique<AsyncWritePort<uint64_t>>(
            this, "out" + std::to_string(outputs.size())));
        sent.push_back(0);
        transaction_prefix.push_back(0);
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
            if ((localCycle() + i) % 7 &&
                outputs[i]->send({transaction_prefix[i] + sent[i] + 1, sent[i] + 1}))
                ++sent[i];
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
    static std::vector<const void*> pendingStorage(const TickSimulation& sim) {
        std::vector<const void*> result;
        if (sim.clock_parallel_) {
            const auto& runtime = *sim.clock_parallel_;
            assert(runtime.pending.size() == sim.config_.max_lookahead_cycles);
            assert(runtime.pending_size == 0);
            for (const auto& batch : runtime.pending) {
                assert(batch.edges.capacity() >= runtime.indexed_domains.size());
                result.push_back(batch.edges.data());
            }
        }
        return result;
    }
    static size_t sharedActor(TickSimulation& sim, size_t lanes) {
        const auto& runtime = *sim.clock_parallel_;
        assert(runtime.bridges.size() ==
               (lanes == 8 ? 4 : 3));  // shared, reverse, self, optionally unrelated
        for (size_t b = 0; b < runtime.bridges.size(); ++b) {
            const auto& group = *runtime.bridges[b];
            if (group.lanes.size() != lanes) continue;
            assert(group.lanes.front().circuit->id() == (lanes == 8 ? 93 : 92));
            assert(group.lanes.back().circuit->id() == 100);
            const auto actor = sim.clusters_.numClusters() + b;
            if (sim.config_.enable_dynamic_rebalance) {
                for (const auto& edge : sim.dynamic_rebalance_adjacency_[actor])
                    assert(edge.num_connections == lanes);
                // Unmeasured work prior remains the SUM of all lane costs.
                assert(std::abs(sim.dynamicClockActorCost_(actor).cost - 1.5 * lanes) < 1e-9);
            }
            return actor;
        }
        assert(false);
        return SIZE_MAX;
    }
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
           bool migrating = false, bool clustered = false, bool stop_resume = false) {
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
    if (clustered) {
        sim.connect(w->order_out, w2->order_in, 0);
        sim.connect(r->order_out, r2->order_in, 0);
    }
    std::vector<AsyncFifo<uint64_t>*> fifos;
    const auto connect = [&](LaneUnit* a, LaneUnit* b, size_t depth, size_t stages) {
        // Deliberately insert IDs out of order; runtime order must be stable.
        const auto id = 100 - fifos.size();
        fifos.push_back(sim.connectAsyncFifo(id, a->output(), b->input(), {depth, stages}));
        a->transaction_prefix.back() = uint64_t{id} << 32;
    };
    for (size_t lane = 0; lane < 8; ++lane) connect(w, r, 2ULL << (lane % 3), 2 + lane % 3);
    connect(w2, r2, 2, 2);  // Same clock pair, different ordered endpoint pair.
    connect(r, w, 4, 3);    // Reverse direction must remain independent.
    connect(w, w, 2, 2);    // Coincident sides of a self-loop.
    auto* probe = custom ? sender::ClockScalingTestAccess::probe(sim) : nullptr;
    const auto output =
        fixture_root / ("shared-t" + std::to_string(workers) + "-d" + std::to_string(dynamic) +
                        "-c" + std::to_string(coincident) + "-custom" + std::to_string(custom) +
                        "-seg" + std::to_string(segmented));
    const bool keep = export_fixtures && workers == 4 && migrating;
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
    const auto storage = sender::ClockScalingTestAccess::pendingStorage(sim);
    const auto shared = workers > 1
                            ? sender::ClockScalingTestAccess::sharedActor(sim, clustered ? 9 : 8)
                            : SIZE_MAX;
    size_t requests = 0;
    if (migrating || stop_resume) {
        w->callback = [&] {
            if (stop_resume && w->localCycle() == 9)
                w->requestTermination(TerminationReason::Completed, 0, "grouped stop");
            if (migrating && requests < 3 && w->localCycle() >= 11 + 47 * requests &&
                Access::request(sim, shared))
                ++requests;
        };
    }
    if (stop_resume) {
        const auto done = sim.runClockEvents(370);
        assert(done > 0 && done < 370);
        assert(sim.wasTerminationRequested());
        assert(sim.terminationRequest().settled_time == sim.lastCommittedTime());
        // Every participating lane has committed at the reported boundary.
        for (const auto* fifo : fifos) (void)fifo->diagnostics();
        sim.resetTermination();
        assert(sim.runClockEvents(370 - done) == 370 - done);
    } else if (segmented) {
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
    assert(storage == sender::ClockScalingTestAccess::pendingStorage(sim));
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
        std::ofstream reference;
        if (keep) {
            reference.open(output / "reference.tsv");
            reference
                << "ts\tunit\tlocal_cycle\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
        }
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
                uint64_t ts = 0;
                for (auto* u : {w, r, w2, r2}) {
                    if (u->id() == id) {
                        name = u->fullPath();
                        ts = u->clockDomain().edge(cycle).floorNanoseconds();
                    }
                }
                assert(!name.empty());
                std::string rest;
                std::getline(row, rest);
                result.trace.push_back(name + " " + std::to_string(cycle) + rest);
                if (keep) reference << ts << '\t' << name << '\t' << cycle << rest << '\n';
            }
        }
        std::sort(result.trace.begin(), result.trace.end());
        if (!keep) std::filesystem::remove_all(output);
    }
    return result;
}

Result runWorkerLimit(size_t workers, TickSimulationConfig::PartitionSolverType solver,
                      bool dynamic, bool clustered) {
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.partition_solver = solver;
    config.initial_partition_sync_cost_ns = 0;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    config.profile_clock_scheduler = true;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "write", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "read", 500'000'000, 1, SimTime::picoseconds(137)));
    auto* w = sim.createUnitInDomain<LaneUnit>(1, "writer");
    auto* r = sim.createUnitInDomain<LaneUnit>(2, "reader");
    auto* w2 = clustered ? sim.createUnitInDomain<LaneUnit>(1, "writer2") : w;
    auto* r2 = clustered ? sim.createUnitInDomain<LaneUnit>(2, "reader2") : r;
    if (clustered) {
        sim.connect(w->order_out, w2->order_in, 0);
        sim.connect(r->order_out, r2->order_in, 0);
    }
    std::vector<AsyncFifo<uint64_t>*> fifos;
    for (size_t lane = 0; lane < 32; ++lane) {
        auto* source = lane % 2 ? w2 : w;
        auto* destination = lane % 2 ? r2 : r;
        fifos.push_back(sim.connectAsyncFifo(lane + 1, source->output(), destination->input(),
                                             {2ULL << (lane % 3), 2 + lane % 3}));
    }
    sim.initialize();
    assert(sim.useParallelExecution() == (workers > 1));
    // Two endpoint clusters and one shared bridge actor, regardless of the
    // number of FIFO lanes or units inside either endpoint cluster.
    const size_t active_workers = std::min(workers, size_t{3});
    assert(sim.clockSchedulerProfile().size() == active_workers);
    size_t requests = 0;
    if (dynamic) {
        w->callback = [&] {
            if (requests < 2 && w->localCycle() >= 11 + 47 * requests &&
                Access::request(sim, Access::bridge(sim, 0)))
                ++requests;
        };
    }
    assert(sim.runClockEvents(37) == 37);
    assert(sim.runClockEvents(173) == 173);
    assert(sim.epochFreeRunCount() == (workers > 1 ? 2 : 0));
    assert(!sim.totalTransportOverflowEvents());
    if (dynamic) {
        assert(requests == 2 && sim.rebalanceCount() == 2);
        Access::assertIdle(sim);
    }
    Result result;
    for (const auto* unit : {w, r, w2, r2}) result.state.push_back(unit->events);
    for (const auto* fifo : fifos) {
        const auto state = fifo->diagnostics();
        result.state.push_back({state.write_binary, state.read_binary, state.write_sync,
                                state.read_sync, state.full, state.empty, state.output_valid,
                                state.ram_occupancy, state.writes, state.reads});
    }
    return result;
}

int main(int argc, char** argv) {
    using Solver = TickSimulationConfig::PartitionSolverType;
    for (bool clustered : {false, true}) {
        const auto reference = runWorkerLimit(1, Solver::Weighted, false, clustered);
        for (auto solver : {Solver::Weighted, Solver::SA})
            for (size_t workers : {2, 32})
                for (bool dynamic : {false, true})
                    assert(runWorkerLimit(workers, solver, dynamic, clustered) == reference);
    }
    export_fixtures = argc > 1;
    fixture_root =
        export_fixtures
            ? std::filesystem::path(argv[1])
            : std::filesystem::temp_directory_path() /
                  ("chronon-scaling-" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    for (bool coincident : {false, true}) {
        const auto reference = run(1, false, false, coincident, false, true);
        assert(run(1, false, true, coincident, true, true) == reference);
        assert(run(4, true, false, coincident, false, true, false, false, true) == reference);
        for (size_t workers : {2, 4, 8})
            for (bool dynamic : {false, true})
                assert(run(workers, dynamic, true, coincident, false, true, dynamic) == reference);
    }
    const auto clustered = run(1, false, false, true, false, false, false, true);
    assert(run(4, true, true, true, false, false, true, true) == clustered);
    if (!export_fixtures) std::filesystem::remove(fixture_root);
    std::cout
        << "shared lanes, sparse domains, fallback callbacks, segmentation and migration passed\n";
}
