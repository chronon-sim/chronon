// SPDX-License-Identifier: MPL-2.0
#include <array>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "chronon/Chronon.hpp"
#include "clock_reference.hpp"

using namespace chronon;
namespace ref = clock_reference;
constexpr uint64_t SEED = 0xc10c09141326ULL;

struct Writer : TickableUnit {
    AsyncWritePort<uint64_t> out{this, "out"};
    uint64_t sent = 0;
    unsigned pattern;
    bool accepted = false, stopped = false;
    explicit Writer(unsigned p = 0) : TickableUnit("writer"), pattern(p) {}
    void tick() override {
        accepted = false;
        if (!stopped && ref::writeEnabled(localCycle(), pattern, SEED)) {
            CdcPacket<uint64_t> packet{sent + 1, sent + 1};
            accepted = out.send(std::move(packet));
            if (accepted) ++sent;
        }
    }
};
struct Reader : TickableUnit {
    AsyncReadPort<uint64_t> in{this, "in"};
    uint64_t received = 0, taken = 0, stop_after = UINT64_MAX;
    unsigned pattern;
    bool accepted = false;
    explicit Reader(unsigned p = 0) : TickableUnit("reader"), pattern(p) {}
    void tick() override {
        accepted = false;
        taken = 0;
        if (ref::readEnabled(localCycle(), pattern, SEED)) {
            if (auto packet = in.take()) {
                taken = packet->data;
                assert(packet->transaction_id == taken && taken == received + 1);
                ++received;
                if (received == stop_after)
                    requestTermination(TerminationReason::Completed, 0, "read target");
            }
            accepted = in.requestRead();
        }
    }
};
template <typename F>
void rejects(F&& f) {
    bool caught = false;
    try {
        f();
    } catch (const std::exception&) {
        caught = true;
    }
    assert(caught);
}

void runCase(uint64_t whz, uint64_t rhz, uint64_t phase, size_t depth, size_t stages,
             unsigned pattern, uint64_t steps, size_t threads = 1, bool reverse = false,
             const std::filesystem::path& trace = {}, bool lossy = false, bool compress = true,
             size_t drain_batch = 11) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    TickSimulation sim(config);
    if (reverse) {
        sim.addClockDomain(ClockDomain::fromHz(2, "lts", rhz, 1, SimTime::picoseconds(phase)));
        sim.addClockDomain(ClockDomain::fromHz(1, "sm", whz));
    } else {
        sim.addClockDomain(ClockDomain::fromHz(1, "sm", whz));
        sim.addClockDomain(ClockDomain::fromHz(2, "lts", rhz, 1, SimTime::picoseconds(phase)));
    }
    Writer* writer;
    Reader* reader;
    if (reverse) {
        reader = sim.createUnitInDomain<Reader>(2, pattern);
        writer = sim.createUnitInDomain<Writer>(1, pattern);
    } else {
        writer = sim.createUnitInDomain<Writer>(1, pattern);
        reader = sim.createUnitInDomain<Reader>(2, pattern);
    }
    auto* fifo = sim.connectAsyncFifo(17, writer->out, reader->in, {depth, stages});
    if (!trace.empty()) {
        ClockTraceRecorder::Config recording;
        recording.output_dir = trace;
        recording.run_id = "reference-9141326";
        recording.stream_capacity = lossy ? 2 : 64;
        recording.drain_batch = lossy ? 1 : drain_batch;
        recording.reverse_drain = reverse;
        recording.lossless = !lossy;
        recording.perfetto_options.checkpoint_interval_packets = 7;
        recording.perfetto_options.compress = compress;
        sim.configureClockTrace(recording);
    }
    sim.initialize();
    assert(!sim.useParallelExecution() && !sim.parallelFallbackReason().empty());
    assert(sim.epochFreeRunCount() == 0);
    ref::Fifo oracle(depth, stages, !trace.empty());
    uint64_t wc = 0, rc = 0;
    std::array<std::array<uint64_t, 8>, 32> recent{};
    for (uint64_t step = 0; step < steps; ++step) {
        const auto wt = ref::edge(whz, 0, wc), rt = ref::edge(rhz, phase, rc);
        const bool w = !(rt < wt), r = !(wt < rt);
        const auto time = w ? wt : rt;
        const bool wen = ref::writeEnabled(wc, pattern, SEED),
                   ren = ref::readEnabled(rc, pattern, SEED);
        assert(sim.runClockEvents(1) == 1);
        oracle.step(w, r, wen, ren, wc, rc);
        const auto state = fifo->diagnostics();
        recent[step % recent.size()] = {step,
                                        wc,
                                        rc,
                                        state.write_binary,
                                        state.read_binary,
                                        state.full,
                                        state.empty,
                                        state.output_valid};
        const auto actual_time = sim.lastCommittedTime();
        const bool okay =
            (!w || writer->accepted == oracle.accepted_write) &&
            (!r ||
             (reader->accepted == oracle.accepted_read && reader->taken == oracle.last_consumed)) &&
            state.full == oracle.full && state.empty == oracle.empty &&
            state.output_valid == bool(oracle.output) &&
            state.ram_occupancy == oracle.memory.size() &&
            state.write_binary == oracle.writes % (2 * depth) &&
            state.read_binary == oracle.reads % (2 * depth) &&
            ref::Wide(actual_time.numerator()) * time.denominator * 1'000'000'000'000ULL ==
                time.numerator * actual_time.denominator();
        if (!okay) {
            std::cerr << "seed=" << SEED << " whz=" << whz << " rhz=" << rhz
                      << " phase_ps=" << phase << " depth=" << depth << " stages=" << stages
                      << " pattern=" << pattern << '\n';
            for (uint64_t n = step > 31 ? step - 31 : 0; n <= step; ++n) {
                for (auto value : recent[n % recent.size()]) std::cerr << value << ' ';
                std::cerr << '\n';
            }
            throw std::runtime_error("multiclock reference mismatch (last 32 batches above)");
        }
        wc += w;
        rc += r;
        assert(writer->localCycle() == wc && reader->localCycle() == rc);
        assert(sim.domainCycleCount(1) == wc && sim.domainCycleCount(2) == rc);
    }
    sim.closeClockTrace();
    if (!trace.empty()) {
        const auto stats = sim.clockTraceRecorder()->stats();
        assert(stats.events + stats.dropped == oracle.events.size());
        if (!lossy) assert(!stats.dropped);
        std::ofstream expected(trace / "reference.tsv");
        expected
            << "ts\tunit\tlocal_cycle\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
        const char* names[] = {"",          "fifo.write",  "fifo.visible",
                               "fifo.read", "fifo.output", "fifo.consume",
                               "fifo.full", "fifo.empty"};
        for (const auto& event : oracle.events) {
            const auto t = event.domain == 1 ? ref::edge(whz, 0, event.cycle)
                                             : ref::edge(rhz, phase, event.cycle);
            expected << t.ns() << '\t' << (event.domain == 1 ? "writer" : "reader") << '\t'
                     << event.cycle << '\t' << names[event.kind] << '\t' << unsigned(event.phase)
                     << '\t' << event.transaction << "\t17\t" << event.value << '\t'
                     << event.ordinal << '\n';
        }
    }
    // A termination request is observed after the complete physical instant.
    reader->pattern = 0;
    reader->stop_after = reader->received + 3;
    assert(sim.runClockEvents(1000) < 1000);
    assert(sim.wasTerminationRequested());
    const auto& termination = sim.terminationRequest();
    assert(termination.clock_domain_id == 2 && termination.physical_time);
    assert(*termination.physical_time == sim.clockDomain(2).edge(termination.cycle));
    assert(*termination.physical_time == sim.lastCommittedTime());
    sim.resetTermination();
    writer->stopped = true;
    reader->stop_after = UINT64_MAX;
    sim.drainCdc(10000);
    assert(sim.cdcDrained() && writer->sent == reader->received);
    rejects([&] { sim.assignClockDomain(*writer, 2); });
    rejects([&] { sim.addClockDomain(ClockDomain::fromHz(3, "late_clock", 100)); });
    rejects([&] { sim.run(1); });
    rejects([&] { writer->out.canSend(); });
}

struct Phased : sender::PhasedTickableUnit<Phased> {
    StageReg<uint64_t> reg;
    explicit Phased(std::string name) : PhasedTickableUnit(std::move(name)) {}
    template <ValidPhase P>
    void tickPhase() {
        reg.template beginCycle<P>();
        if (localCycle()) {
            assert(reg.template valid<P>());
            assert(reg.template consume<P>() == localCycle() - 1);
        }
        reg.template write<P>(localCycle());
    }
};
struct Local : TickableUnit {
    OutPort<uint64_t> out{this, "out"};
    InPort<uint64_t> in{this, "in"};
    bool source;
    uint64_t received = 0;
    Local(std::string name, bool source_) : TickableUnit(std::move(name)), source(source_) {}
    void tick() override {
        if (source) {
            assert(out.send(localCycle()));
        } else if (auto packet = in.tryReceive(localCycle())) {
            assert(*packet == localCycle());
            ++received;
        }
    }
};
void topologyAndLimits() {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 600'000'000, 1, SimTime::picoseconds(137)));
    auto* b = sim.createUnitInDomain<Phased>(2, "slow");
    auto* a = sim.createUnitInDomain<Phased>(1, "fast");
    auto* sink = sim.createUnitInDomain<Local>(2, "a_sink", false);
    auto* source = sim.createUnitInDomain<Local>(2, "z_source", true);
    sim.connect(source->out, sink->in, 0);
    sim.initialize();
    assert(sim.runUntilTime(SimTime{}) == 0);
    sim.runUntilTime(SimTime::nanoseconds(10));
    assert(a->localCycle() == 10 && b->localCycle() == 6 && sink->received == 6);
    sim.runDomainCycles(2, 100);
    assert(b->localCycle() == 106 && sink->received == 106);
    assert(a->localCycle() != b->localCycle());
    rejects([&] { source->out.connect(&sink->in, 0); });
    TickSimulation invalid(config);
    auto* x = invalid.createUnit<Local>("x", true);
    auto* y = invalid.createUnit<Local>("y", false);
    x->out.connect(&y->in, 0);  // Deliberately bypass sim.connect registration.
    invalid.addClockDomain(ClockDomain::fromHz(1, "other", 914'000'000));
    invalid.assignClockDomain(*y, 1);
    rejects([&] { invalid.initialize(); });
    TickSimulation cross(config);
    cross.addClockDomain(ClockDomain::fromHz(1, "other", 1'001'000'000));
    x = cross.createUnitInDomain<Local>(0, "x", true);
    y = cross.createUnitInDomain<Local>(1, "y", false);
    rejects([&] { cross.connect(x->out, y->in); });
}

int main(int argc, char** argv) {
    topologyAndLimits();
    const std::array<std::pair<uint64_t, uint64_t>, 9> ratios{{{1'000'000'000, 1'000'000'000},
                                                               {2'000'000'000, 500'000'000},
                                                               {500'000'000, 2'000'000'000},
                                                               {1'500'000'000, 1'000'000'000},
                                                               {1'000'000'000, 1'500'000'000},
                                                               {1'000'000'000, 600'000'000},
                                                               {1'000'000'000, 1'001'000'000},
                                                               {914'000'000, 1'326'000'000},
                                                               {1'326'000'000, 914'000'000}}};
    size_t cases = 0;
    for (auto [w, r] : ratios)
        for (uint64_t phase : {0, 1, 137, 500})
            for (size_t depth : {2, 4, 16})
                for (size_t stages : {2, 3, 5})
                    for (unsigned pattern : {0, 1, 2}) {
                        runCase(w, r, phase, depth, stages, pattern, 3000);
                        ++cases;
                    }
    for (auto [w, r] : ratios)
        for (size_t threads : {2, 4}) {
            runCase(w, r, 137, 8, 2, 2, 10000, threads, true);
        }
    runCase(914'000'000, 1'326'000'000, 137, 16, 3, 2, 1'000'000);
    if (argc > 1) {
        const std::filesystem::path root(argv[1]);
        runCase(914'000'000, 1'326'000'000, 137, 8, 2, 2, 10000, 1, false, root / "serial");
        runCase(914'000'000, 1'326'000'000, 137, 8, 2, 2, 10000, 4, true, root / "fallback");
        runCase(914'000'000, 1'326'000'000, 137, 8, 2, 2, 10000, 4, true, root / "lossy", true);
        runCase(1'000'000'000, 1'000'000'000, 0, 4, 3, 0, 3000, 1, false, root / "coincident",
                false, false);
        runCase(1'000'000'000, 1'001'000'000, 1, 4, 2, 0, 10000, 1, false, root / "near");
        runCase(4'000'000'000ULL, 4'000'000'000ULL, 0, 8, 2, 2, 4000, 1, false,
                root / "collision-forward");
        runCase(4'000'000'000ULL, 4'000'000'000ULL, 0, 8, 2, 2, 4000, 4, true,
                root / "collision-reverse");
        runCase(4'000'000'000ULL, 4'000'000'000ULL, 0, 8, 2, 2, 4000, 1, false,
                root / "collision-batch1", false, false, 1);
        runCase(4'000'000'000ULL, 4'000'000'000ULL, 0, 8, 2, 2, 4000, 4, true,
                root / "collision-batch64", false, true, 64);
        runCase(4'000'000'000ULL, 4'000'000'000ULL, 0, 8, 2, 2, 4000, 4, true,
                root / "collision-lossy", true);
        runCase(3'000'000'000ULL, 4'000'000'000ULL, 125, 8, 2, 2, 4000, 4, true,
                root / "collision-phase");
    }
    std::cout << "multiclock: " << cases
              << " ratio/phase/depth/stage/stimulus cases, thread/order equivalence, long stress "
                 "and drain passed\n";
}
