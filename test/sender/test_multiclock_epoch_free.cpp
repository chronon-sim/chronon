// SPDX-License-Identifier: MPL-2.0
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <thread>
#include <tuple>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;

struct Recorded : TickableUnit {
    std::vector<uint64_t> events;
    std::thread::id worker;
    explicit Recorded(std::string name) : TickableUnit(std::move(name)) {}
    void record(uint64_t a, uint64_t b = 0, uint64_t c = 0) {
        worker = std::this_thread::get_id();
        events.insert(events.end(), {localCycle(), a, b, c});
        clockEvent(ClockEventKind::User, 0, a);
    }
};

struct Source : Recorded {
    OutPort<uint64_t> out{this, "out", 1};
    AsyncReadPort<uint64_t> ack{this, "ack"};
    uint64_t sent = 0;
    bool feedback;
    Source(std::string name, bool feedback) : Recorded(std::move(name)), feedback(feedback) {}
    void tick() override {
        uint64_t accepted = 0, returned = 0;
        if (feedback) {
            if (auto packet = ack.take()) returned = packet->data;
            ack.requestRead();
        }
        if (localCycle() % 11 != 4 && out.canSend()) {
            const auto value = (feedback ? 1'000'000 : 2'000'000) + sent + 1;
            if (out.send(value)) {
                ++sent;
                accepted = value;
            }
        }
        record(accepted, returned);
    }
};

struct Writer : Recorded {
    InPort<uint64_t> in{this, "in", 8};
    AsyncWritePort<uint64_t> out{this, "out"};
    std::optional<uint64_t> pending;
    Writer() : Recorded("writer") {}
    void tick() override {
        if (!pending) pending = in.tryReceive(localCycle());
        const bool full = !out.canSend();
        uint64_t sent = 0;
        if (pending) {
            CdcPacket<uint64_t> packet{*pending, *pending};
            if (out.send(std::move(packet))) {
                sent = *pending;
                pending.reset();
            }
        }
        record(sent, full, pending.value_or(0));
    }
};

struct Forward : Recorded {
    AsyncReadPort<uint64_t> in{this, "in"};
    AsyncWritePort<uint64_t> out{this, "out"};
    std::optional<CdcPacket<uint64_t>> pending;
    bool lazy;
    Forward(std::string name, bool lazy) : Recorded(std::move(name)), lazy(lazy) {}
    void tick() override {
        uint64_t received = 0, sent = 0;
        if (!pending && localCycle() % 13 < 9) {
            pending = in.take();
            if (pending) received = pending->data;
        }
        const bool read = in.requestRead();
        if (pending && out.send(std::move(*pending))) {
            sent = received ? received : pending->transaction_id;
            pending.reset();
        }
        record(received, sent, read);
        // Empty input and no output pending: only the bridge can make work.
        // Do not sleep with an unconsumed output register or a held packet.
        if (lazy && !pending && !in.outputValid() && !read) sleepForever();
    }
};

using Digest = std::vector<std::vector<uint64_t>>;

Digest runGraph(size_t threads, uint64_t whz, uint64_t rhz, uint64_t phase, size_t depth, bool lazy,
                bool segmented, uint32_t window, bool dynamic,
                const std::filesystem::path& output = {}, unsigned trace_mode = 3,
                std::vector<std::string>* trace_records = nullptr) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.max_lookahead_cycles = window;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "source", whz));
    sim.addClockDomain(ClockDomain::fromHz(2, "middle", rhz, 1, SimTime::picoseconds(phase)));
    sim.addClockDomain(ClockDomain::fromHz(3, "sink", 700'000'000));
    auto* a = sim.createUnitInDomain<Source>(1, "a", true);
    auto* b = sim.createUnitInDomain<Source>(1, "b", false);
    auto* w = sim.createUnitInDomain<Writer>(1);
    auto* m = sim.createUnitInDomain<Forward>(2, "middle", lazy);
    auto* c = sim.createUnitInDomain<Forward>(3, "consumer", lazy);
    sim.connect(a->out, w->in, 0);
    sim.connect(b->out, w->in, 2);
    auto* first = sim.connectAsyncFifo(1, w->out, m->in, {depth, 2});
    auto* second = sim.connectAsyncFifo(2, m->out, c->in, {4, 3});
    auto* ack = sim.connectAsyncFifo(3, c->out, a->ack, {2, 2});
    if (!output.empty()) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.text = trace_mode & 1;
        trace.perfetto = trace_mode & 2;
        trace.stream_capacity = 2;
        trace.drain_batch = 1;
        trace.reverse_drain = threads > 1;
        trace.perfetto_options.clock_buffer_records = 128;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    assert(sim.useParallelExecution() == (threads > 1));
    if (segmented) {
        for (size_t i = 0; i < 20; ++i) assert(sim.runClockEvents(37) == 37);
    } else {
        assert(sim.runClockEvents(740) == 740);
    }
    sim.runUntilTime(SimTime::nanoseconds(2000));
    sim.runDomainCycles(2, 19);
    assert(sim.epochFreeRunCount() == (threads > 1 ? (segmented ? 22 : 3) : 0));
    assert(sim.totalTransportOverflowEvents() == 0);
    if (threads > 1) {
        assert(a->worker == w->worker);  // Same-cycle ordinary dependency.
        assert(a->worker != m->worker || a->worker != c->worker || a->worker != b->worker);
    }
    sim.closeClockTrace();
    if (!output.empty()) {
        const auto stats = sim.clockTraceRecorder()->stats();
        assert(!stats.dropped && stats.events > 0);
        assert(stats.native_buffer_peak_records <= 128);
        assert(stats.allocated_buffer_bytes + stats.allocated_staging_bytes <= 256 * 1024 * 1024);
        if (threads > 1) assert(stats.allocated_staging_bytes && stats.peak_staging_records);
    }
    if (trace_records) {
        const std::array<Recorded*, 5> units{a, b, w, m, c};
        for (const auto& entry : std::filesystem::directory_iterator(output)) {
            if (!entry.path().filename().string().starts_with("text-domain-")) continue;
            std::ifstream file(entry.path());
            std::string line;
            while (std::getline(file, line)) {
                if (line.starts_with('#')) continue;
                std::istringstream row(line);
                uint64_t cycle, id, phase_, transaction, fifo, value, ordinal;
                std::string kind;
                row >> cycle >> id >> kind >> phase_ >> transaction >> fifo >> value >> ordinal;
                assert(row);
                const auto unit = std::find_if(units.begin(), units.end(),
                                               [&](auto* u) { return u->id() == id; });
                assert(unit != units.end());
                std::ostringstream normalized;
                normalized << (*unit)->clockDomain().edge(cycle).floorNanoseconds() << '\t'
                           << (*unit)->fullPath() << '\t' << cycle << '\t' << kind << '\t' << phase_
                           << '\t' << transaction << '\t' << fifo << '\t' << value << '\t'
                           << ordinal;
                trace_records->push_back(normalized.str());
            }
        }
        std::sort(trace_records->begin(), trace_records->end());
        if (trace_mode & 2) {
            std::ofstream reference(output / "reference.tsv");
            reference
                << "ts\tunit\tlocal_cycle\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
            for (const auto& row : *trace_records) reference << row << '\n';
        }
    }
    const auto state = [](const auto* fifo) {
        auto s = fifo->diagnostics();
        return std::vector<uint64_t>{s.write_binary, s.read_binary, s.write_sync,   s.read_sync,
                                     s.full,         s.empty,       s.output_valid, s.ram_occupancy,
                                     s.writes,       s.reads};
    };
    return {a->events,
            b->events,
            w->events,
            m->events,
            c->events,
            state(first),
            state(second),
            state(ack),
            {sim.schedulerSteps(), sim.domainCycleCount(1), sim.domainCycleCount(2),
             sim.domainCycleCount(3), sim.lastCommittedTime().numerator(),
             sim.lastCommittedTime().denominator()}};
}

struct Runner : TickableUnit {
    std::atomic<uint64_t>& progress;
    Runner(std::string name, std::atomic<uint64_t>& progress)
        : TickableUnit(std::move(name)), progress(progress) {}
    void tick() override {
        clockEvent(ClockEventKind::User, 0, localCycle());
        progress.store(localCycle() + 1, std::memory_order_release);
    }
};
struct StalledWriter : TickableUnit {
    AsyncWritePort<uint64_t> out{this, "out"};
    std::atomic<uint64_t>& independent;
    explicit StalledWriter(std::atomic<uint64_t>& independent)
        : TickableUnit("stalled"), independent(independent) {}
    void tick() override {
        if (!localCycle()) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (independent.load(std::memory_order_acquire) < 8) {
                if (std::chrono::steady_clock::now() > deadline)
                    throw std::runtime_error("unrelated same-domain unit could not advance");
                std::this_thread::yield();
            }
        }
        out.send({localCycle() + 1, localCycle()});
    }
};
struct Reader : TickableUnit {
    AsyncReadPort<uint64_t> in{this, "in"};
    Reader() : TickableUnit("reader") {}
    void tick() override {
        in.take();
        in.requestRead();
    }
};

void noDomainBarrier(const std::filesystem::path& output = {}, unsigned mode = 3) {
    std::atomic<uint64_t> independent{0}, other{0};
    TickSimulationConfig config;
    config.num_threads = 4;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 600'000'000));
    auto* w = sim.createUnitInDomain<StalledWriter>(1, independent);
    auto* r = sim.createUnitInDomain<Reader>(2);
    auto* free = sim.createUnitInDomain<Runner>(1, "independent", independent);
    sim.createUnitInDomain<Runner>(2, "other", other);
    sim.connectAsyncFifo(1, w->out, r->in);
    if (!output.empty()) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.text = mode & 1;
        trace.perfetto = mode & 2;
        trace.stream_capacity = 2;
        trace.drain_batch = 1;
        trace.perfetto_options.clock_buffer_records = 64;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    assert(sim.useParallelExecution());
    assert(sim.assignedThread(w) != sim.assignedThread(free));
    assert(sim.runClockEvents(100) == 100);
    sim.closeClockTrace();
}

struct Stopper : Recorded {
    uint64_t stop;
    Stopper(std::string name, uint64_t stop) : Recorded(std::move(name)), stop(stop) {}
    void tick() override {
        record(localCycle());
        if (localCycle() == stop) requestTermination(TerminationReason::Completed, 0, "stop");
    }
};

void terminationAndResume(const std::filesystem::path& output = {}) {
    TickSimulationConfig config;
    config.num_threads = 4;
    config.max_lookahead_cycles = 16;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 600'000'000));
    auto* a = sim.createUnitInDomain<Stopper>(1, "a", 3);
    auto* b = sim.createUnitInDomain<Stopper>(1, "b", 3);
    sim.createUnitInDomain<Stopper>(2, "c", UINT64_MAX);
    if (!output.empty()) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.stream_capacity = 2;
        trace.drain_batch = 1;
        trace.perfetto_options.clock_buffer_records = 16;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    const auto count = sim.runClockEvents(1000);
    assert(count < 1000 && count == sim.schedulerSteps());
    assert(sim.terminationRequest().unit_name == "a");
    assert(sim.terminationRequest().physical_time == SimTime::nanoseconds(3));
    const auto boundary = sim.lastCommittedTime();
    assert(sim.terminationRequest().settled_time == boundary);
    assert(boundary >= *sim.terminationRequest().physical_time);
    assert(boundary <= SimTime::nanoseconds(19));  // A bounded admission window.
    for (auto id : {1u, 2u})
        assert(sim.domainCycleCount(id) == sim.clockDomain(id).edgeAfter(boundary));
    assert(a->localCycle() == b->localCycle());
    sim.resetTermination();
    assert(sim.runClockEvents(23) == 23);
    assert(!sim.wasTerminationRequested());
    assert(sim.schedulerSteps() == count + 23);
    sim.closeClockTrace();
}

struct Throws : TickableUnit {
    Throws() : TickableUnit("throws") {}
    void tick() override {
        if (localCycle() == 3) throw std::runtime_error("deliberate");
    }
};

void exceptionStopsPeers() {
    TickSimulationConfig config;
    config.num_threads = 2;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "clock", 1'000'000'000));
    sim.createUnitInDomain<Throws>(1);
    sim.createUnitInDomain<Stopper>(1, "peer", UINT64_MAX);
    bool caught = false;
    try {
        sim.runClockEvents(1000);
    } catch (const std::exception&) {
        caught = true;
    }
    assert(caught);
    caught = false;
    try {
        sim.runClockEvents(1);
    } catch (const std::logic_error&) {
        caught = true;
    }
    assert(caught);
}

int main(int argc, char** argv) {
    const std::filesystem::path root = argc > 1 ? argv[1] : "out/clock-parallel";
    std::filesystem::remove_all(root);
    for (const auto& [w, r, phase] : {std::tuple{1'000'000'000ULL, 1'000'000'000ULL, 0ULL},
                                      std::tuple{914'000'000ULL, 1'326'000'000ULL, 137ULL},
                                      std::tuple{2'000'000'000ULL, 500'000'000ULL, 1ULL},
                                      std::tuple{500'000'000ULL, 2'000'000'000ULL, 137ULL}}) {
        for (const bool lazy : {false, true}) {
            const auto reference = runGraph(1, w, r, phase, 2, lazy, false, 32, false);
            for (const size_t threads : {2, 4})
                for (const uint32_t window : {1, 32})
                    for (const bool dynamic : {false, true}) {
                        assert(runGraph(threads, w, r, phase, 2, lazy, false, window, dynamic) ==
                               reference);
                        assert(runGraph(threads, w, r, phase, 2, lazy, true, window, dynamic) ==
                               reference);
                    }
        }
    }
    noDomainBarrier();
    terminationAndResume();
    exceptionStopsPeers();
    for (unsigned mode : {1u, 2u, 3u}) {
        noDomainBarrier(root / ("independent-" + std::to_string(mode)), mode);
        std::vector<std::string> reference;
        const auto expected = runGraph(1, 3'000'000'000ULL, 4'000'000'000ULL, 125, 4, true, false,
                                       32, false, root / ("serial-" + std::to_string(mode)), mode,
                                       mode & 1 ? &reference : nullptr);
        for (size_t threads : {2, 4}) {
            for (uint32_t window : {1, 32}) {
                std::vector<std::string> actual;
                const auto output = root / ("graph-" + std::to_string(mode) + "-" +
                                            std::to_string(threads) + "-" + std::to_string(window));
                assert(runGraph(threads, 3'000'000'000ULL, 4'000'000'000ULL, 125, 4, true, true,
                                window, true, output, mode,
                                mode & 1 ? &actual : nullptr) == expected);
                assert(actual == reference);
            }
        }
    }
    for (size_t i = 0; i < 8; ++i) terminationAndResume(root / ("stop-" + std::to_string(i)));
    std::cout << "multiclock epoch-free: differential graphs, local progress, stop/resume and "
                 "exception propagation passed\n";
}
