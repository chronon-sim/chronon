// SPDX-License-Identifier: MPL-2.0
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <thread>
#include <tuple>
#include <vector>

#include "ClockMigrationTestAccess.hpp"
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
    std::function<void(uint64_t)> after_tick;
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
        if (after_tick) after_tick(localCycle());
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

std::vector<std::string> readTrace(const std::filesystem::path& output,
                                   std::span<Recorded* const> units, bool native) {
    std::vector<std::string> records;
    for (const auto& entry : std::filesystem::directory_iterator(output)) {
        if (!entry.path().filename().string().starts_with("text-domain-")) continue;
        std::ifstream file(entry.path());
        std::string line;
        while (std::getline(file, line)) {
            if (line.starts_with('#')) continue;
            std::istringstream row(line);
            uint64_t cycle, id, phase, transaction, fifo, value, ordinal;
            std::string kind;
            row >> cycle >> id >> kind >> phase >> transaction >> fifo >> value >> ordinal;
            assert(row);
            const auto unit =
                std::find_if(units.begin(), units.end(), [&](auto* u) { return u->id() == id; });
            assert(unit != units.end());
            std::ostringstream normalized;
            normalized << (*unit)->clockDomain().edge(cycle).floorNanoseconds() << '\t'
                       << (*unit)->fullPath() << '\t' << cycle << '\t' << kind << '\t' << phase
                       << '\t' << transaction << '\t' << fifo << '\t' << value << '\t' << ordinal;
            records.push_back(normalized.str());
        }
    }
    std::sort(records.begin(), records.end());
    if (native) {
        std::ofstream reference(output / "reference.tsv");
        reference
            << "ts\tunit\tlocal_cycle\tevent\tphase\ttransaction_id\tfifo_id\tvalue\tordinal\n";
        for (const auto& row : records) reference << row << '\n';
    }
    return records;
}

Digest runGraph(size_t threads, uint64_t whz, uint64_t rhz, uint64_t phase, size_t depth, bool lazy,
                bool segmented, uint32_t window, bool dynamic,
                const std::filesystem::path& output = {}, unsigned trace_mode = 3,
                std::vector<std::string>* trace_records = nullptr, bool hot_migrations = false) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    if (hot_migrations) config.rebalance_check_interval_cycles = UINT64_MAX;
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
    size_t requests = 0;
    if (hot_migrations) {
        assert(dynamic && threads > 1);
        using Access = sender::DynamicMigrationTestAccess;
        const std::vector<size_t> actors{Access::bridge(sim, 0),  Access::cluster(sim, w),
                                         Access::bridge(sim, 1),  Access::bridge(sim, 2),
                                         Access::cluster(sim, b), Access::cluster(sim, m),
                                         Access::cluster(sim, c), Access::bridge(sim, 0)};
        w->after_tick = [&, actors](uint64_t cycle) {
            // The first request is inside the writer's tick: its bridge has
            // already begun, but cannot commit until this tick completes.
            if (requests < actors.size() && cycle >= 11 + 64 * requests &&
                Access::request(sim, actors[requests]))
                ++requests;
        };
    }
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
        if (!hot_migrations)
            assert(a->worker != m->worker || a->worker != c->worker || a->worker != b->worker);
    }
    if (hot_migrations) {
        assert(requests == 8 && sim.rebalanceCount() == requests);
        sender::DynamicMigrationTestAccess::assertIdle(sim);
        assert(sim.assignedThread(a) == sim.assignedThread(w));
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
        *trace_records = readTrace(output, units, trace_mode & 2);
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

void externalTerminationBetweenRuns(size_t threads, uint64_t warmup) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = threads == 4;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 3'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 600'000'000, 1, SimTime::picoseconds(137)));
    auto* a = sim.createUnitInDomain<Stopper>(1, "a", UINT64_MAX);
    auto* b = sim.createUnitInDomain<Stopper>(2, "b", UINT64_MAX);
    if (warmup) assert(sim.runClockEvents(warmup) == warmup);
    const auto boundary = sim.lastCommittedTime();
    const auto ac = sim.domainCycleCount(1), bc = sim.domainCycleCount(2);
    const auto runs_before = sim.epochFreeRunCount();
    const Digest events_before{a->events, b->events};
    const std::array<std::function<uint64_t()>, 8> runs{
        [&] { return sim.runClockEvents(100); },
        [&] { return sim.runClockEvents(0); },
        [&] { return sim.runUntilTime(SimTime::nanoseconds(1000)); },
        [&] { return sim.runUntilTime(boundary); },
        [&] { return sim.runDomainCycles(1, 100); },
        [&] { return sim.runDomainCycles(1, 0); },
        [&] { return sim.drainCdc(100); },  // Already drained; no batch is attempted.
        [&] { return sim.drainCdc(0); }};
    for (const auto& run : runs) {
        // Fresh request for EACH entry point, so a previous run cannot hide
        // missing publication on a zero-work path.
        sim.requestTermination(TerminationReason::UserInterrupted, 7, "between clock runs");
        const auto original = sim.terminationRequest();
        assert(!original.settled_time);
        assert(run() == 0);
        assert(sim.useParallelExecution() == (threads > 1));
        const auto& request = sim.terminationRequest();
        assert(request.settled_time == boundary && sim.lastCommittedTime() == boundary);
        assert(request.reason == original.reason && request.exit_code == original.exit_code);
        assert(request.cycle == original.cycle && request.physical_time == original.physical_time);
        assert(request.clock_domain_id == original.clock_domain_id);
        assert(request.unit_name == original.unit_name && request.message == original.message);
        assert(sim.schedulerSteps() == warmup && sim.epochFreeRunCount() == runs_before);
        assert(sim.domainCycleCount(1) == ac && sim.domainCycleCount(2) == bc);
        assert(a->localCycle() == ac && b->localCycle() == bc);
        assert((Digest{a->events, b->events} == events_before));
        assert(sim.runClockEvents(1) == 0 && sim.terminationRequest().settled_time == boundary);
        sim.resetTermination();
        assert(!sim.wasTerminationRequested() && !sim.terminationRequest().settled_time);
        assert(sim.runClockEvents(0) == 0 && !sim.terminationRequest().settled_time);
    }
    assert(sim.runClockEvents(23) == 23);
    assert(sim.schedulerSteps() == warmup + 23 && !sim.wasTerminationRequested());
    assert(!sim.terminationRequest().settled_time);
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

struct Loopback : Recorded {
    AsyncWritePort<uint64_t> out{this, "out"};
    AsyncReadPort<uint64_t> in{this, "in"};
    uint64_t sent = 0, received = 0;
    std::function<void(uint64_t)> after_tick;
    Loopback() : Recorded("loopback") {}
    void tick() override {
        if (localCycle() % 13 < 9) {
            if (auto packet = in.take()) {
                assert(packet->transaction_id == ++received && packet->data == received);
            }
        }
        const bool read = in.requestRead();
        if (out.send({sent + 1, sent + 1})) ++sent;
        record(sent, received, read);
        if (after_tick) after_tick(localCycle());
    }
};

Digest runSelfLoop(size_t threads, const std::filesystem::path& output, unsigned mode, bool dynamic,
                   std::vector<std::string>& records) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "loop", 3'000'000'000));
    auto* loop = sim.createUnitInDomain<Loopback>(1);
    // Keep a second cluster so this tests the parallel path, not its fallback.
    auto* peer = sim.createUnitInDomain<Stopper>(1, "peer", UINT64_MAX);
    auto* fifo = sim.connectAsyncFifo(1, loop->out, loop->in, {2, 2});
    if (mode) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.text = mode & 1;
        trace.perfetto = mode & 2;
        trace.stream_capacity = 2;
        trace.drain_batch = 1;
        trace.reverse_drain = threads > 1;
        trace.perfetto_options.clock_buffer_records = 128;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    assert(sim.useParallelExecution() == (threads > 1));
    size_t requests = 0;
    if (dynamic) {
        using Access = sender::DynamicMigrationTestAccess;
        const std::array actors{Access::bridge(sim, 0), Access::cluster(sim, loop)};
        loop->after_tick = [&, actors](uint64_t cycle) {
            if (requests < actors.size() && cycle >= 17 + 64 * requests &&
                Access::request(sim, actors[requests]))
                ++requests;
        };
    }
    for (size_t i = 0; i < 8; ++i) assert(sim.runClockEvents(37) == 37);
    assert(loop->received > 20 && sim.totalTransportOverflowEvents() == 0);
    if (dynamic) {
        assert(requests == 2 && sim.rebalanceCount() == requests);
        sender::DynamicMigrationTestAccess::assertIdle(sim);
    }
    sim.closeClockTrace();
    if (mode) {
        const auto stats = sim.clockTraceRecorder()->stats();
        assert(!stats.dropped && stats.events > 2 * 296);
        if (mode & 1) {
            const std::array<Recorded*, 2> units{loop, peer};
            records = readTrace(output, units, mode & 2);
            assert(records.size() == stats.events);
        }
    }
    const auto s = fifo->diagnostics();
    assert(s.writes == loop->sent && s.reads >= loop->received);
    return {loop->events,
            peer->events,
            {s.write_binary, s.read_binary, s.write_sync, s.read_sync, s.full, s.empty,
             s.output_valid, s.ram_occupancy, s.writes, s.reads}};
}

int main(int argc, char** argv) {
    const std::filesystem::path root = argc > 1 ? argv[1] : "out/clock-parallel";
    std::filesystem::remove_all(root);
    for (size_t threads : {1, 2, 4})
        for (uint64_t warmup : {0, 37}) externalTerminationBetweenRuns(threads, warmup);
    Digest self_loop_reference;
    for (unsigned mode : {0u, 1u, 2u, 3u}) {
        std::vector<std::string> reference;
        const auto prefix = "self-loop-" + std::to_string(mode);
        const auto expected = runSelfLoop(1, root / (prefix + "-serial"), mode, false, reference);
        if (!mode) self_loop_reference = expected;
        assert(expected == self_loop_reference);
        for (size_t threads : {2, 4}) {
            for (bool dynamic : {false, true}) {
                std::vector<std::string> actual;
                const auto output =
                    root / (prefix + "-" + std::to_string(threads) + "-" + std::to_string(dynamic));
                assert(runSelfLoop(threads, output, mode, dynamic, actual) == expected);
                assert(actual == reference);
            }
        }
    }
    for (const auto& [w, r, phase] : {std::tuple{1'000'000'000ULL, 1'000'000'000ULL, 0ULL},
                                      std::tuple{914'000'000ULL, 1'326'000'000ULL, 137ULL},
                                      std::tuple{2'000'000'000ULL, 500'000'000ULL, 1ULL},
                                      std::tuple{500'000'000ULL, 2'000'000'000ULL, 137ULL}}) {
        for (const bool lazy : {false, true}) {
            const auto reference = runGraph(1, w, r, phase, 2, lazy, false, 32, false);
            for (const size_t threads : {2, 4})
                for (const uint32_t window : {1, 32})
                    assert(runGraph(threads, w, r, phase, 2, lazy, true, window, true, {}, 3,
                                    nullptr, true) == reference);
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
                                window, true, output, mode, mode & 1 ? &actual : nullptr,
                                true) == expected);
                assert(actual == reference);
            }
        }
    }
    for (size_t i = 0; i < 8; ++i) terminationAndResume(root / ("stop-" + std::to_string(i)));
    std::cout << "multiclock epoch-free: differential graphs, local progress, stop/resume and "
                 "exception propagation passed\n";
}
