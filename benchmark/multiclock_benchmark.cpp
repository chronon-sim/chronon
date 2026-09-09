// SPDX-License-Identifier: MPL-2.0
#include <sys/resource.h>

#include <chrono>
#include <filesystem>
#include <iostream>

#include "chronon/Chronon.hpp"

using namespace chronon;
struct Producer : TickableUnit {
    AsyncWritePort<uint64_t> out{this, "out"};
    uint64_t sent = 0;
    Producer() : TickableUnit("producer") {}
    void tick() override {
        CdcPacket<uint64_t> packet{sent + 1, sent + 1};
        if (out.send(std::move(packet))) ++sent;
    }
};
struct Consumer : TickableUnit {
    AsyncReadPort<uint64_t> in{this, "in"};
    uint64_t received = 0, checksum = 0;
    Consumer() : TickableUnit("consumer") {}
    void tick() override {
        if (auto packet = in.take()) {
            ++received;
            checksum += packet->data;
        }
        in.requestRead();
    }
};
struct Empty : TickableUnit {
    uint64_t value = 1;
    explicit Empty(std::string name) : TickableUnit(std::move(name)) {}
    void tick() override { value = value * 6364136223846793005ULL + 1442695040888963407ULL; }
};
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: multiclock_benchmark off|text|perfetto|both|single|calendar STEPS "
                     "[new-output-dir]\n";
        return 2;
    }
    const std::string mode = argv[1];
    const uint64_t steps = std::stoull(argv[2]);
    if (!steps || steps > 1'000'000'000) throw std::invalid_argument("steps outside [1,10^9]");
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation sim(config);
    Producer* producer = nullptr;
    Consumer* consumer = nullptr;
    if (mode == "single") {
        sim.createUnit<Empty>("one");
        sim.createUnit<Empty>("two");
    } else {
        sim.addClockDomain(ClockDomain::fromHz(1, "sm", 914'000'000));
        sim.addClockDomain(
            ClockDomain::fromHz(2, "lts", 1'326'000'000, 1, SimTime::picoseconds(137)));
        if (mode == "calendar") {
            sim.createUnitInDomain<Empty>(1, "one");
            sim.createUnitInDomain<Empty>(2, "two");
        } else {
            producer = sim.createUnitInDomain<Producer>(1);
            consumer = sim.createUnitInDomain<Consumer>(2);
            sim.connectAsyncFifo(1, producer->out, consumer->in, {16, 2});
        }
        if (mode == "text" || mode == "perfetto" || mode == "both") {
            if (argc < 4)
                throw std::invalid_argument("tracing mode requires a new output directory");
            ClockTraceRecorder::Config trace;
            trace.output_dir = argv[3];
            trace.run_id = "multiclock-benchmark";
            trace.text = mode != "perfetto";
            trace.perfetto = mode != "text";
            sim.configureClockTrace(trace);
        } else if (mode != "off" && mode != "calendar")
            throw std::invalid_argument("unknown mode");
    }
    sim.initialize();
    const auto begin = std::chrono::steady_clock::now();
    if (mode == "single")
        sim.run(steps);
    else
        sim.runClockEvents(steps);
    const auto run_end = std::chrono::steady_clock::now();
    sim.closeClockTrace();
    const auto end = std::chrono::steady_clock::now();
    ClockTraceRecorder::Stats stats;
    if (sim.clockTraceRecorder()) stats = sim.clockTraceRecorder()->stats();
    const auto seconds = std::chrono::duration<double>(end - begin).count();
    const auto run_seconds = std::chrono::duration<double>(run_end - begin).count();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    const auto unit_ticks =
        mode == "single" ? 2 * steps : sim.domainCycleCount(1) + sim.domainCycleCount(2);
    std::cout
        << "mode,steps,wall_s,run_s,events,dropped,events_per_s,ns_per_event,allocated_ingress_"
           "bytes,peak_ingress_bytes,file_bytes,maxrss_kib,sent,received,checksum,unit_ticks\n"
        << mode << ',' << steps << ',' << seconds << ',' << run_seconds << ',' << stats.events
        << ',' << stats.dropped << ',' << (stats.events / seconds) << ','
        << (stats.events ? seconds * 1e9 / stats.events : 0) << ',' << stats.allocated_buffer_bytes
        << ',' << stats.peak_buffer_bytes << ',' << stats.file_bytes << ',' << usage.ru_maxrss
        << ',' << (producer ? producer->sent : 0) << ',' << (consumer ? consumer->received : 0)
        << ',' << (consumer ? consumer->checksum : 0) << ',' << unit_ticks << '\n';
}
