// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;

struct DenseUnit : TickableUnit {
    size_t per_edge;
    bool burst;
    uint64_t events = 0, digest = 0;
    DenseUnit(std::string name, size_t count, bool burst_)
        : TickableUnit(std::move(name)), per_edge(count), burst(burst_) {}
    void tick() override {
        const auto count = burst && localCycle() != 63 ? size_t{1} : per_edge;
        for (size_t i = 0; i < count; ++i) {
            ++events;
            digest = digest * 6364136223846793005ULL + localCycle() + i;
            clockEvent(ClockEventKind::User, 0, events);
        }
    }
};

struct Result {
    uint64_t events = 0, digest = 0;
    ClockTraceRecorder::Stats stats;
};

Result run(const std::filesystem::path& output, size_t units, size_t per_edge,
           size_t native_capacity, bool burst = false, unsigned mode = 2, bool reverse = false,
           bool lossy = false, bool sub_ns = false) {
    std::filesystem::remove_all(output);
    TickSimulationConfig config;
    config.enable_parallel = reverse;
    config.num_threads = reverse ? 4 : 1;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "sm", sub_ns ? 4'000'000'000ULL : 1'000'000'000ULL));
    std::vector<DenseUnit*> producers(units);
    for (size_t i = 0; i < units; ++i) {
        const auto index = reverse ? units - i - 1 : i;
        producers[index] =
            sim.createUnitInDomain<DenseUnit>(1, "unit-" + std::to_string(index), per_edge, burst);
    }
    if (mode) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = output;
        trace.text = mode & 1;
        trace.perfetto = mode & 2;
        trace.lossless = !lossy;
        trace.reverse_drain = reverse;
        trace.stream_capacity = lossy ? 2 : 256;
        trace.drain_batch = lossy ? 1 : 256;
        trace.perfetto_options.clock_buffer_records = native_capacity;
        trace.perfetto_options.checkpoint_interval_packets = 257;
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    assert(sim.useParallelExecution() == (reverse && units > 1));
    assert(sim.runClockEvents(64) == 64);
    // Resume across multiple API calls, including a partially filled ns bucket.
    assert(sim.runClockEvents(3) == 3);
    sim.closeClockTrace();
    Result result;
    for (auto* unit : producers) {
        result.events += unit->events;
        result.digest += unit->digest;
    }
    if (mode) {
        result.stats = sim.clockTraceRecorder()->stats();
        assert(result.stats.events + result.stats.dropped == result.events);
        if (!lossy) assert(result.stats.dropped == 0);
        assert(result.stats.native_buffer_peak_records <= native_capacity);
        // Independent identity/time reference for optional real TP import.
        if (!burst && per_edge == 1 && !lossy) {
            std::ofstream reference(output / "expected.tsv");
            reference << "unit\tlocal_cycle\tts\tvalue\n";
            for (auto* unit : producers)
                for (uint64_t cycle = 0; cycle < 67; ++cycle)
                    reference << unit->fullPath() << '\t' << cycle << '\t'
                              << (sub_ns ? cycle / 4 : cycle) << '\t' << cycle + 1 << '\n';
        }
    }
    return result;
}

int main(int argc, char** argv) {
    const std::filesystem::path root = argc > 1 ? argv[1] : "out/clock-trace-budget";
    const auto reference = run(root / "off", 128, 1, 65536, false, 0);
    for (unsigned mode : {1u, 2u, 3u}) {
        const auto result =
            run(root / ("mode-" + std::to_string(mode)), 128, 1, 65536, false, mode);
        assert(result.events == reference.events && result.digest == reference.digest);
    }
    const auto reversed = run(root / "reverse", 128, 1, 8192, false, 3, true);
    assert(reversed.digest == reference.digest);
    const auto lossy = run(root / "lossy", 128, 1, 65536, false, 3, false, true);
    assert(lossy.digest == reference.digest);
    run(root / "many-units", 512, 1, 65536);
    run(root / "sub-ns", 128, 1, 65536, false, 3, true, false, true);
    // Prior sparse buckets + sudden dense bucket exceed the byte budget;
    // each bucket individually fits. Publication must work mid-tick.
    run(root / "burst", 64, 80, 65536, true);
    run(root / "record-pressure", 32, 2, 128);
    run(root / "parallel-record-pressure", 32, 2, 128, false, 3, true);
    run(root / "parallel-burst", 64, 80, 65536, true, 3, true);
    run(root / "parallel-lossy", 128, 1, 65536, false, 3, true, true);
    for (const auto& [events, capacity] : {std::pair{3u, 2u}, std::pair{8000u, 65536u}}) {
        bool rejected = false;
        try {
            run(root / ("single-bucket-overflow-" + std::to_string(capacity)), 1, events, capacity);
        } catch (const std::exception& e) {
            rejected = std::string(e.what()).find("single-nanosecond bucket") != std::string::npos;
        }
        assert(rejected);
    }
    // Both peers can be blocked publishing when the backend discovers overflow.
    // Failure must release all workers, including the admission coordinator.
    for (const auto& [events, capacity] : {std::pair{3u, 2u}, std::pair{8000u, 65536u}}) {
        bool rejected = false;
        try {
            run(root / ("parallel-overflow-" + std::to_string(capacity)), 2, events, capacity,
                false, 3, true);
        } catch (const std::exception&) {
            rejected = true;
        }
        assert(rejected);
    }
    std::cout
        << "clock trace budget: dense, burst, sub-ns, ordering, modes and true overflow passed\n";
}
