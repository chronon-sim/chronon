// SPDX-License-Identifier: MPL-2.0
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "../observe/PftraceTestDecoder.hpp"
#include "ClockMigrationTestAccess.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using Access = sender::DynamicMigrationTestAccess;
inline const auto CLOCK_OBS = Category<"clock_observation", "Unified clock observation test">{};

struct Endpoint : TickableUnit, ObservableUnit {
    AsyncWritePort<uint64_t> out{this, "out"};
    AsyncReadPort<uint64_t> in{this, "in"};
    EventCounter ticks{this, "ticks", "Executed edges"};
    EventCounter transfers{this, "transfers", "Accepted transfers"};
    uint64_t count = 0, checksum = 0, stop_at = UINT64_MAX;
    bool writer;
    explicit Endpoint(std::string name, bool write)
        : TickableUnit(std::move(name)), writer(write) {}
    void tick() override {
        ++ticks;
        clockEvent(observe::ClockEventKind::User, localCycle() + 1, count);
        if (writer) {
            if (out.send({count + 1, count + 1})) {
                ++count;
                ++transfers;
            }
        } else {
            if (auto packet = in.take()) {
                checksum += packet->data;
                ++count;
                ++transfers;
            }
            in.requestRead();
        }
        event<"edge">(CLOCK_OBS, arg<"value">(count), arg<"label">(std::string_view("payload")),
                      flow(count + 1));
        pipeStage<0, "edge_slot">(CLOCK_OBS, localCycle() + 1);
        debug<"domain edge {} value {}">(localCycle(), count);
        if (localCycle() == stop_at)
            requestTermination(TerminationReason::Completed, 0, "observation test stop");
    }
};

struct Result {
    std::vector<uint64_t> values;
    std::filesystem::path output;
};

observe::ObservationYAMLConfig observation(const std::filesystem::path& output) {
    observe::ObservationYAMLConfig result;
    result.enabled = true;
    result.output_dir = output;
    result.queue_capacity = 4096;
    result.backpressure = observe::BackpressurePolicy::SpinWait;
    result.counters.periodic_dump_cycles = 7;
    result.counters.reference_clock = "fast";
    result.timeline.compress = false;
    result.unified_logging.enabled = true;
    result.unified_logging.trace_channel.enabled = true;
    result.unified_logging.categories.push_back({"clock_observation", true, {}});
    return result;
}

Result run(const std::filesystem::path& path, bool observed, size_t threads, bool migration,
           bool segmented, bool stop_resume = false) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = migration;
    config.rebalance_check_interval_cycles = UINT64_MAX;
    config.max_lookahead_cycles = 8;
    config.timeline_trace.enabled = observed;
    TickSimulation simulation(config);
    simulation.addClockDomain(ClockDomain::fromHz(1, "fast", 3'000'000'000));
    simulation.addClockDomain(
        ClockDomain::fromHz(2, "slow", 400'000'000, 1, SimTime::picoseconds(137)));
    if (observed) {
        simulation.configureObservation(observation(path));
        observe::ClockTraceRecorder::Config native;
        native.output_dir = path / "native";
        native.stream_capacity = 2;
        native.drain_batch = 1;
        simulation.configureClockTrace(native);
    }
    std::vector<Endpoint*> endpoints;
    for (size_t i = 0; i < 3; ++i) {
        auto* writer =
            simulation.createUnitInDomain<Endpoint>(1, "writer" + std::to_string(i), true);
        auto* reader =
            simulation.createUnitInDomain<Endpoint>(2, "reader" + std::to_string(i), false);
        simulation.connectAsyncFifo(i, writer->out, reader->in, {4, 2});
        endpoints.push_back(writer);
        endpoints.push_back(reader);
    }
    simulation.initialize();
    assert(simulation.useParallelExecution() == (threads > 1));
    auto& manager = observe::ObservationManager::instance();
    if (observed) {
        manager.reregisterAllCounters();
        manager.startBackend();
    }
    if (migration) assert(Access::request(simulation, Access::cluster(simulation, endpoints[0])));
    if (stop_resume) {
        endpoints[0]->stop_at = 23;
        simulation.runUntilTime(SimTime::nanoseconds(100));
        assert(simulation.wasTerminationRequested());
        if (observed) manager.dumpFinalCounterSnapshot(0);
        endpoints[0]->stop_at = UINT64_MAX;
        simulation.resetTermination();
    }
    if (segmented) {
        simulation.runUntilTime(SimTime::nanoseconds(17));
        if (observed) {
            manager.dumpFinalCounterSnapshot(0);
            manager.dumpFinalCounterSnapshot(0);  // same revision must not emit/reset twice
        }
        simulation.runUntilTime(SimTime::nanoseconds(31));
    }
    simulation.runUntilTime(SimTime::nanoseconds(100));
    if (migration) {
        assert(simulation.rebalanceCount() > 0);
        Access::assertIdle(simulation);
    }
    Result result;
    for (auto* unit : endpoints) {
        result.values.insert(result.values.end(),
                             {unit->localCycle(), unit->count, unit->checksum});
    }
    if (observed) {
        manager.dumpFinalCounterSnapshot(0);
        manager.dumpFinalCounterSnapshot(0);
        simulation.writeTimelineTrace();
        result.output = manager.backend()->outputDir();
        manager.stopBackend();
        simulation.closeClockTrace();
        const auto trace = pftrace_test::decodeFile(result.output / "timeline.pftrace");
        size_t edges = 0, pipelines = 0;
        for (const auto& event : trace.events) {
            if (event.name == "edge") {
                ++edges;
                assert(event.uint_annotations.contains("domain_id"));
                assert(event.uint_annotations.contains("local_cycle"));
                assert(event.uint_annotations.contains("time_num"));
                assert(event.uint_annotations.contains("time_den"));
                assert(event.string_annotations.at("label") == "payload");
            }
            if (event.type == 1 && event.uint_annotations.contains("domain_id")) ++pipelines;
        }
        size_t total_edges = 0;
        for (auto* endpoint : endpoints) total_edges += endpoint->localCycle();
        assert(edges == total_edges && pipelines == total_edges);
        std::cout << "trace=" << result.output << '\n';
    }
    return result;
}

void phaseGap(const std::filesystem::path& path) {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    simulation.addClockDomain(
        ClockDomain::fromHz(1, "fast", 1'000'000, 1, SimTime::nanoseconds(50)));
    simulation.configureObservation(observation(path));
    auto* endpoint = simulation.createUnitInDomain<Endpoint>(1, "writer", true);
    // Unconnected write endpoints must not send; use a reader with empty input instead.
    endpoint->writer = false;
    simulation.initialize();
    auto& manager = observe::ObservationManager::instance();
    manager.reregisterAllCounters();
    manager.startBackend();
    assert(simulation.runUntilTime(SimTime::nanoseconds(20)) == 0);
    assert(simulation.runClockEvents(0) == 0);
    manager.dumpFinalCounterSnapshot(0);
    assert(simulation.runUntilTime(SimTime::nanoseconds(40)) == 0);
    manager.dumpFinalCounterSnapshot(0);
    assert(endpoint->localCycle() == 0);
    manager.stopBackend();
    std::ifstream csv(manager.backend()->outputDir() / "counters.csv");
    std::string header, line, extra;
    std::getline(csv, header);
    std::getline(csv, line);
    std::getline(csv, extra);
    assert(header.starts_with("time_num,time_den,sample,"));
    assert(line.starts_with("1,50000000,final_before,"));
    assert(extra.empty());
}

struct LifecycleUnit : TickableUnit, ObservableUnit {
    EventCounter callbacks{this, "callbacks", "Lifecycle callbacks"};
    LifecycleUnit() : TickableUnit("lifecycle") {}
    void initialize() override {
        ++callbacks;
        event<"initialized">(CLOCK_OBS);
        info<"initialized">();
    }
    void tick() override { ++callbacks; }
    void finalize() override {
        ++callbacks;
        event<"finalized">(CLOCK_OBS);
        info<"finalized">();
    }
};

void lifecycle(const std::filesystem::path& path) {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    simulation.addClockDomain(
        ClockDomain::fromHz(1, "fast", 1'000'000, 1, SimTime::nanoseconds(50)));
    simulation.configureObservation(observation(path));
    simulation.createUnitInDomain<LifecycleUnit>(1);
    simulation.initialize();
    auto& manager = observe::ObservationManager::instance();
    manager.reregisterAllCounters();
    manager.startBackend();
    assert(simulation.runUntilTime(SimTime::nanoseconds(20)) == 0);
    manager.dumpFinalCounterSnapshot(0);  // Before finalize: initialization residual only.
    simulation.finalize();
    simulation.finalize();
    manager.dumpFinalCounterSnapshot(0);
    manager.dumpFinalCounterSnapshot(0);
    const auto output = manager.backend()->outputDir();
    manager.stopBackend();
    const auto trace = pftrace_test::decodeFile(output / "timeline.pftrace");
    size_t found = 0;
    for (const auto& event : trace.events) {
        if (event.name != "initialized" && event.name != "finalized") continue;
        const bool initial = event.name == "initialized";
        assert(event.timestamp == (initial ? 0 : 20));
        assert(event.string_annotations.at("lifecycle") == (initial ? "initialize" : "finalize"));
        ++found;
    }
    assert(found == 2);
    std::ifstream csv(output / "counters.csv");
    std::string header, row;
    std::getline(csv, header);
    std::getline(csv, row);
    assert(header.find("lifecycle.callbacks") != std::string::npos);
    uint64_t total = 0;
    size_t rows = 0;
    do {
        assert(row.starts_with("1,50000000,final_before,"));
        std::stringstream names(header), values(row);
        std::string name, value;
        bool counted = false;
        while (std::getline(names, name, ',') && std::getline(values, value, ',')) {
            if (name == "lifecycle.callbacks") {
                total += std::stoull(value);
                counted = true;
            }
        }
        assert(counted);
        ++rows;
    } while (std::getline(csv, row));
    assert(rows == 2 && total == 2);
}

struct SpanUnit : TickableUnit, ObservableUnit {
    EventCounter edges{this, "edges", "Executed edges"};
    SpanUnit() : TickableUnit("span") {}
    void tick() override {
        ++edges;
        if (localCycle() == 0) spanBegin<"work">(CLOCK_OBS, "span_work"_ev);
        if (localCycle() == 4) spanEnd<"work">();
    }
};

void namedSpanAndFreshSession(const std::filesystem::path& path) {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    for (bool fresh : {false, true}) {
        TickSimulation simulation(config);
        simulation.addClockDomain(
            ClockDomain::fromHz(1, "fast", 3'000'000'000, 1, SimTime::picoseconds(137)));
        simulation.configureObservation(observation(path / (fresh ? "fresh" : "span")));
        simulation.createUnitInDomain<SpanUnit>(1);
        simulation.initialize();
        auto& manager = observe::ObservationManager::instance();
        manager.reregisterAllCounters();
        manager.startBackend();
        assert(simulation.runClockEvents(fresh ? 0 : 5) == (fresh ? 0 : 5));
        manager.dumpFinalCounterSnapshot(0);
        const auto output = manager.backend()->outputDir();
        manager.stopBackend();
        if (fresh) {
            std::ifstream csv(output / "counters.csv");
            std::string header, row;
            std::getline(csv, header);
            std::getline(csv, row);
            assert(row.starts_with("0,1,final_before,"));
        } else {
            const auto trace = pftrace_test::decodeFile(output / "timeline.pftrace");
            uint64_t track = 0;
            size_t ended = 0;
            for (const auto& event : trace.events) {
                if (event.name == "span_work") {
                    track = event.track_uuid;
                    assert(event.timestamp == 0 && event.type == 1);
                    assert(event.uint_annotations.at("time_num") == 137);
                    assert(event.uint_annotations.at("time_den") == 1'000'000'000'000);
                }
                if (track && event.track_uuid == track && event.type == 2) {
                    assert(event.timestamp == 1);
                    ++ended;
                }
            }
            assert(track && ended == 1);
        }
    }
}

struct Flood : TickableUnit, ObservableUnit {
    size_t records;
    explicit Flood(size_t count, std::string name)
        : TickableUnit(std::move(name)), records(count) {}
    void tick() override {
        for (size_t i = 0; i < records; ++i) event<"flood">(CLOCK_OBS, arg<"index">(i));
    }
};

void failure(const std::filesystem::path& path, bool budget, size_t threads = 1) {
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    TickSimulation simulation(config);
    simulation.addClockDomain(ClockDomain::fromHz(1, "fast", 1'000'000'000));
    auto observe = observation(path);
    observe.counters.periodic_dump_cycles = 0;
    if (budget)
        observe.service_buffer_bytes = 65536;
    else
        observe.timeline.file = "/dev/full";
    simulation.configureObservation(observe);
    for (size_t i = 0; i < threads; ++i)
        simulation.createUnitInDomain<Flood>(1, budget ? 5000 : 10, "flood" + std::to_string(i));
    simulation.initialize();
    auto& manager = observe::ObservationManager::instance();
    manager.reregisterAllCounters();
    bool failed = false;
    try {
        manager.startBackend();
        simulation.runClockEvents(10);
        manager.stopBackend();
    } catch (const std::exception& error) {
        failed = true;
        const std::string message = error.what();
        assert(message.find(budget ? "service_buffer_bytes" : "Perfetto trace") !=
               std::string::npos);
        try {
            manager.stopBackend();
        } catch (...) {
        }
    }
    assert(failed);
}

void overhead(const std::filesystem::path& root) {
    using Clock = std::chrono::steady_clock;
    std::vector<uint64_t> expected;
    for (size_t threads : {size_t{1}, size_t{3}}) {
        for (size_t repeat = 0; repeat < 7; ++repeat) {
            for (std::string mode : {"disabled", "counters", "full"}) {
                TickSimulationConfig config;
                config.num_threads = threads;
                config.enable_parallel = threads > 1;
                config.enable_dynamic_rebalance = false;
                config.max_lookahead_cycles = 64;
                TickSimulation simulation(config);
                simulation.addClockDomain(ClockDomain::fromHz(1, "fast", 3'000'000'000));
                simulation.addClockDomain(
                    ClockDomain::fromHz(2, "slow", 400'000'000, 1, SimTime::picoseconds(137)));
                if (mode != "disabled") {
                    auto observe = observation(root / (std::to_string(threads) + "-" + mode + "-" +
                                                       std::to_string(repeat)));
                    observe.queue_capacity = 256 * 1024;
                    observe.counters.periodic_dump_cycles = 128;
                    if (mode == "counters") observe.unified_logging.enabled = false;
                    simulation.configureObservation(observe);
                }
                std::vector<Endpoint*> endpoints;
                for (size_t i = 0; i < 3; ++i) {
                    auto* writer = simulation.createUnitInDomain<Endpoint>(
                        1, "writer" + std::to_string(i), true);
                    auto* reader = simulation.createUnitInDomain<Endpoint>(
                        2, "reader" + std::to_string(i), false);
                    simulation.connectAsyncFifo(i, writer->out, reader->in, {4, 2});
                    endpoints.push_back(writer);
                    endpoints.push_back(reader);
                }
                simulation.initialize();
                auto& manager = observe::ObservationManager::instance();
                if (mode != "disabled") {
                    manager.reregisterAllCounters();
                    manager.startBackend();
                }
                const auto begin = Clock::now();
                simulation.runUntilTime(SimTime::nanoseconds(3000));
                const auto ran = Clock::now();
                simulation.finalize();
                if (mode != "disabled") {
                    manager.dumpFinalCounterSnapshot(0);
                    manager.stopBackend();
                }
                const auto finished = Clock::now();
                std::vector<uint64_t> values;
                for (const auto* endpoint : endpoints)
                    values.insert(values.end(),
                                  {endpoint->localCycle(), endpoint->count, endpoint->checksum});
                if (expected.empty()) expected = values;
                assert(values == expected);
                const auto ns = [](auto duration) {
                    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
                };
                std::cout << "{\"threads\":" << threads << ",\"mode\":\"" << mode
                          << "\",\"repeat\":" << repeat << ",\"run_ns\":" << ns(ran - begin)
                          << ",\"run_and_drain_ns\":" << ns(finished - begin) << "}\n";
            }
        }
    }
}

int main(int argc, char** argv) {
    if (argc > 1 && std::string_view(argv[1]) == "--overhead") {
        overhead(argc > 2 ? std::filesystem::path(argv[2]) : "/tmp/chronon-observation-overhead");
        return 0;
    }
    const auto root = argc > 1 ? std::filesystem::path(argv[1])
                               : std::filesystem::temp_directory_path() /
                                     ("chronon-clock-observation-" + std::to_string(getpid()));
    std::filesystem::remove_all(root);
    const auto baseline = run(root / "disabled", false, 1, false, false).values;
    for (size_t threads : {size_t{1}, size_t{3}})
        for (bool segmented : {false, true}) {
            const auto name = std::to_string(threads) + (segmented ? "-segmented" : "-whole");
            assert(run(root / name, true, threads, threads > 1, segmented).values == baseline);
        }
    assert(run(root / "stop-resume", true, 3, true, false, true).values == baseline);
    namedSpanAndFreshSession(root / "named-span");
    phaseGap(root / "phase-gap");
    lifecycle(root / "lifecycle");
    failure(root / "budget-failure", true);
    failure(root / "output-failure", false);
    failure(root / "parallel-budget-failure", true, 3);
    failure(root / "parallel-output-failure", false, 3);
    lifecycle(root / "recovery");
    std::cout << "multiclock unified observation passed\n";
}
