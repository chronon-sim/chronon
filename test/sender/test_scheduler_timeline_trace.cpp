// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <zlib.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/schedule/SchedulerTimelineStyle.hpp"
#include "sender/schedule/SchedulerTimelineTrace.hpp"

using namespace chronon::sender;

namespace {

class TimelineWorkUnit : public TickableUnit {
public:
    explicit TimelineWorkUnit(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {
        uint64_t value = localCycle() + 1;
        for (size_t i = 0; i < 100; ++i) value = value * 33 + i;
        checksum_ ^= value;
    }

private:
    uint64_t checksum_ = 0;
};

std::string_view eventString(const chronon::observe::TimelineStreamData& data, size_t stream,
                             uint32_t offset, uint32_t length) {
    const std::string& arena = data.arenas.at(stream);
    REQUIRE(static_cast<size_t>(offset) + length <= arena.size());
    return std::string_view(arena).substr(offset, length);
}

std::string inflateFirstZlibStream(std::string_view bytes) {
    for (size_t offset = 0; offset + 2 <= bytes.size(); ++offset) {
        const auto cmf = static_cast<uint8_t>(bytes[offset]);
        const auto flg = static_cast<uint8_t>(bytes[offset + 1]);
        if ((cmf & 0x0fU) != Z_DEFLATED || ((static_cast<unsigned>(cmf) << 8U) + flg) % 31U != 0) {
            continue;
        }

        z_stream stream{};
        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(bytes.data() + offset));
        stream.avail_in = static_cast<uInt>(bytes.size() - offset);
        if (inflateInit(&stream) != Z_OK) continue;

        std::string output;
        std::array<char, 4096> chunk{};
        int rc = Z_OK;
        while (rc == Z_OK) {
            stream.next_out = reinterpret_cast<Bytef*>(chunk.data());
            stream.avail_out = static_cast<uInt>(chunk.size());
            rc = inflate(&stream, Z_NO_FLUSH);
            output.append(chunk.data(), chunk.size() - stream.avail_out);
        }
        inflateEnd(&stream);
        if (rc == Z_STREAM_END) return output;
    }
    return {};
}

void testDisabledRecorderIsInert() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    trace.configure(config);
    REQUIRE(!trace.enabled());
    REQUIRE(!trace.traceUnits());
    REQUIRE(!trace.traceWaits());
    REQUIRE(!trace.traceEpochs());
    REQUIRE(!trace.traceThreadCpuTime());

    trace.start({{0}}, {});
    const auto now = SchedulerTimelineTrace::Clock::now();
    trace.recordDuration(0, "ignored", "ignored", 0, now, now + std::chrono::nanoseconds(10));
    trace.recordInstant(0, "ignored", "ignored", 0, now);
    REQUIRE(trace.exportData().empty());
}

void testFilteringBudgetAndOwnedStrings() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.file.clear();
    config.max_events = 2;
    config.start_cycle = 10;
    config.end_cycle = 20;
    config.trace_units = false;
    config.trace_waits = true;
    config.trace_epochs = false;
    config.trace_thread_cpu_time = true;
    config.min_duration_ns = 5;
    trace.configure(config);

    REQUIRE(trace.enabled());
    REQUIRE(!trace.traceUnits());
    REQUIRE(trace.traceWaits());
    REQUIRE(!trace.traceEpochs());
    REQUIRE(!trace.traceThreadCpuTime());
    REQUIRE(trace.file().empty());

    trace.start({{0, 1}, {2}}, {});
    const size_t scheduler_stream = trace.schedulerStream();
    REQUIRE(scheduler_stream == 2);
    // start() is intentionally idempotent; a second call must not rebuild the arenas.
    trace.start({{0}}, {});
    REQUIRE(trace.schedulerStream() == scheduler_stream);

    const auto now = SchedulerTimelineTrace::Clock::now();
    trace.recordDuration(99, "bad", "stream", 10, now, now + std::chrono::nanoseconds(10));
    trace.recordDuration(0, "bad", "early", 9, now, now + std::chrono::nanoseconds(10));
    trace.recordDuration(0, "bad", "late", 20, now, now + std::chrono::nanoseconds(10));
    trace.recordDuration(0, "bad", "negative", 10, now + std::chrono::nanoseconds(10), now);
    trace.recordDuration(0, "bad", "short", 10, now, now + std::chrono::nanoseconds(4));

    std::string category = "unit category";
    std::string name = "execute unit";
    std::string detail = "unit=a";
    trace.recordDuration(0, category, name, 10, now, now + std::chrono::nanoseconds(8), detail);
    // Mutating caller-owned strings proves the recorder copied them into its arena.
    category.assign("changed");
    name.assign("changed");
    detail.assign("changed");

    trace.recordInstant(scheduler_stream, "scheduler", "epoch boundary", 19,
                        now + std::chrono::nanoseconds(12), "epoch=3");
    // Valid but over budget: this increments dropped_events without adding a slice.
    trace.recordDuration(1, "unit", "dropped", 12, now, now + std::chrono::nanoseconds(9));

    const auto data = trace.exportData();
    REQUIRE(data.streams.size() == 3);
    REQUIRE(data.arenas.size() == 3);
    REQUIRE(data.stream_names ==
            std::vector<std::string>(
                {"stream 0 (logical worker)", "stream 1 (logical worker)", "scheduler"}));
    REQUIRE(data.dropped_events == 1);
    REQUIRE(data.streams[0].size() == 1);
    REQUIRE(data.streams[1].empty());
    REQUIRE(data.streams[scheduler_stream].size() == 1);

    const auto& duration = data.streams[0][0];
    REQUIRE(eventString(data, 0, duration.cat_off, duration.cat_len) == "unit category");
    REQUIRE(eventString(data, 0, duration.name_off, duration.name_len) == "execute unit");
    REQUIRE(eventString(data, 0, duration.detail_off, duration.detail_len) == "unit=a");
    REQUIRE(duration.cycle == 10);
    REQUIRE(duration.dur_ns == 8);
    REQUIRE(!duration.instant);

    const auto& instant = data.streams[scheduler_stream][0];
    REQUIRE(eventString(data, scheduler_stream, instant.cat_off, instant.cat_len) == "scheduler");
    REQUIRE(eventString(data, scheduler_stream, instant.name_off, instant.name_len) ==
            "epoch boundary");
    REQUIRE(eventString(data, scheduler_stream, instant.detail_off, instant.detail_len) ==
            "epoch=3");
    REQUIRE(instant.cycle == 19);
    REQUIRE(instant.dur_ns == 0);
    REQUIRE(instant.instant);
}

void testReconfigureRebuildsRecorderState() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.max_events = 2;
    trace.configure(config);
    trace.start({{0}}, {});

    const auto now = SchedulerTimelineTrace::Clock::now();
    trace.recordInstant(0, "old", "event", 0, now);

    config.max_events = 1;
    trace.configure(config);
    trace.start({{0}, {1}}, {});
    REQUIRE(trace.schedulerStream() == 2);
    const auto restarted = SchedulerTimelineTrace::Clock::now();
    trace.recordInstant(1, "new", "event", 0, restarted);
    trace.recordInstant(trace.schedulerStream(), "scheduler", "over-budget", 0, restarted);

    const auto data = trace.exportData();
    REQUIRE(data.streams.size() == 3);
    REQUIRE(data.streams[0].empty());
    REQUIRE(data.streams[1].size() == 1);
    REQUIRE(data.streams[trace.schedulerStream()].empty());
    REQUIRE(data.dropped_events == 1);
}

void testChunkedBudgetAcrossStreams() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.max_events = 2'048;
    trace.configure(config);
    trace.start({{0}, {1}}, {});

    const auto now = SchedulerTimelineTrace::Clock::now();
    const size_t scheduler_stream = trace.schedulerStream();
    for (uint64_t cycle = 0; cycle < 1'024; ++cycle) {
        trace.recordDuration(0, "unit", "worker", cycle, now, now + std::chrono::nanoseconds(1));
        trace.recordInstant(scheduler_stream, "scheduler", "marker", cycle, now);
    }
    trace.recordDuration(1, "unit", "over-budget", 0, now, now + std::chrono::nanoseconds(1));

    const auto data = trace.exportData();
    REQUIRE(data.streams[0].size() == 1'024);
    REQUIRE(data.streams[1].empty());
    REQUIRE(data.streams[scheduler_stream].size() == 1'024);
    REQUIRE(data.dropped_events == 1);
}

void testChunkedBudgetReclaimsSparseStreams() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.max_events = 1'024;
    trace.configure(config);
    trace.start({{0}}, {});

    const auto now = SchedulerTimelineTrace::Clock::now();
    const size_t scheduler_stream = trace.schedulerStream();
    trace.recordDuration(0, "unit", "sparse-worker", 0, now, now + std::chrono::nanoseconds(1));
    for (uint64_t cycle = 0; cycle < 1'023; ++cycle) {
        trace.recordInstant(scheduler_stream, "scheduler", "marker", cycle, now);
    }
    trace.recordInstant(scheduler_stream, "scheduler", "over-budget", 1'023, now);

    const auto data = trace.exportData();
    REQUIRE(data.streams[0].size() == 1);
    REQUIRE(data.streams[scheduler_stream].size() == 1'023);
    REQUIRE(data.dropped_events == 1);
}

void testConcurrentChunkReclamationPreservesExactCap() {
    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.max_events = 4'097;
    trace.configure(config);
    trace.start({{0}, {1}, {2}, {3}}, {});

    constexpr size_t kWorkers = 4;
    constexpr uint64_t kEventsPerWorker = 2'000;
    const auto now = SchedulerTimelineTrace::Clock::now();
    std::array<std::thread, kWorkers> workers;
    for (size_t stream = 0; stream < kWorkers; ++stream) {
        workers[stream] = std::thread([&, stream] {
            for (uint64_t cycle = 0; cycle < kEventsPerWorker; ++cycle) {
                trace.recordInstant(stream, "unit", "worker", cycle, now);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    const auto data = trace.exportData();
    size_t recorded = 0;
    for (const auto& stream : data.streams) {
        recorded += stream.size();
    }
    REQUIRE(recorded == config.max_events);
    REQUIRE(data.dropped_events == kWorkers * kEventsPerWorker - config.max_events);
}

void testStandaloneWriteAndStyleMapping() {
    const auto cluster = schedulerStallStyle("stall: cluster-dep");
    const auto floor = schedulerStallStyle("stall: lookahead-floor");
    const auto no_ready = schedulerStallStyle("stall: no-ready-cluster");
    const auto fallback = schedulerStallStyle("stall: custom");
    REQUIRE(!cluster.category.empty() && !cluster.name.empty());
    REQUIRE(cluster.category != floor.category);
    REQUIRE(floor.category != no_ready.category);
    REQUIRE(fallback.category == cluster.category);
    REQUIRE(fallback.name != cluster.name);
    REQUIRE(schedulerStallStyle("stall: cluster-dep").category == cluster.category);

    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const std::filesystem::path output_dir =
        std::filesystem::temp_directory_path() / ("chronon-scheduler-trace-test-" + unique);
    const std::filesystem::path expected = output_dir / "nested" / "scheduler.pftrace";

    SchedulerTimelineTrace trace;
    SchedulerTimelineTraceConfig config;
    config.enabled = true;
    config.file = "nested/scheduler.pftrace";
    config.max_events = 4;
    trace.configure(config);
    trace.start({}, {});
    trace.recordInstant(trace.schedulerStream(), "scheduler", "start", 0,
                        SchedulerTimelineTrace::Clock::now());
    trace.write(output_dir);
    REQUIRE(std::filesystem::is_regular_file(expected));
    const auto first_size = std::filesystem::file_size(expected);
    REQUIRE(first_size > 0);
    trace.write(output_dir);
    REQUIRE(std::filesystem::file_size(expected) == first_size);

    std::error_code ec;
    std::filesystem::remove_all(output_dir, ec);
    REQUIRE(!ec);
}

void testSimulationTimelineIncludesThreadCpuDiagnostics() {
    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const std::filesystem::path output = std::filesystem::temp_directory_path() /
                                         ("chronon-scheduler-cpu-trace-" + unique + ".pftrace");

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_dynamic_rebalance = false;
    config.initial_partition_sync_cost_ns = 0.0;
    config.timeline_trace.enabled = true;
    config.timeline_trace.file = output.string();
    config.timeline_trace.trace_units = true;
    config.timeline_trace.trace_thread_cpu_time = true;
    config.timeline_trace.max_events = 1'000;

    TickSimulation simulation(config);
    for (size_t i = 0; i < 6; ++i) {
        simulation.createUnit<TimelineWorkUnit>("timeline-unit-" + std::to_string(i));
    }
    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 0.0;
    simulation.setPrecomputedUnitCosts(std::vector<double>(6, 100.0), metrics);
    simulation.run(4);
    REQUIRE(simulation.timelineTraceEnabled());
    simulation.writeTimelineTrace();

    REQUIRE(std::filesystem::is_regular_file(output));
    std::ifstream input(output, std::ios::binary);
    REQUIRE(input.good());
    const std::string bytes{std::istreambuf_iterator<char>(input),
                            std::istreambuf_iterator<char>()};
    const std::string inflated = inflateFirstZlibStream(bytes);
    REQUIRE(!inflated.empty());
    REQUIRE(inflated.find("timeline-unit-") != std::string::npos);
    REQUIRE(inflated.find("wall_ns=") != std::string::npos);
    REQUIRE(inflated.find("thread_cpu_ns=") != std::string::npos);
    REQUIRE(inflated.find("wall_cpu_gap_ns=") != std::string::npos);
    REQUIRE(inflated.find("cpu_begin=") != std::string::npos);
    REQUIRE(inflated.find("cpu_end=") != std::string::npos);

    std::error_code ec;
    std::filesystem::remove(output, ec);
    REQUIRE(!ec);
}

}  // namespace

int main() {
    testDisabledRecorderIsInert();
    testFilteringBudgetAndOwnedStrings();
    testReconfigureRebuildsRecorderState();
    testChunkedBudgetAcrossStreams();
    testChunkedBudgetReclaimsSparseStreams();
    testConcurrentChunkReclamationPreservesExactCap();
    testStandaloneWriteAndStyleMapping();
    testSimulationTimelineIncludesThreadCpuDiagnostics();
    return chronon::test::failureCount() == 0 ? 0 : 1;
}
