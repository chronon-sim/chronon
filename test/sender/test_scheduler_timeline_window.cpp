// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/port/InPort.hpp"
#include "sender/port/OutPort.hpp"

using namespace chronon::sender;

namespace {
std::atomic<bool> counting{false};
std::atomic<size_t> clock_reads{0}, cpu_reads{0}, one_byte_allocations{0}, total_allocations{0};

struct Counts {
    size_t clock, cpu, allocations, total_allocations;
};

template <typename Fn>
Counts measure(Fn&& fn) {
    clock_reads = 0;
    cpu_reads = 0;
    one_byte_allocations = 0;
    total_allocations = 0;
    counting = true;
    fn();
    counting = false;
    return {clock_reads.load(), cpu_reads.load(), one_byte_allocations.load(),
            total_allocations.load()};
}

void checkOutside(Counts counts, bool dynamic) {
#if defined(CHRONON_TEST_WRAP_CLOCK)
    CHECK(counts.cpu == 0);
    CHECK(counts.allocations == 0);
    // Dynamic scheduling must retain its own sparse wall-time samples.
    if (!dynamic) CHECK(counts.clock == 0);
#else
    (void)counts;
    (void)dynamic;
#endif
}
}  // namespace

#if defined(CHRONON_TEST_WRAP_CLOCK)
// Linker wrapping preserves the sanitizer runtime's new/delete implementations.
extern "C" void* CHRONON_TEST_REAL_NEW(size_t size);
extern "C" void* CHRONON_TEST_WRAP_NEW(size_t size) {
    if (counting.load(std::memory_order_relaxed)) {
        total_allocations.fetch_add(1, std::memory_order_relaxed);
        if (size == 1) one_byte_allocations.fetch_add(1, std::memory_order_relaxed);
    }
    return CHRONON_TEST_REAL_NEW(size);
}
#define CHRONON_TEST_STRINGIFY_IMPL(value) #value
#define CHRONON_TEST_STRINGIFY(value) CHRONON_TEST_STRINGIFY_IMPL(value)
// Keep C++ linkage for the C++ return type; assembler labels name the symbols
// expected by --wrap without Clang's -Wreturn-type-c-linkage diagnostic.
std::chrono::steady_clock::time_point realClockNow() asm(
    CHRONON_TEST_STRINGIFY(CHRONON_TEST_REAL_CLOCK));
std::chrono::steady_clock::time_point wrappedClockNow() asm(
    CHRONON_TEST_STRINGIFY(CHRONON_TEST_WRAP_CLOCK));
#undef CHRONON_TEST_STRINGIFY
#undef CHRONON_TEST_STRINGIFY_IMPL

std::chrono::steady_clock::time_point wrappedClockNow() {
    if (counting.load(std::memory_order_relaxed)) {
        clock_reads.fetch_add(1, std::memory_order_relaxed);
    }
    return realClockNow();
}
extern "C" int __real_clock_gettime(clockid_t, timespec*) noexcept;
extern "C" int __wrap_clock_gettime(clockid_t id, timespec* value) noexcept {
    if (id == CLOCK_THREAD_CPUTIME_ID && counting.load(std::memory_order_relaxed)) {
        cpu_reads.fetch_add(1, std::memory_order_relaxed);
    }
    return __real_clock_gettime(id, value);
}
#endif

namespace chronon::sender {
// Reuse the existing test-only access point for dynamic sampling and trace data.
struct DynamicMigrationTestAccess {
    static auto exportTrace(TickSimulation& sim) { return sim.timeline_trace_.exportData(); }
    static uint64_t samples(const TickSimulation& sim) {
        uint64_t count = 0;
        for (size_t c = 0; c < sim.dynamic_runtime_cluster_count_; ++c) {
            count += sim.cluster_sample_count_[c].load(std::memory_order_relaxed);
        }
        for (size_t u = 0; u < sim.dynamic_runtime_unit_count_; ++u) {
            count += sim.dynamic_unit_active_sample_count_[u].load(std::memory_order_relaxed);
            count += sim.dynamic_unit_inactive_sample_count_[u].load(std::memory_order_relaxed);
        }
        return count;
    }
    static void marker(TickSimulation& sim, uint64_t cycle, const std::string& detail) {
        sim.recordDynamicSchedulerMarker_("a scheduler marker with a non-SSO name", cycle, detail);
    }
    static void flushMarkers(TickSimulation& sim) { sim.flushDynamicSchedulerMarkers_(); }
};
}  // namespace chronon::sender

namespace {
class WindowUnit : public TickableUnit {
public:
    WindowUnit(std::string name, bool idle, bool slow)
        : TickableUnit(std::move(name)), idle_(idle), slow_(slow) {}
    void tick() override {
        ++ticks;
        uint64_t value = localCycle();
        for (size_t i = 0; i < (slow_ ? 5000 : 10); ++i) value = value * 33 + i;
        checksum ^= value;
        if (idle_) sleepForever();
    }
    uint64_t ticks = 0, checksum = 0;

private:
    bool idle_, slow_;
};

void runWindow(const std::string& mode) {
    const bool dynamic = mode.find("dynamic") != std::string::npos;
    const bool idle = mode.starts_with("idle");
    const bool crossing = mode.ends_with("crossing");
    const bool waits = mode == "waits";
    const bool epochs = mode == "epochs";
    const bool empty = mode == "empty";
    const bool zero_start = mode.ends_with("zero_start");
    TickSimulationConfig cfg;
    cfg.num_threads = 2;
    cfg.max_lookahead_cycles = waits ? 1 : 100;
    cfg.enable_parallel = true;
    cfg.enable_dynamic_rebalance = dynamic;
    cfg.rebalance_check_interval_cycles = 8192;
    cfg.initial_partition_sync_cost_ns = 0;
    cfg.timeline_trace.enabled = true;
    cfg.timeline_trace.trace_units = !waits && !epochs;
    cfg.timeline_trace.trace_waits = waits;
    cfg.timeline_trace.trace_epochs = epochs;
    cfg.timeline_trace.trace_thread_cpu_time = !idle && !waits && !epochs;
    // Deliberately straddle the dynamic scheduler's short bursts.
    cfg.timeline_trace.start_cycle = zero_start ? 0 : 101;
    cfg.timeline_trace.end_cycle = empty ? 101 : 103;
    cfg.timeline_trace.max_events = 1000;
    TickSimulation sim(cfg);
    std::vector<WindowUnit*> units;
    for (size_t i = 0; i < 6; ++i) {
        units.push_back(
            sim.createUnit<WindowUnit>("window-unit-" + std::to_string(i), idle, waits && i == 0));
    }
    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 0;
    sim.setPrecomputedUnitCosts(std::vector<double>(6, 100.0), metrics);
    CHECK(sim.run(64) == 64);
    CHECK(sim.useParallelExecution());
    if (empty) {
        checkOutside(measure([&] { CHECK(sim.run(136) == 136); }), dynamic);
    } else if (crossing) {
        const auto counts = measure([&] { CHECK(sim.run(136) == 136); });
#if defined(CHRONON_TEST_WRAP_CLOCK)
        CHECK(counts.cpu == 24);
        CHECK(counts.allocations == 0);
        if (!dynamic) CHECK(counts.clock == 24);
#else
        (void)counts;
#endif
    } else {
        const auto before = measure([&] { CHECK(sim.run(37) == 37); });
        if (!zero_start) checkOutside(before, dynamic);
        const auto inside = measure([&] { CHECK(sim.run(2) == 2); });
#if defined(CHRONON_TEST_WRAP_CLOCK)
        if (!idle && !waits && !epochs) {
            CHECK(inside.cpu == 24);
            // Active flags share the timestamp scratch; no per-cluster
            // one-byte allocation is needed, even on the first capture.
            CHECK(inside.allocations == 0);
            if (!dynamic) CHECK(inside.clock == 24);
        }
        if (epochs) CHECK(inside.clock == 2);
#else
        (void)inside;
#endif
        checkOutside(measure([&] { CHECK(sim.run(97) == 97); }), dynamic);
    }
    for (const auto* unit : units) {
        CHECK(unit->localCycle() == 200);
        CHECK(unit->ticks == (idle ? 1 : 200));
    }
    if (dynamic) {
        // Samples must continue after the trace has closed, for active and idle units.
        const auto before = DynamicMigrationTestAccess::samples(sim);
        const auto counts = measure([&] { CHECK(sim.run(1024) == 1024); });
        checkOutside(counts, true);
        CHECK(DynamicMigrationTestAccess::samples(sim) > before);
    }
    const auto data = DynamicMigrationTestAccess::exportTrace(sim);
    size_t events = 0;
    for (size_t stream = 0; stream < data.streams.size(); ++stream) {
        for (const auto& event : data.streams[stream]) {
            ++events;
            CHECK(event.cycle >= cfg.timeline_trace.start_cycle && event.cycle < 103);
            const auto category =
                std::string_view(data.arenas[stream]).substr(event.cat_off, event.cat_len);
            if (idle) CHECK(category == "unit idle");
            if (!idle && !waits && !epochs) {
                CHECK(category == "unit");
                const auto detail = std::string_view(data.arenas[stream])
                                        .substr(event.detail_off, event.detail_len);
#if defined(__linux__)
                CHECK(detail.find("thread_cpu_ns=") != std::string_view::npos);
#endif
            }
        }
    }
    if (empty)
        CHECK(events == 0);
    else if (epochs)
        CHECK(events == 1);
    else if (idle)
        CHECK(events >= 6);
    else if (waits)
        CHECK(events > 0);
    else if (!waits)
        CHECK(events == (zero_start ? 618 : 12));
    CHECK(data.dropped_events == 0);
}

class MixedUnit : public WindowUnit {
public:
    explicit MixedUnit(std::string name) : WindowUnit(std::move(name), false, false) {}
    InPort<int> in{this, "in"};
    OutPort<int> out{this, "out"};
};

void mixedActivity(bool dynamic) {
    TickSimulationConfig cfg;
    cfg.num_threads = 2;
    cfg.enable_parallel = true;
    cfg.enable_dynamic_rebalance = dynamic;
    cfg.rebalance_check_interval_cycles = 8192;
    cfg.initial_partition_sync_cost_ns = 0;
    cfg.timeline_trace.enabled = true;
    cfg.timeline_trace.trace_waits = false;
    cfg.timeline_trace.trace_epochs = false;
    cfg.timeline_trace.max_events = 1000;
    TickSimulation sim(cfg);
    std::vector<MixedUnit*> units;
    for (size_t i = 0; i < 6; ++i) {
        units.push_back(sim.createUnit<MixedUnit>("mixed" + std::to_string(i)));
    }
    // Unequal tight clusters reuse scratch; active and idle entries alternate
    // within the same cluster, so stale active bits would mislabel the trace.
    sim.connect(units[1]->out, units[2]->in, 0);
    sim.connect(units[3]->out, units[4]->in, 0);
    sim.connect(units[4]->out, units[5]->in, 0);
    units[1]->setTickInterval(2);
    units[4]->setTickInterval(2);
    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 0;
    sim.setPrecomputedUnitCosts(std::vector<double>(6, 100.0), metrics);
    CHECK(sim.run(1) == 1);
    CHECK(sim.useParallelExecution());
    const auto counts = measure([&] { CHECK(sim.run(49) == 49); });
    CHECK(counts.allocations == 0);
    const auto data = DynamicMigrationTestAccess::exportTrace(sim);
    size_t events = 0;
    for (size_t stream = 0; stream < data.streams.size(); ++stream) {
        const auto arena = std::string_view(data.arenas[stream]);
        for (const auto& event : data.streams[stream]) {
            const auto name = arena.substr(event.name_off, event.name_len);
            const auto category = arena.substr(event.cat_off, event.cat_len);
            const bool inactive = (name == "mixed1" || name == "mixed4") && event.cycle % 2;
            CHECK(category == (inactive ? "unit idle" : "unit"));
            ++events;
        }
    }
    CHECK(events == 300);
    CHECK(data.dropped_events == 0);
    for (size_t i = 0; i < units.size(); ++i) {
        CHECK(units[i]->ticks == (i == 1 || i == 4 ? 25 : 50));
    }
}

void markers() {
    TickSimulationConfig cfg;
    cfg.num_threads = 1;
    cfg.enable_parallel = false;
    cfg.timeline_trace.enabled = true;
    cfg.timeline_trace.start_cycle = 101;
    cfg.timeline_trace.end_cycle = 103;
    TickSimulation sim(cfg);
    sim.createUnit<WindowUnit>("marker-owner", false, false);
    sim.initialize();
    const std::string detail(128, 'x');
    const auto outside = measure([&] {
        DynamicMigrationTestAccess::marker(sim, 100, detail);
        DynamicMigrationTestAccess::marker(sim, 103, detail);
    });
    checkOutside(outside, false);
    CHECK(outside.total_allocations == 0);
    {
        std::string owned = detail;
        DynamicMigrationTestAccess::marker(sim, 101, owned);
        DynamicMigrationTestAccess::marker(sim, 102, owned);
        owned.assign(128, 'y');
    }
    DynamicMigrationTestAccess::flushMarkers(sim);
    const auto data = DynamicMigrationTestAccess::exportTrace(sim);
    size_t events = 0;
    for (size_t stream = 0; stream < data.streams.size(); ++stream) {
        for (const auto& event : data.streams[stream]) {
            CHECK(event.cycle == 101 + events++);
            CHECK(
                std::string_view(data.arenas[stream]).substr(event.detail_off, event.detail_len) ==
                detail);
        }
    }
    CHECK(events == 2);
}
}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    if (mode == "markers")
        markers();
    else if (mode.starts_with("mixed"))
        mixedActivity(mode == "mixed_dynamic");
    else
        runWindow(mode);
    std::cout << "Scheduler timeline window " << mode << ": PASSED\n";
}
