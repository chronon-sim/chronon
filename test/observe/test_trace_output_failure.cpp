// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <sys/resource.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include "PftraceTestDecoder.hpp"
#include "observe/ObservationManager.hpp"
#include "sender/app/SimulationApp.hpp"

using namespace chronon::observe;

namespace {

// A process-local EFBIG injection; no disk filling, root access, or fork after
// sanitizer initialization. Keep the hard limit so healthy-output tests can
// restore the soft limit. Each CTest invocation has its own process and timeout.
class FileSizeLimit {
public:
    explicit FileSizeLimit(rlim_t bytes) {
        CHECK(getrlimit(RLIMIT_FSIZE, &saved_) == 0);
        previous_ = std::signal(SIGXFSZ, SIG_IGN);
        CHECK(previous_ != SIG_ERR);
        auto limited = saved_;
        limited.rlim_cur = bytes;
        CHECK(setrlimit(RLIMIT_FSIZE, &limited) == 0);
    }
    ~FileSizeLimit() {
        CHECK(setrlimit(RLIMIT_FSIZE, &saved_) == 0);
        CHECK(std::signal(SIGXFSZ, previous_) != SIG_ERR);
    }

private:
    rlimit saved_{};
    void (*previous_)(int) = SIG_DFL;
};

template <typename F>
std::string expectFailure(F&& operation, const std::filesystem::path& path) {
    try {
        operation();
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        CHECK(message.find("Perfetto trace") != std::string::npos);
        CHECK(message.find(path.string()) != std::string::npos);
        return message;
    }
    CHECK(false && "trace output failure was swallowed");
    return {};
}

void testWriter(const std::filesystem::path& root, bool compress) {
    PerfettoTraceWriter writer;
    PerfettoTraceWriter::Options options;
    options.compress = compress;
    for (bool close_only : {false, true}) {
        for (bool native : {false, true}) {
            const auto path = root / "failed.pftrace";
            CHECK(writer.open(path, options));
            uint32_t stream = 0;
            if (native)
                stream =
                    writer.addClockStream(chronon::ClockDomain::fromHz(1, "clock", 1'000'000'000));
            const auto track = writer.addTrack("track");
            if (native) {
                writer.clockInstant(stream, track, "test", "event", 1, 0, {});
                if (!close_only) writer.advanceClockWatermark(2);
            } else {
                writer.instant(track, "test", "event", 1);
            }
            {
                FileSizeLimit limit(1);
                if (close_only) {
                    expectFailure([&] { writer.close(); }, path);
                } else {
                    expectFailure([&] { writer.flush(); }, path);
                    expectFailure([&] { writer.flush(); }, path);
                    expectFailure([&] { writer.close(); }, path);
                }
                CHECK(!writer.isOpen());
                CHECK(writer.bytesWritten() == 0);
                CHECK(std::filesystem::file_size(path) <= 1);
            }
            writer.close();  // Cleanup remains idempotent after failure.
        }
    }

    // Keep accounting for an already successful prefix, but exclude a failed
    // large batch (including a direct write that exceeds the stream buffer).
    const auto path = root / "prefix.pftrace";
    CHECK(writer.open(path, options));
    const auto track = writer.addTrack("track");
    writer.instant(track, "test", "prefix", 1);
    writer.flush();
    const auto prefix_bytes = writer.bytesWritten();
    CHECK(prefix_bytes == std::filesystem::file_size(path) && prefix_bytes > 0);
    std::string payload(256 * 1024, '\0');
    uint32_t random = 1;
    for (auto& ch : payload) {
        random = random * 1664525 + 1013904223;
        ch = static_cast<char>('!' + ((random >> 24) % 90));
    }
    const PerfettoTraceWriter::Annotation annotation{
        "data", PerfettoTraceWriter::Annotation::Kind::String, 0, payload};
    writer.instant(track, "test", "large", 2, 0, {&annotation, 1});
    {
        FileSizeLimit limit(prefix_bytes + 1);
        expectFailure([&] { writer.flush(); }, path);
        CHECK(writer.bytesWritten() == prefix_bytes);
        expectFailure([&] { writer.close(); }, path);
    }

    CHECK(writer.open(root / "healthy.pftrace", options));
    writer.instant(writer.addTrack("reopened"), "test", "healthy", 1);
    writer.close();
    CHECK(writer.bytesWritten() == std::filesystem::file_size(root / "healthy.pftrace"));
    const auto decoded = pftrace_test::decodeFile(root / "healthy.pftrace");
    CHECK(decoded.events.size() == 1 && decoded.events[0].name == "healthy");
}

void testFullDevice() {
    // Linux's ENOSPC device exercises a different OS error without filling disk.
    const std::filesystem::path path = "/dev/full";
    CHECK(std::filesystem::exists(path));
    for (bool compress : {false, true}) {
        PerfettoTraceWriter writer;
        PerfettoTraceWriter::Options options;
        options.compress = compress;
        CHECK(writer.open(path, options));
        writer.instant(writer.addTrack("events"), "test", "disk_full", 1);
        expectFailure([&] { writer.flush(); }, path);
        CHECK(writer.bytesWritten() == 0);
        expectFailure([&] { writer.close(); }, path);
        CHECK(!writer.isOpen());
    }
}

struct ClockProducer : chronon::sender::TickableUnit {
    explicit ClockProducer(std::string name) : TickableUnit(std::move(name)) {}
    void tick() override {
        for (size_t i = 0; i < 8; ++i) clockEvent(ClockEventKind::User, 0, localCycle());
    }
};

void testParallelClock(const std::filesystem::path& root) {
    chronon::sender::TickSimulationConfig config;
    config.num_threads = 2;
    config.max_lookahead_cycles = 16;
    chronon::sender::TickSimulation sim(config);
    sim.addClockDomain(chronon::ClockDomain::fromHz(1, "clock", 1'000'000'000));
    for (size_t i = 0; i < 4; ++i)
        sim.createUnitInDomain<ClockProducer>(1, "unit-" + std::to_string(i));
    ClockTraceRecorder::Config trace;
    trace.output_dir = root / "parallel";
    trace.text = false;
    trace.stream_capacity = 2;
    trace.drain_batch = 1;
    trace.perfetto_options.clock_buffer_records = 64;
    sim.configureClockTrace(trace);
    sim.initialize();
    CHECK(sim.useParallelExecution());
    {
        FileSizeLimit limit(1);
        bool failed = false;
        try {
            sim.runClockEvents(100'000);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        CHECK(failed);  // Includes producers waiting on two-record ingress rings.
        expectFailure([&] { sim.closeClockTrace(); }, trace.output_dir / "timeline.pftrace");
    }
    bool rejected = false;
    try {
        sim.runClockEvents(1);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    CHECK(rejected);
}

void testBackend(const std::filesystem::path& root, bool reorder, bool pressure) {
    auto& threads = ThreadContextManager::instance();
    threads.setQueueCapacity(4096);
    threads.setBackpressurePolicy(BackpressurePolicy::SpinWait);
    ObservationQueue queue(4096);
    ObservationBackend::Config config;
    config.output_dir = root.string();
    config.enable_counter_csv = false;
    config.enable_reordering = reorder;
    config.reorder_watermark_cycles = 0;
    config.reorder_max_events = 64;
    config.timeline_compress = reorder;
    ObservationBackend backend(queue, config);
    ObservationContext context(&queue, [] { return 0ULL; }, 0, "producer", 1);
    context.enableCategory(category::TRACE);
    const auto track = TimelineTrackRegistry::instance().registerTrack(
        {"events", 1, 1, TimelineTrackInfo::Layout::Normal});
    const auto emit = [&](uint64_t cycle, uint32_t name) {
        context.setCurrentCycleValue(cycle);
        return context.timelineEvent(category::TRACE, TimelineEventKind::Instant, track, 0, name, 0,
                                     nullptr, 0);
    };
    backend.start();
    const auto path = backend.outputDir() / "timeline.pftrace";
    {
        FileSizeLimit limit(1);
        const auto name = EventName<"before_failure">::ref().id;
        for (uint64_t i = 0; i < (pressure ? 100'000ULL : 1ULL); ++i) (void)emit(i, name);
        threads.flushAll();
        if (pressure) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            // The drain thread may enqueue the entire producer batch before
            // the I/O thread reaches its first write. Wait for error publication
            // and callback removal instead of assuming relative thread speeds.
            bool failed = false;
            while (!failed || threads.wakeBackend()) {
                CHECK(std::chrono::steady_clock::now() < deadline);
                try {
                    backend.rethrowIfFailed();
                } catch (const std::runtime_error&) {
                    failed = true;
                }
                std::this_thread::yield();
            }
            for (uint64_t i = 100'000; i < 104'096; ++i) (void)emit(i, name);
            // The producer must finish even after the consumer fails with a
            // full queue. The CTest timeout catches an unbounded SpinWait.
            CHECK(context.observationStats().get<ObservationChannel::Trace>().dropped > 0);
            CHECK(!threads.wakeBackend());
        }
        backend.stop();
        CHECK(!backend.isRunning() && !backend.timelineEnabled());
        const auto first = expectFailure([&] { backend.rethrowIfFailed(); }, path);
        backend.stop();
        CHECK(first == expectFailure([&] { backend.rethrowIfFailed(); }, path));
    }
    // Restart the same backend after recovery. No unread records from the
    // failed run may survive and appear under this run's source/track metadata.
    backend.start();
    backend.rethrowIfFailed();
    CHECK(emit(1, EventName<"after_restart">::ref().id));
    threads.flushAll();
    backend.stop();
    backend.rethrowIfFailed();
    const auto decoded = pftrace_test::decodeFile(backend.outputDir() / "timeline.pftrace");
    CHECK(decoded.events.size() == 1 && decoded.events[0].name == "after_restart");
}

void testManager(const std::filesystem::path& root, bool shutdown) {
    auto& manager = ObservationManager::instance();
    ObservationYAMLConfig config;
    config.enabled = true;
    config.output_dir = root.string();
    config.counters.csv_output = false;
    manager.initialize(config);
    manager.startBackend();
    const auto path = manager.backend()->outputDir() / "timeline.pftrace";
    {
        FileSizeLimit limit(1);
        expectFailure([&] { shutdown ? manager.shutdown() : manager.stopBackend(); }, path);
        CHECK(!manager.isBackendRunning());
        if (!shutdown) expectFailure([&] { manager.shutdown(); }, path);
    }
    manager.initialize(config);
    manager.startBackend();
    manager.stopBackend();
    manager.shutdown();
}

struct AppUnit : chronon::sender::TickableUnit {
    bool crash;
    explicit AppUnit(bool should_crash) : TickableUnit("unit"), crash(should_crash) {}
    void tick() override {
        if (crash) throw std::runtime_error("original simulation error");
    }
};

void testApp(const std::filesystem::path& root, bool crash) {
    const auto config_path = root / "config.yaml";
    {
        std::ofstream config(config_path);
        config << "simulation:\n  name: output_failure\n  num_workers: 1\n"
                  "  enable_parallel: false\n  run_cycles: 2\n"
                  "  observation:\n    enabled: true\n    output_dir: "
               << root.string()
               << "\n    counters:\n      enabled: false\n      csv_output: false\n"
                  "    timeline:\n      enabled: true\nunits: []\n";
    }
    std::ostringstream output, errors;
    auto* old_out = std::cout.rdbuf(output.rdbuf());
    auto* old_err = std::cerr.rdbuf(errors.rdbuf());
    bool built = false, completed = false;
    {
        FileSizeLimit limit(1);
        chronon::SimulationApp app("trace failure test");
        app.setDefaultConfig(config_path.string())
            .onPostBuild([&](auto& result) {
                built = true;
                result.simulation->template createUnit<AppUnit>(crash);
            })
            .onPostRun([&](auto&) { completed = true; });
        char arg[] = "trace_failure_test";
        char* argv[] = {arg};
        CHECK(app.run(1, argv) == 1);
    }
    std::cout.rdbuf(old_out);
    std::cerr.rdbuf(old_err);
    CHECK(built && !completed);
    CHECK(output.str().find("SIMULATION COMPLETE") == std::string::npos);
    CHECK(errors.str().find("Perfetto trace") != std::string::npos);
    CHECK(errors.str().find("timeline.pftrace") != std::string::npos);
    if (crash) CHECK(errors.str().find("original simulation error") != std::string::npos);
    auto& manager = ObservationManager::instance();
    CHECK(!manager.isBackendRunning());
    expectFailure([&] { manager.shutdown(); }, root);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-trace-failure-" + mode + "-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);
    if (mode == "raw" || mode == "compressed") {
        testWriter(root, mode == "compressed");
    } else if (mode == "full_device") {
        testFullDevice();
    } else if (mode == "parallel_clock") {
        testParallelClock(root);
    } else if (mode == "immediate" || mode == "async" || mode == "final_flush") {
        testBackend(root, mode != "immediate", mode != "final_flush");
    } else if (mode == "manager_stop" || mode == "manager_shutdown") {
        testManager(root, mode == "manager_shutdown");
    } else {
        CHECK(mode == "app" || mode == "app_crash");
        testApp(root, mode == "app_crash");
    }
    std::filesystem::remove_all(root);
    std::cout << "Trace output failure " << mode << ": PASSED\n";
}
