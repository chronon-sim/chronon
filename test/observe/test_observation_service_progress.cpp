// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "chronon/Chronon.hpp"
#include "observe/ObservationBackend.hpp"

using namespace chronon;
using namespace chronon::observe;

#define CHECK(x)                                \
    do {                                        \
        if (!(x)) throw std::runtime_error(#x); \
    } while (false)

template <class Predicate, class Pump>
void awaitProgress(Predicate predicate, Pump pump) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
        CHECK(std::chrono::steady_clock::now() < deadline);
        pump();
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

namespace chronon::observe {
struct ClockTraceStreamTestAccess {
    static HostServiceRegistration& service(ClockTraceStream& stream) { return *stream.service_; }
};
}  // namespace chronon::observe

struct Producer : TickableUnit {
    ObservationContext& context;
    FormatId format;
    bool enabled = false;
    Producer(std::string name, ObservationContext& context, FormatId format)
        : TickableUnit(std::move(name)), context(context), format(format) {}
    void tick() override {
        if (!enabled) return;
        for (uint64_t n = 0; n < 8; ++n) context.log<LogLevel::Info>(format, n);
        // Publish the sparse batch without invoking producer assistance.
        ThreadContextManager::instance().getContext()->queue().forceCommitWrite();
    }
};

struct Witness : HostService {
    size_t poll(size_t) noexcept override {
        ++calls;
        return 0;
    }
    std::atomic<size_t> calls{0};
};

void shortRuns(const std::filesystem::path& root, bool dynamic, bool clocks, size_t workers,
               uint64_t step_size) {
    auto& threads = ThreadContextManager::instance();
    threads.setQueueCapacity(4096);
    threads.setBackpressurePolicy(BackpressurePolicy::Drop);
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.initial_partition_sync_cost_ns = 0;
    TickSimulation sim(config);
    // More registrations than workers also catches restarting every cursor at zero.
    std::array<Witness, 5> witnesses;
    std::vector<std::shared_ptr<HostServiceRegistration>> registrations;
    for (auto& witness : witnesses) registrations.push_back(sim.hostServices().add(witness));
    ObservationQueue queue(4096);
    ObservationBackend::Config output;
    output.output_dir = root.string();
    output.enable_counter_csv = output.timeline_enabled = output.enable_reordering = false;
    ObservationBackend backend(queue, output);
    backend.attachScheduler(sim.hostServices());
    std::atomic<size_t> received{0};
    backend.setLogHandler([&](auto, const std::byte*, size_t) { ++received; });
    const auto format = FormatRegistry::instance().registerFormat(
        "short run {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    if (clocks) sim.addClockDomain(ClockDomain::fromHz(1, "clock", 1'000'000'000));
    std::vector<std::unique_ptr<ObservationContext>> contexts;
    std::vector<Producer*> producers;
    for (size_t i = 0; i < config.num_threads; ++i) {
        auto context = std::make_unique<ObservationContext>(&queue, [] { return uint64_t{0}; });
        context->enableCategory(category::LOG_INFO);
        const auto name = "producer-" + std::to_string(i);
        producers.push_back(clocks ? sim.createUnitInDomain<Producer>(1, name, *context, format)
                                   : sim.createUnit<Producer>(name, *context, format));
        contexts.push_back(std::move(context));
    }
    sim.setPrecomputedUnitCosts(std::vector<double>(config.num_threads, 1000), {});
    sim.initialize();
    CHECK(sim.useParallelExecution() == (workers > 1));
    backend.start();
    const auto step = [&] {
        CHECK((clocks ? sim.runClockEvents(step_size) : sim.run(step_size)) == step_size);
    };
    for (size_t batch = 1; batch <= 64; ++batch) {
        for (auto* producer : producers) producer->enabled = true;
        step();
        for (auto* producer : producers) producer->enabled = false;
        // Every invocation stays short. Wait for output before producing again,
        // so a slow I/O lane cannot cause legitimate Drop-policy overflow.
        awaitProgress([&] { return received == batch * 8 * producers.size() * step_size; }, step);
    }
    CHECK(backend.serviceStats().records == received);
    for (auto& context : contexts)
        CHECK(context->observationStats().get<ObservationChannel::Info>().dropped == 0);
    for (auto& witness : witnesses) CHECK(witness.calls > 0);
    backend.stop();
    backend.rethrowIfFailed();
    CHECK(received == 64 * 8 * producers.size() * step_size);
    for (auto& registration : registrations) registration->detach();
}

void startedBackend(const std::filesystem::path& root) {
    auto& manager = ObservationManager::instance();
    ObservationYAMLConfig observation;
    observation.enabled = true;
    observation.output_dir = root.string();
    observation.timeline.enabled = false;
    manager.initialize(observation);
    {
        TickSimulationConfig config;
        config.num_threads = 1;
        config.enable_parallel = false;
        TickSimulation sim(config);
        auto* context = manager.createContextForUnit("producer", [] { return uint64_t{0}; });
        CHECK(context);
        context->enableCategory(category::LOG_INFO);
        const auto format = FormatRegistry::instance().registerFormat(
            "started backend {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
        auto* producer = sim.createUnit<Producer>("producer", *context, format);
        auto* backend = manager.backend();
        std::atomic<size_t> received{0};
        backend->setLogHandler([&](auto, const std::byte*, size_t) { ++received; });
        manager.startBackend();
        context->log<LogLevel::Info>(format, uint64_t{0});
        ThreadContextManager::instance().flushAll();
        awaitProgress([&] { return backend->serviceStats().records == 1; }, [] {});
        sim.initialize();  // Keep the already-running standalone scheduler.
        CHECK(manager.backend() == backend && manager.isBackendRunning());
        producer->enabled = true;
        CHECK(sim.run(1) == 1);
        producer->enabled = false;
        // No further simulation polls: its original standalone driver still works.
        awaitProgress([&] { return backend->serviceStats().records == 9; }, [] {});
        sim.finalize();
        manager.stopBackend();
        CHECK(received == 9);
    }
    manager.shutdown();
}

void clockWakeup(const std::filesystem::path& root) {
    for (size_t capacity : {2, 32, 128}) {
        ClockTraceRecorder::Config config;
        config.output_dir = root / std::to_string(capacity);
        config.perfetto = config.lossless = false;
        config.stream_capacity = capacity;
        config.drain_batch = capacity;
        ClockTraceRecorder recorder(config);
        auto* stream =
            recorder.addStream(ClockDomain::fromHz(1, "clock", 1'000'000'000), 1, "producer");
        recorder.start();
        auto& service = ClockTraceStreamTestAccess::service(*stream);
        const auto idle = [&](uint64_t calls, uint64_t records) {
            const auto stats = service.stats();
            return stats.calls >= calls && stats.records == records && !service.ready.load();
        };
        // The initial empty scan and its frontier I/O must finish before publication.
        awaitProgress([&] { return idle(2, 0); }, [] {});
        for (uint64_t n = 0; n < 128; ++n) {
            const auto calls = service.stats().calls;
            stream->record(n, ClockEventKind::User);
            // Require consumption AND a fresh empty scan between sparse records.
            // No advance(), finish(), full-queue assistance or close() can wake it.
            awaitProgress([&] { return idle(calls + 2, n + 1); }, [] {});
        }
        recorder.close();
        CHECK(recorder.stats().events == 128);
        CHECK(recorder.stats().dropped == 0);
    }
}

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-service-progress-" + mode + "-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    if (mode == "clock-wakeup")
        clockWakeup(root);
    else if (mode == "started-backend")
        startedBackend(root);
    else
        shortRuns(root, mode.ends_with("dynamic"), mode.starts_with("clock-"),
                  mode.find("sequential") != std::string::npos ? 1 : 4,
                  mode == "sequential-stride" ? 6 : 1);
    std::filesystem::remove_all(root);
    std::cout << mode << ": service progress passed\n";
}
