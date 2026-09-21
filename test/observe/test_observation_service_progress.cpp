// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <optional>
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
struct ObservationBackendTestAccess {
    static HostServices* standaloneScheduler(ObservationBackend& backend) {
        return backend.standalone_scheduler_.get();
    }
};
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

void reattachedBackend(const std::filesystem::path& root) {
    auto& threads = ThreadContextManager::instance();
    threads.setQueueCapacity(4096);
    threads.setBackpressurePolicy(BackpressurePolicy::Drop);
    std::atomic<bool> driver_exited{false};
    std::atomic<size_t> received{0};
    HostServices scheduler;
    ObservationQueue queue(4096);
    ObservationBackend::Config output;
    output.output_dir = root.string();
    output.enable_counter_csv = output.timeline_enabled = output.enable_reordering = false;
    ObservationBackend backend(queue, output);
    backend.setLogHandler([&](auto, const std::byte*, size_t) { ++received; });
    ObservationContext context(&queue, [] { return uint64_t{0}; });
    context.enableCategory(category::LOG_INFO);
    const auto format = FormatRegistry::instance().registerFormat(
        "reattached backend {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    backend.start();
    auto* standalone = ObservationBackendTestAccess::standaloneScheduler(backend);
    CHECK(standalone);
    // A TLS destructor observes the actual driver thread exit without relying
    // on sleeps, process-wide thread counts, or reusable thread IDs.
    struct DriverExit {
        std::atomic<bool>& exited;
        ~DriverExit() { exited.store(true); }
    };
    auto marker = standalone->addIO(nullptr, &driver_exited, [](void* flag) noexcept {
        thread_local DriverExit on_exit{*static_cast<std::atomic<bool>*>(flag)};
    });
    marker->submit();
    marker->wait();
    marker.reset();  // Do not keep the old executor alive through the test job.
    backend.stop();
    backend.rethrowIfFailed();
    CHECK(!driver_exited);
    // A standalone restart must retain its scheduler and still make progress.
    backend.start();
    CHECK(ObservationBackendTestAccess::standaloneScheduler(backend) == standalone);
    context.log<LogLevel::Info>(format, uint64_t{0});
    threads.flushAll();
    awaitProgress([&] { return received == 1; }, [] {});
    backend.stop();
    backend.rethrowIfFailed();
    CHECK(!driver_exited);
    for (size_t session = 1; session <= 2; ++session) {
        backend.attachScheduler(scheduler);
        CHECK(driver_exited);  // Reattachment joins the retired standalone driver.
        CHECK(!ObservationBackendTestAccess::standaloneScheduler(backend));
        backend.start();
        context.log<LogLevel::Info>(format, uint64_t{session});
        threads.flushAll();
        size_t cursor = 0;
        awaitProgress([&] { return received == session + 1; }, [&] { scheduler.poll(cursor); });
        backend.stop();
        backend.rethrowIfFailed();
    }
    CHECK(context.observationStats().get<ObservationChannel::Info>().dropped == 0);
}

void reusedScheduler(const std::filesystem::path& root) {
    auto& threads = ThreadContextManager::instance();
    threads.setQueueCapacity(4096);
    threads.setBackpressurePolicy(BackpressurePolicy::Drop);
    std::atomic<size_t> received{0};
    Witness witness;
    std::optional<HostServices> scheduler(std::in_place);
    auto witness_registration = scheduler->add(witness);
    ObservationQueue queue(4096);
    ObservationBackend::Config output;
    output.output_dir = root.string();
    output.enable_counter_csv = output.timeline_enabled = output.enable_reordering = false;
    ObservationBackend backend(queue, output);
    backend.setLogHandler([&](auto, const std::byte*, size_t) { ++received; });
    ObservationContext context(&queue, [] { return uint64_t{0}; });
    context.enableCategory(category::LOG_INFO);
    const auto format = FormatRegistry::instance().registerFormat(
        "reused scheduler {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    for (size_t session = 0; session < 32; ++session) {
        backend.attachScheduler(*scheduler);
        backend.attachScheduler(*scheduler);  // Also cover repeated attachment before start.
        backend.start();
        const auto records = backend.serviceStats().records;
        context.log<LogLevel::Info>(format, uint64_t{session});
        threads.flushAll();
        size_t cursor = 0;
        // One witness and one backend: a single round must reach the backend,
        // regardless of the number of earlier attachments or stopped sessions.
        scheduler->poll(cursor);
        scheduler->poll(cursor);
        CHECK(backend.serviceStats().records == records + 1);
        awaitProgress([&] { return received == session + 1; }, [] {});
        backend.stop();
        backend.rethrowIfFailed();
    }
    // The backend's I/O job outlives its scheduler. Reuse the scheduler's exact
    // address to catch ownership checks based only on a cached raw pointer.
    auto* address = &*scheduler;
    scheduler.emplace();
    CHECK(&*scheduler == address);
    witness_registration->poll(true);
    CHECK(witness.calls == 1);  // Retired registrations stay detached.
    backend.attachScheduler(*scheduler);
    backend.start();
    const auto records = backend.serviceStats().records;
    context.log<LogLevel::Info>(format, uint64_t{32});
    threads.flushAll();
    size_t cursor = 0;
    scheduler->poll(cursor);  // The replacement scheduler has just the backend.
    CHECK(backend.serviceStats().records == records + 1);
    awaitProgress([&] { return received == 33; }, [] {});
    backend.stop();
    backend.rethrowIfFailed();
    CHECK(context.observationStats().get<ObservationChannel::Info>().dropped == 0);
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

struct ClockProducer : TickableUnit {
    size_t burst = 0;
    explicit ClockProducer(std::string name) : TickableUnit(std::move(name)) {}
    void tick() override {
        for (size_t n = 0; n < burst; ++n)
            clockTraceStream()->record(localCycle(), ClockEventKind::User);
    }
};

void coordinatedClockWakeup(const std::filesystem::path& root, size_t capacity,
                            size_t configurations = 1) {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "fast", 1'000'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "slow", 500'000'000));
    auto* producer = sim.createUnitInDomain<ClockProducer>(1, "producer");
    sim.createUnitInDomain<ClockProducer>(2, "quiet");
    ClockTraceRecorder::Config output;
    output.output_dir = root;
    output.lossless = false;
    output.stream_capacity = output.drain_batch = capacity;
    output.perfetto_options.compress = capacity == 32;
    for (size_t n = 0; n < configurations; ++n) {
        output.output_dir = root / std::to_string(n);
        sim.configureClockTrace(output);
    }
    sim.initialize();
    CHECK(!sim.useParallelExecution());
    auto& service = ClockTraceStreamTestAccess::service(*producer->clockTraceStream());
    const auto idle = [&](uint64_t calls, uint64_t records) {
        const auto stats = service.stats();
        return stats.calls >= calls && stats.records == records && !service.ready.load();
    };
    size_t cursor = 0;
    const auto poll = [&] { sim.hostServices().poll(cursor); };
    awaitProgress([&] { return idle(1, 0); }, poll);
    uint64_t expected = 0;
    for (size_t batch = 0; batch < 8; ++batch) {
        const auto calls = service.stats().calls;
        producer->burst = batch % 2 ? capacity : 1;
        expected += producer->burst;
        CHECK(sim.runClockEvents(1) == 1);
        if (configurations > 1) {
            // Replaced recorders must not leave dead slots ahead of the active
            // service. One scheduler poll must still drain this small ring.
            cursor = 0;
            poll();
            CHECK(service.stats().records == expected);
        }
        // Stay below the 64-batch watermark boundary. Ordinary scheduler polls
        // must drain both sparse records and full bursts before the next batch,
        // without advance(), close() or full-queue producer assistance.
        awaitProgress([&] { return idle(calls + 2, expected); }, poll);
    }
    sim.closeClockTrace();
    CHECK(sim.clockTraceRecorder()->stats().events == expected);
    CHECK(sim.clockTraceRecorder()->stats().dropped == 0);
    for (size_t n = 0; n + 1 < configurations; ++n)
        CHECK(!std::filesystem::exists(root / std::to_string(n)));
}

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-service-progress-" + mode + "-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    if (mode == "clock-wakeup")
        clockWakeup(root);
    else if (mode == "clock-reconfigure")
        coordinatedClockWakeup(root, 2, 33);
    else if (mode.starts_with("clock-coordinated-"))
        coordinatedClockWakeup(root, std::stoull(mode.substr(mode.find_last_of('-') + 1)));
    else if (mode == "started-backend")
        startedBackend(root);
    else if (mode == "reattached-backend")
        reattachedBackend(root);
    else if (mode == "scheduler-reuse")
        reusedScheduler(root);
    else
        shortRuns(root, mode.ends_with("dynamic"), mode.starts_with("clock-"),
                  mode.find("sequential") != std::string::npos ? 1 : 4,
                  mode == "sequential-stride" ? 6 : 1);
    std::filesystem::remove_all(root);
    std::cout << mode << ": service progress passed\n";
}
