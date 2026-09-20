// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
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
static thread_local bool in_tick = false;

namespace chronon::observe {
struct ObservationBackendTestAccess {
    static size_t arenaBytes(const ObservationBackend& backend) {
        return backend.reorder_buffer_ ? backend.reorder_buffer_->arenaBytesUsed() : 0;
    }
};
}  // namespace chronon::observe

class Producer : public TickableUnit, public ObservableUnit {
public:
    Producer(size_t index, FormatId format, ObservationContext& context, bool burst, bool large)
        : TickableUnit("producer" + std::to_string(index)),
          index_(index),
          format_(format),
          burst_(burst),
          large_(large) {
        setObservationContext(&context);
    }
    void tick() override {
        in_tick = true;
        auto* ctx = observationContext();
        const uint64_t count = burst_ ? 1024 : 1;
        for (uint64_t i = 0; i < count; ++i) {
            const uint64_t id = index_ * 8192 + emitted_++;
            if (large_)
                ctx->log<LogLevel::Info>(format_, id, std::string_view(payload_));
            else
                ctx->log<LogLevel::Info>(format_, id);
        }
        in_tick = false;
    }
    size_t emitted() const { return emitted_; }

private:
    size_t index_, emitted_ = 0;
    FormatId format_;
    bool burst_;
    bool large_;
    std::string payload_ = std::string(1000, 'x');
};

// A second service exercises fair visits independently of observation pressure.
struct Witness : HostService {
    size_t poll(size_t) noexcept override {
        ++calls;
        return 0;
    }
    std::atomic<size_t> calls{0};
};

int main(int argc, char** argv) {
    CHECK(argc == 4);
    const size_t workers = std::stoul(argv[1]);
    const bool dynamic = std::string_view(argv[2]) == "dynamic";
    const std::string mode = argv[3];
    const bool burst = mode != "sparse";
    const bool slow = mode == "slow";
    const bool large = mode == "large";
    auto& threads = ThreadContextManager::instance();
    threads.setQueueCapacity(4096);
    threads.setBackpressurePolicy(BackpressurePolicy::SpinWait);
    const auto format =
        large ? FormatRegistry::instance().registerFormat("event {} {}", __FILE__, __LINE__,
                                                          {ArgType::UInt64, ArgType::String}, true,
                                                          LogLevel::Info)
              : FormatRegistry::instance().registerFormat("event {}", __FILE__, __LINE__,
                                                          {ArgType::UInt64}, true, LogLevel::Info);
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-service-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    TickSimulationConfig cfg;
    cfg.num_threads = workers;
    cfg.enable_parallel = workers > 1;
    cfg.enable_dynamic_rebalance = dynamic;
    cfg.initial_partition_sync_cost_ns = 0;
    TickSimulation sim(cfg);
    ObservationQueue queue(4096);
    ObservationBackend::Config config;
    config.output_dir = root.string();
    config.enable_counter_csv = config.timeline_enabled = false;
    config.enable_reordering = mode != "immediate";
    config.reorder_max_events = large ? 100000 : 64;
    config.service_buffer_bytes = 65536;
    ObservationBackend backend(queue, config);
    backend.attachScheduler(sim.hostServices());
    Witness witness;
    auto witness_registration = sim.hostServices().add(witness);
    witness_registration->setRunnable(false);
    witness_registration->ready.store(true);  // Publication while I/O owns the batch.
    witness_registration->poll(true);         // Even producer assistance must wait.
    CHECK(witness.calls == 0);
    witness_registration->setRunnable(true);
    witness_registration->poll();
    CHECK(witness.calls == 1);
    std::vector<bool> seen(workers * 8192);
    size_t received = 0;
    std::thread::id sink_thread;
    backend.setLogHandler([&](auto, const std::byte* data, size_t size) {
        CHECK(!in_tick);
        CHECK(ObservationBackendTestAccess::arenaBytes(backend) <= config.service_buffer_bytes);
        if (received == 0) sink_thread = std::this_thread::get_id();
        CHECK(sink_thread == std::this_thread::get_id());  // Includes the final reorder tail.
        CHECK(size >= sizeof(StructuredRecord) + sizeof(uint64_t));
        uint64_t id;
        std::memcpy(&id, data + sizeof(StructuredRecord), sizeof(id));
        CHECK(id < seen.size() && !seen[id]);
        seen[id] = true;
        if (++received % 256 == 0 && slow)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    std::vector<std::unique_ptr<ObservationContext>> contexts;
    std::vector<Producer*> units;
    for (size_t i = 0; i < workers; ++i) {
        auto ctx = std::make_unique<ObservationContext>(&queue, [] { return uint64_t{0}; });
        ctx->enableCategory(category::LOG_INFO);
        units.push_back(sim.createUnit<Producer>(i, format, *ctx, burst, large));
        contexts.push_back(std::move(ctx));
    }
    sim.setPrecomputedUnitCosts(std::vector<double>(workers, 1000), {});
    sim.initialize();
    CHECK(sim.useParallelExecution() == (workers > 1));
    backend.start();
    const uint64_t cycles = burst ? 8 : 8192;
    CHECK(sim.run(cycles) == cycles);
    // Repeated runs retain registrations and service progress.
    if (!burst) CHECK(witness.calls > 0);
    backend.stop();
    backend.rethrowIfFailed();
    for (size_t i = 0; i < workers; ++i) {
        CHECK(units[i]->emitted() == 8192);
        CHECK(contexts[i]->observationStats().get<ObservationChannel::Info>().dropped == 0);
        units[i]->setObservationContext(nullptr);
    }
    CHECK(received == workers * 8192);
    CHECK(backend.eventsProcessed() == received);
    const auto stats = backend.serviceStats();
    CHECK(stats.records > 0 && stats.elapsed_ns > 0);
    CHECK(sink_thread != std::thread::id{});
    witness_registration->detach();
    std::filesystem::remove_all(root);
    std::cout << "workers=" << workers << " mode=" << mode << " records=" << received
              << " service_records=" << stats.records << " max_poll_ns=" << stats.max_poll_ns
              << '\n';
}
