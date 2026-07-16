// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "observe/CounterRegistry.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"
#include "observe/ThreadContextManager.hpp"

using namespace chronon::observe;

namespace {

enum class Mode { UpdatesOff, UpdatesOnly, SnapshotOnly, Csv, Perfetto, Full };

struct Options {
    Mode mode = Mode::SnapshotOnly;
    size_t counters = 64;
    size_t threads = 1;
    uint64_t cycles = 1'000'000;
    uint64_t period = 10'000;
    size_t queue_capacity = 8 * 1024 * 1024;
    std::string output_dir = "/tmp/chronon_counter_periodic_profile";
};

Mode parseMode(std::string_view value) {
    if (value == "updates-off") return Mode::UpdatesOff;
    if (value == "updates-only") return Mode::UpdatesOnly;
    if (value == "snapshot-only") return Mode::SnapshotOnly;
    if (value == "csv") return Mode::Csv;
    if (value == "perfetto") return Mode::Perfetto;
    if (value == "full") return Mode::Full;
    throw std::invalid_argument("unknown mode: " + std::string(value));
}

const char* modeName(Mode mode) {
    switch (mode) {
        case Mode::UpdatesOff:
            return "updates-off";
        case Mode::UpdatesOnly:
            return "updates-only";
        case Mode::SnapshotOnly:
            return "snapshot-only";
        case Mode::Csv:
            return "csv";
        case Mode::Perfetto:
            return "perfetto";
        case Mode::Full:
            return "full";
    }
    return "unknown";
}

uint64_t parseUnsigned(const char* value, const char* option) {
    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (!end || *end != '\0') throw std::invalid_argument(std::string("invalid ") + option);
    return parsed;
}

Options parseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (i + 1 >= argc) throw std::invalid_argument("missing value for " + std::string(arg));
        const char* value = argv[++i];
        if (arg == "--mode") {
            options.mode = parseMode(value);
        } else if (arg == "--counters") {
            options.counters = parseUnsigned(value, "--counters");
        } else if (arg == "--threads") {
            options.threads = parseUnsigned(value, "--threads");
        } else if (arg == "--cycles") {
            options.cycles = parseUnsigned(value, "--cycles");
        } else if (arg == "--period") {
            options.period = parseUnsigned(value, "--period");
        } else if (arg == "--queue-capacity") {
            options.queue_capacity = parseUnsigned(value, "--queue-capacity");
        } else if (arg == "--output-dir") {
            options.output_dir = value;
        } else {
            throw std::invalid_argument("unknown option: " + std::string(arg));
        }
    }
    if (options.threads == 0 || options.threads > ThreadContextManager::MAX_THREADS) {
        throw std::invalid_argument("--threads must be between 1 and 64");
    }
    if (options.counters == 0 || options.cycles == 0) {
        throw std::invalid_argument("--counters and --cycles must be nonzero");
    }
    if (options.mode != Mode::UpdatesOff && options.mode != Mode::UpdatesOnly &&
        options.period == 0) {
        throw std::invalid_argument("--period must be nonzero for snapshot modes");
    }
    return options;
}

uint64_t directoryBytes(const std::filesystem::path& root) {
    uint64_t bytes = 0;
    std::error_code ec;
    for (std::filesystem::recursive_directory_iterator it(root, ec), end; it != end && !ec;
         it.increment(ec)) {
        if (it->is_regular_file(ec)) bytes += it->file_size(ec);
    }
    return bytes;
}

template <Mode BenchmarkMode>
int run(const Options& options) {
    constexpr bool updates = BenchmarkMode != Mode::UpdatesOff;
    constexpr bool snapshots =
        BenchmarkMode != Mode::UpdatesOff && BenchmarkMode != Mode::UpdatesOnly;
    constexpr bool output = BenchmarkMode == Mode::Csv || BenchmarkMode == Mode::Perfetto ||
                            BenchmarkMode == Mode::Full;

    ObservationQueue shared(options.queue_capacity);
    std::vector<std::unique_ptr<ObservationContext>> contexts;
    std::vector<std::vector<SimpleCounter*>> counters(options.threads);
    contexts.reserve(options.threads);
    for (size_t owner = 0; owner < options.threads; ++owner) {
        auto context = std::make_unique<ObservationContext>(
            &shared, [] { return uint64_t{0}; }, static_cast<uint32_t>(owner),
            "worker_" + std::to_string(owner));
        context->setCounterOwnerId(owner);
        counters[owner].reserve(options.counters);
        std::vector<CounterId> ids;
        ids.reserve(options.counters);
        for (size_t i = 0; i < options.counters; ++i) {
            ids.push_back(context->counters().addCounter("counter_" + std::to_string(i)));
        }
        // addCounter() may grow the backing vector; take stable addresses only
        // after registration is complete.
        for (CounterId id : ids) {
            counters[owner].push_back(&context->counters().getUnchecked(id));
        }
        contexts.push_back(std::move(context));
    }

    CounterRegistry registry;
    registry.reregisterAll(contexts);

    std::unique_ptr<ObservationBackend> backend;
    std::filesystem::path actual_output;
    if constexpr (output) {
        ThreadContextManager::instance().setQueueCapacity(options.queue_capacity);
        ObservationBackend::Config config;
        config.output_dir = options.output_dir;
        config.enable_counter_csv = BenchmarkMode == Mode::Csv || BenchmarkMode == Mode::Full;
        config.counter_csv_format = CounterCsvFormat::Pivoted;
        config.timeline_enabled = BenchmarkMode == Mode::Perfetto || BenchmarkMode == Mode::Full;
        config.timeline_counters = config.timeline_enabled;
        config.timeline_trace_events = false;
        config.enable_reordering = true;
        backend = std::make_unique<ObservationBackend>(shared, config);
        backend->setCounterSnapshotPlans(registry.snapshotPlans());
        backend->start();
        actual_output = backend->outputDir();
    }

    std::chrono::steady_clock::time_point wall_begin;
    auto mark_wall_begin = [&wall_begin]() noexcept {
        wall_begin = std::chrono::steady_clock::now();
    };
    std::barrier start_line(static_cast<std::ptrdiff_t>(options.threads + 1), mark_wall_begin);
    std::atomic<uint64_t> snapshot_ns{0};
    std::atomic<uint64_t> queue_bytes{0};
    std::atomic<uint64_t> dropped{0};
    std::atomic<uint64_t> checksum{0};
    std::vector<std::thread> workers;
    workers.reserve(options.threads);

    for (size_t owner = 0; owner < options.threads; ++owner) {
        workers.emplace_back([&, owner] {
            ThreadContext local_context(static_cast<uint32_t>(owner), options.queue_capacity);
            ThreadContext* producer = &local_context;
            if constexpr (output) {
                producer = ThreadContextManager::instance().getContext();
                if (!producer) throw std::runtime_error("thread context pool exhausted");
            }

            uint64_t local_checksum = owner + 1;
            start_line.arrive_and_wait();
            for (uint64_t cycle = 1; cycle <= options.cycles; ++cycle) {
                local_checksum = local_checksum * 6364136223846793005ULL + cycle;
                if constexpr (updates) {
                    for (SimpleCounter* counter : counters[owner]) counter->increment();
                }
                if constexpr (snapshots) {
                    if (cycle % options.period == 0) {
                        const auto begin = std::chrono::steady_clock::now();
                        const size_t owner_id = owner;
                        (void)registry.pushOwnerSnapshots(
                            cycle, std::span<const size_t>(&owner_id, 1), *producer);
                        const auto end = std::chrono::steady_clock::now();
                        snapshot_ns.fetch_add(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                                .count(),
                            std::memory_order_relaxed);

                        if constexpr (BenchmarkMode == Mode::SnapshotOnly) {
                            auto& queue = producer->queue();
                            while (std::byte* ptr = queue.prepareRead()) {
                                const auto* header =
                                    reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
                                queue.finishRead(header->total_size);
                            }
                            queue.forceCommitRead();
                        }
                    }
                }
            }
            producer->queue().forceCommitWrite();
            queue_bytes.fetch_add(producer->queue().bytesWritten(), std::memory_order_relaxed);
            dropped.fetch_add(producer->droppedCount(), std::memory_order_relaxed);
            checksum.fetch_xor(local_checksum, std::memory_order_relaxed);
        });
    }

    start_line.arrive_and_wait();
    for (auto& worker : workers) worker.join();
    const auto simulation_end = std::chrono::steady_clock::now();
    if (backend) backend->stop();
    const auto wall_end = std::chrono::steady_clock::now();

    const double simulation_seconds =
        std::chrono::duration<double>(simulation_end - wall_begin).count();
    const double total_seconds = std::chrono::duration<double>(wall_end - wall_begin).count();
    const uint64_t snapshots_per_thread = snapshots ? options.cycles / options.period : 0;
    const uint64_t snapshot_count = snapshots_per_thread * options.threads;
    const uint64_t output_bytes = actual_output.empty() ? 0 : directoryBytes(actual_output);
    double snapshot_ns_each = 0.0;
    double queue_bytes_each = 0.0;
    if constexpr (snapshots) {
        if (snapshot_count != 0) {
            snapshot_ns_each = static_cast<double>(snapshot_ns.load()) / snapshot_count;
            queue_bytes_each = static_cast<double>(queue_bytes.load()) / snapshot_count;
        }
    }

    std::cout << "RESULT"
              << " mode=" << modeName(BenchmarkMode) << " counters=" << options.counters
              << " threads=" << options.threads << " period=" << options.period
              << " cycles=" << options.cycles << " simulation_seconds=" << simulation_seconds
              << " total_seconds=" << total_seconds
              << " cycles_per_second=" << static_cast<double>(options.cycles) / simulation_seconds
              << " snapshot_ns=" << snapshot_ns_each << " queue_bytes=" << queue_bytes.load()
              << " queue_bytes_per_snapshot=" << queue_bytes_each << " dropped=" << dropped.load()
              << " output_bytes=" << output_bytes
              << " output_bytes_per_second=" << static_cast<double>(output_bytes) / total_seconds
              << " checksum=" << checksum.load() << '\n';
    return dropped.load() == 0 ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parseOptions(argc, argv);
        switch (options.mode) {
            case Mode::UpdatesOff:
                return run<Mode::UpdatesOff>(options);
            case Mode::UpdatesOnly:
                return run<Mode::UpdatesOnly>(options);
            case Mode::SnapshotOnly:
                return run<Mode::SnapshotOnly>(options);
            case Mode::Csv:
                return run<Mode::Csv>(options);
            case Mode::Perfetto:
                return run<Mode::Perfetto>(options);
            case Mode::Full:
                return run<Mode::Full>(options);
        }
    } catch (const std::exception& error) {
        std::cerr << "counter periodic benchmark: " << error.what() << '\n';
        return 1;
    }
    return 1;
}
