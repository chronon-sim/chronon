// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <array>
#include <barrier>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "chronon/CpuPause.hpp"
#include "sender/port/MessageQueue.hpp"
#include "sender/port/Port.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using chronon::sender::DirectSPSCQueueAdapter;
using chronon::sender::LockFreeQueueAdapter;

constexpr uint64_t DEFAULT_ITEMS = 5'000'000;
constexpr uint32_t DEFAULT_REPETITIONS = 5;
constexpr uint64_t MAX_ITEMS = 1'000'000'000;
constexpr uint32_t MAX_REPETITIONS = 100;

struct Sample {
    double nanoseconds = 0.0;
    uint64_t checksum = 0;
};

struct Statistics {
    double median_ns = 0.0;
    double p10_ns = 0.0;
    double p90_ns = 0.0;
    double cv = 0.0;
    uint64_t checksum = 0;
};

[[nodiscard]] uint64_t parseBounded(std::string_view text, uint64_t maximum,
                                    std::string_view name) {
    uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value == 0 || value > maximum) {
        throw std::invalid_argument(std::string(name) + " must be in [1, " +
                                    std::to_string(maximum) + "]");
    }
    return value;
}

[[nodiscard]] double percentile(const std::vector<double>& sorted, double quantile) {
    if (sorted.size() == 1) return sorted.front();
    const double position = quantile * static_cast<double>(sorted.size() - 1);
    const size_t lower = static_cast<size_t>(position);
    const size_t upper = std::min(sorted.size() - 1, lower + 1);
    const double fraction = position - static_cast<double>(lower);
    return sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction;
}

template <typename Runner>
[[nodiscard]] Statistics measure(uint32_t repetitions, Runner&& runner) {
    (void)runner();
    std::vector<double> samples;
    samples.reserve(repetitions);
    uint64_t checksum = 0;
    for (uint32_t repetition = 0; repetition < repetitions; ++repetition) {
        const Sample sample = runner();
        if (repetition != 0 && sample.checksum != checksum) {
            throw std::runtime_error("microbenchmark checksum changed across repetitions");
        }
        checksum = sample.checksum;
        samples.push_back(sample.nanoseconds);
    }
    std::sort(samples.begin(), samples.end());
    const double mean =
        std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size());
    double squared = 0.0;
    for (double sample : samples) squared += (sample - mean) * (sample - mean);
    const double cv = samples.size() < 2 || mean == 0.0
                          ? 0.0
                          : std::sqrt(squared / static_cast<double>(samples.size() - 1)) / mean;
    return {.median_ns = percentile(samples, 0.5),
            .p10_ns = percentile(samples, 0.1),
            .p90_ns = percentile(samples, 0.9),
            .cv = cv,
            .checksum = checksum};
}

template <typename Queue>
[[nodiscard]] Sample runLocal(uint64_t items) {
    Queue queue(256);
    uint64_t checksum = 0;
    const auto begin = Clock::now();
    for (uint64_t cycle = 0; cycle < items; ++cycle) {
        if (!queue.push(cycle, cycle)) {
            throw std::runtime_error("local SPSC queue unexpectedly full");
        }
        auto value = queue.tryPop(cycle);
        if (!value) throw std::runtime_error("local SPSC queue lost a message");
        checksum += *value;
        checksum += queue.admissionOccupancy(cycle);
    }
    const auto end = Clock::now();
    return {.nanoseconds = std::chrono::duration<double, std::nano>(end - begin).count() /
                           static_cast<double>(items),
            .checksum = checksum};
}

template <size_t Bytes>
struct Payload {
    static_assert(Bytes >= sizeof(uint64_t) && Bytes % sizeof(uint64_t) == 0);
    std::array<uint64_t, Bytes / sizeof(uint64_t)> words{};
};

static_assert(sizeof(Payload<8>) == 8);
static_assert(sizeof(Payload<64>) == 64);
static_assert(sizeof(Payload<144>) == 144);
static_assert(sizeof(Payload<256>) == 256);

template <size_t Bytes>
[[nodiscard]] Sample runTwoThread(uint64_t items) {
    using Message = chronon::sender::detail::PortEnvelope<Payload<Bytes>>;
    DirectSPSCQueueAdapter<Message> queue;
    Clock::time_point begin;
    Clock::time_point end;
    uint64_t checksum = 0;
    bool order_ok = true;

    std::barrier start_gate(3, [&]() noexcept { begin = Clock::now(); });
    std::thread producer([&] {
        start_gate.arrive_and_wait();
        for (uint64_t sequence = 0; sequence < items; ++sequence) {
            Message message{};
            message.data.words.front() = sequence;
            message.enqueue_cycle = sequence;
            message.sender_id = 1;
            while (true) {
                // Registered edges query architectural admission before their
                // direct publication. This also retires the consumer's pop
                // ledger and keeps this a production-path measurement.
                (void)queue.admissionOccupancy(sequence);
                if (queue.pushDirect(std::move(message), sequence, 1)) break;
                chronon::cpuPause();
            }
        }
    });
    std::thread consumer([&] {
        start_gate.arrive_and_wait();
        uint64_t expected = 0;
        while (expected < items) {
            const bool consumed = queue.consumeReady(expected, [&](Message& message) {
                const uint64_t sequence = message.data.words.front();
                order_ok = order_ok && sequence == expected;
                checksum += sequence;
                ++expected;
            });
            if (!consumed) chronon::cpuPause();
        }
        end = Clock::now();
    });

    start_gate.arrive_and_wait();
    producer.join();
    consumer.join();
    if (!order_ok || checksum != items * (items - 1) / 2) {
        throw std::runtime_error("two-thread DirectSPSC handoff lost or reordered messages");
    }
    return {.nanoseconds = std::chrono::duration<double, std::nano>(end - begin).count() /
                           static_cast<double>(items),
            .checksum = checksum};
}

void printRow(std::string_view label, const Statistics& result) {
    std::cout << std::left << std::setw(24) << label << std::right << std::fixed
              << std::setprecision(2) << std::setw(12) << result.median_ns << std::setw(12)
              << result.p10_ns << std::setw(12) << result.p90_ns << std::setw(10)
              << 100.0 * result.cv << '%' << std::setw(14) << 1000.0 / result.median_ns
              << "  [chk=" << std::hex << result.checksum << std::dec << "]\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const uint64_t items = argc > 1 ? parseBounded(argv[1], MAX_ITEMS, "items") : DEFAULT_ITEMS;
        const uint32_t repetitions =
            argc > 2 ? static_cast<uint32_t>(parseBounded(argv[2], MAX_REPETITIONS, "repetitions"))
                     : DEFAULT_REPETITIONS;
        if (argc > 3)
            throw std::invalid_argument(
                "usage: chronon_direct_spsc_benchmark [items] [repetitions]");

        std::cout
            << "=== Chronon DirectSPSC microbenchmark ===\n"
            << "items=" << items << " repetitions=" << repetitions
            << " hw-concurrency=" << std::thread::hardware_concurrency() << '\n'
            << "affinity is external; bind producer and consumer to separate physical cores\n\n"
            << "path                      median(ns)     p10(ns)     p90(ns)        CV"
               "        Mmsg/s\n"
            << "------------------------ ------------ ------------ ------------ ----------"
               " --------------\n";

        printRow("legacy local 8B", measure(repetitions, [&] {
                     return runLocal<LockFreeQueueAdapter<uint64_t>>(items);
                 }));
        printRow("direct local 8B", measure(repetitions, [&] {
                     return runLocal<DirectSPSCQueueAdapter<uint64_t>>(items);
                 }));
        printRow("direct 2-thread 8B",
                 measure(repetitions, [&] { return runTwoThread<8>(items); }));
        printRow("direct 2-thread 64B",
                 measure(repetitions, [&] { return runTwoThread<64>(items); }));
        printRow("direct 2-thread 144B",
                 measure(repetitions, [&] { return runTwoThread<144>(items); }));
        printRow("direct 2-thread 256B",
                 measure(repetitions, [&] { return runTwoThread<256>(items); }));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 2;
    }
}
