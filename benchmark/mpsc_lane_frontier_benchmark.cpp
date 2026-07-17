// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>

#include "sender/port/MessageQueue.hpp"

namespace {

using Queue = chronon::sender::MultiProducerQueueAdapter<uint64_t>;

struct Result {
    double sparse_roundtrip_ns;
    double four_way_ns_per_message;
    double empty_min_arrival_ns;
    uint64_t checksum;
    size_t storage_capacity;
};

template <size_t LaneCount>
Result measure() {
    constexpr uint64_t kReadyCycle = std::numeric_limits<uint64_t>::max();
    constexpr size_t kWarmup = 20'000;
    constexpr size_t kSparseIterations = 1'000'000;
    constexpr size_t kBurstIterations = 250'000;
    constexpr size_t kEmptyIterations = 1'000'000;

    // Eight physical entries are enough for this benchmark. This also checks
    // that bounded direct lanes do not inherit the historical 4096-slot floor.
    Queue queue(std::numeric_limits<size_t>::max(), 8);
    std::vector<size_t> lanes;
    lanes.reserve(LaneCount);
    for (size_t lane = 0; lane < LaneCount; ++lane) {
        lanes.push_back(queue.addProducerThread(lane + 1));
    }

    const std::array<size_t, 4> active{lanes[1 % LaneCount], lanes[(LaneCount / 3) % LaneCount],
                                       lanes[(2 * LaneCount / 3) % LaneCount],
                                       lanes[LaneCount - 1]};

    uint64_t checksum = 0;
    auto roundtrip = [&](size_t iteration) {
        const size_t lane = active[iteration & 3U];
        const uint64_t payload = (static_cast<uint64_t>(iteration) << 16U) ^ lane;
        if (!queue.pushFromThread(lane, payload, iteration, static_cast<uint32_t>(lane))) {
            throw std::runtime_error("MPSC benchmark lane unexpectedly full");
        }
        auto value = queue.tryPop(kReadyCycle);
        if (!value) throw std::runtime_error("MPSC benchmark lost a sparse message");
        checksum += *value;
    };

    for (size_t i = 0; i < kWarmup; ++i) roundtrip(i);
    auto begin = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kSparseIterations; ++i) roundtrip(i + kWarmup);
    auto end = std::chrono::steady_clock::now();
    const double sparse_ns =
        std::chrono::duration<double, std::nano>(end - begin).count() / kSparseIterations;

    begin = std::chrono::steady_clock::now();
    for (size_t iteration = 0; iteration < kBurstIterations; ++iteration) {
        for (size_t pos = active.size(); pos-- > 0;) {
            const size_t lane = active[pos];
            const uint64_t payload = (static_cast<uint64_t>(iteration) << 16U) ^ lane;
            if (!queue.pushFromThread(lane, payload, iteration, static_cast<uint32_t>(lane))) {
                throw std::runtime_error("MPSC benchmark lane unexpectedly full in burst");
            }
        }
        for (size_t pos = 0; pos < active.size(); ++pos) {
            auto value = queue.tryPop(kReadyCycle);
            if (!value) throw std::runtime_error("MPSC benchmark lost a burst message");
            checksum += *value;
        }
    }
    end = std::chrono::steady_clock::now();
    const double burst_ns = std::chrono::duration<double, std::nano>(end - begin).count() /
                            (kBurstIterations * active.size());

    begin = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kEmptyIterations; ++i) {
        checksum += queue.minArrivalCycle().has_value();
    }
    end = std::chrono::steady_clock::now();
    const double empty_ns =
        std::chrono::duration<double, std::nano>(end - begin).count() / kEmptyIterations;

    return Result{.sparse_roundtrip_ns = sparse_ns,
                  .four_way_ns_per_message = burst_ns,
                  .empty_min_arrival_ns = empty_ns,
                  .checksum = checksum,
                  .storage_capacity = queue.storageCapacity()};
}

template <size_t LaneCount>
void printResult() {
    const Result result = measure<LaneCount>();
    std::cout << "lanes=" << std::setw(3) << LaneCount
              << " mode=" << (LaneCount < Queue::kFrontierLaneThreshold ? "scan    " : "frontier")
              << " payload-storage/lane=" << result.storage_capacity << std::fixed
              << std::setprecision(2) << " sparse-ns=" << result.sparse_roundtrip_ns
              << " four-way-ns/msg=" << result.four_way_ns_per_message
              << " empty-min-ns=" << result.empty_min_arrival_ns << " checksum=" << result.checksum
              << '\n';
}

}  // namespace

int main() {
    printResult<16>();
    printResult<64>();
    printResult<256>();
}
