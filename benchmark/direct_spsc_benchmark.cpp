// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string_view>

#include "sender/port/MessageQueue.hpp"

namespace {

constexpr uint64_t kIterations = 10'000'000;

template <typename Queue>
double run(Queue& queue) {
    uint64_t checksum = 0;
    const auto begin = std::chrono::steady_clock::now();
    for (uint64_t cycle = 0; cycle < kIterations; ++cycle) {
        if (!queue.push(cycle, cycle)) return 0.0;
        auto value = queue.tryPop(cycle);
        checksum += *value;
        checksum += queue.admissionOccupancy(cycle);
    }
    const auto end = std::chrono::steady_clock::now();
    if (checksum == UINT64_MAX) std::cerr << checksum;
    return std::chrono::duration<double, std::nano>(end - begin).count() /
           static_cast<double>(kIterations);
}

}  // namespace

int main() {
    chronon::sender::LockFreeQueueAdapter<uint64_t> legacy(256);
    chronon::sender::DirectSPSCQueueAdapter<uint64_t> direct(256);
    const double legacy_ns = run(legacy);
    const double direct_ns = run(direct);
    std::cout << std::fixed << std::setprecision(2) << "legacy: " << legacy_ns
              << " ns/message\ndirect: " << direct_ns
              << " ns/message\nspeedup: " << legacy_ns / direct_ns << "x\n";
}
