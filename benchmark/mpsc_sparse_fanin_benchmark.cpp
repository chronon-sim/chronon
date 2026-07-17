// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>

#include "sender/port/MessageQueue.hpp"

namespace {

using chronon::sender::MultiProducerQueueAdapter;

struct Result {
    double empty_ns = 0.0;
    double sparse_ns = 0.0;
    uint64_t checksum = 0;
};

Result runOnce(size_t lanes, size_t active_lanes, uint64_t iterations, bool bitmap) {
    setenv("CHRONON_EXPERIMENTAL_MPSC_ACTIVE_LANES", bitmap ? "1" : "0", 1);
    MultiProducerQueueAdapter<uint64_t> queue;
    std::vector<size_t> queue_ids;
    for (size_t i = 0; i < lanes; ++i) queue_ids.push_back(queue.addProducerThread(i + 1));

    auto begin = std::chrono::steady_clock::now();
    uint64_t checksum = 0;
    for (uint64_t i = 0; i < iterations; ++i) checksum += queue.tryPop(0).has_value();
    auto middle = std::chrono::steady_clock::now();

    for (uint64_t i = 0; i < iterations; ++i) {
        const size_t lane = queue_ids[i % active_lanes];
        if (!queue.pushFromThread(lane, i, 0, static_cast<uint32_t>(lane + 1))) std::abort();
        auto value = queue.tryPop(0);
        if (!value || *value != i) std::abort();
        checksum += *value;
    }
    auto end = std::chrono::steady_clock::now();
    const double divisor = static_cast<double>(iterations);
    return {.empty_ns = std::chrono::duration<double, std::nano>(middle - begin).count() / divisor,
            .sparse_ns = std::chrono::duration<double, std::nano>(end - middle).count() / divisor,
            .checksum = checksum};
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const size_t middle = values.size() / 2;
    if (values.size() % 2 != 0) return values[middle];
    return (values[middle - 1] + values[middle]) / 2.0;
}

}  // namespace

int main(int argc, char** argv) {
    const size_t lanes = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 64;
    const size_t active_lanes = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1;
    const uint64_t iterations = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 1'000'000;
    const size_t repeats = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 5;
    if (lanes == 0 || active_lanes == 0 || active_lanes > lanes || repeats == 0) return 2;

    std::vector<double> scan_empty;
    std::vector<double> scan_sparse;
    std::vector<double> bitmap_empty;
    std::vector<double> bitmap_sparse;
    uint64_t checksum = 0;
    for (size_t i = 0; i < repeats; ++i) {
        const auto scan = runOnce(lanes, active_lanes, iterations, false);
        const auto bitmap = runOnce(lanes, active_lanes, iterations, true);
        scan_empty.push_back(scan.empty_ns);
        scan_sparse.push_back(scan.sparse_ns);
        bitmap_empty.push_back(bitmap.empty_ns);
        bitmap_sparse.push_back(bitmap.sparse_ns);
        checksum ^= scan.checksum ^ bitmap.checksum;
    }
    unsetenv("CHRONON_EXPERIMENTAL_MPSC_ACTIVE_LANES");

    std::cout << std::fixed << std::setprecision(2) << "lanes=" << lanes
              << " active=" << active_lanes << " empty_scan_ns=" << median(scan_empty)
              << " empty_bitmap_ns=" << median(bitmap_empty)
              << " sparse_scan_ns=" << median(scan_sparse)
              << " sparse_bitmap_ns=" << median(bitmap_sparse) << " checksum=" << checksum << '\n';
}
