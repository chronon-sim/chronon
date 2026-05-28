// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "../../chronon/CpuPause.hpp"

namespace chronon::sender {

/** @brief Measured platform synchronization costs. */
struct PlatformMetrics {
    double atomic_roundtrip_ns;  ///< One-way atomic sync cost in ns.
};

/**
 * @brief Measures cross-thread atomic sync cost via ping-pong on this hardware.
 *
 * Used by the weighted partitioner to account for sync overhead when placing
 * units on different threads.
 */
class PlatformBenchmark {
public:
    static PlatformMetrics measure(size_t iterations = 10000) {
        PlatformMetrics metrics;

        // Warmup so caches are hot before the timed loop.
        measureAtomicRoundtrip_(1000);

        double roundtrip_ns = measureAtomicRoundtrip_(iterations);
        metrics.atomic_roundtrip_ns = roundtrip_ns / 2.0;

        return metrics;
    }

private:
    static double measureAtomicRoundtrip_(size_t iterations) {
        alignas(64) std::atomic<uint64_t> flag_a{0};
        alignas(64) std::atomic<uint64_t> flag_b{0};

        std::thread thread_b([&]() {
            for (size_t i = 1; i <= iterations; ++i) {
                while (flag_a.load(std::memory_order_acquire) != i) {
                    cpuPause();
                }
                flag_b.store(i, std::memory_order_release);
            }
        });

        auto start = std::chrono::steady_clock::now();

        for (size_t i = 1; i <= iterations; ++i) {
            flag_a.store(i, std::memory_order_release);
            while (flag_b.load(std::memory_order_acquire) != i) {
                cpuPause();
            }
        }

        auto end = std::chrono::steady_clock::now();
        thread_b.join();

        double elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
        return elapsed_ns / static_cast<double>(iterations);
    }
};

}  // namespace chronon::sender
