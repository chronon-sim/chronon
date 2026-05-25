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

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

namespace chronon::sender {

/** @brief Measured platform synchronization costs. */
struct PlatformMetrics {
    double atomic_roundtrip_ns;  ///< One-way atomic sync cost in ns.
    double rdtsc_to_ns;          ///< Calibrated rdtsc ticks per nanosecond.
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
        metrics.rdtsc_to_ns = calibrateRdtsc_();

        // Warmup so caches are hot before the timed loop.
        measureAtomicRoundtrip_(1000);

        double roundtrip_ns = measureAtomicRoundtrip_(iterations);
        metrics.atomic_roundtrip_ns = roundtrip_ns / 2.0;

        return metrics;
    }

private:
    static double calibrateRdtsc_() {
#if defined(__x86_64__) || defined(_M_X64)
        auto start_time = std::chrono::steady_clock::now();
        uint64_t start_tsc = __rdtsc();

        uint64_t dummy = 0;
        for (uint64_t i = 0; i < 10'000'000; ++i) {
            dummy += i;
            asm volatile("" : "+r"(dummy));
        }

        uint64_t end_tsc = __rdtsc();
        auto end_time = std::chrono::steady_clock::now();

        double elapsed_ns = static_cast<double>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count());
        double elapsed_tsc = static_cast<double>(end_tsc - start_tsc);

        if (elapsed_ns > 0) {
            return elapsed_tsc / elapsed_ns;
        }
#endif
        return 1.0;  // Fallback when rdtsc isn't available.
    }

    static double measureAtomicRoundtrip_(size_t iterations) {
        alignas(64) std::atomic<uint64_t> flag_a{0};
        alignas(64) std::atomic<uint64_t> flag_b{0};

        std::thread thread_b([&]() {
            for (size_t i = 1; i <= iterations; ++i) {
                while (flag_a.load(std::memory_order_acquire) != i) {
#if defined(__x86_64__) || defined(_M_X64)
                    _mm_pause();
#elif defined(__aarch64__)
                    asm volatile("yield" ::: "memory");
#endif
                }
                flag_b.store(i, std::memory_order_release);
            }
        });

        auto start = std::chrono::steady_clock::now();

        for (size_t i = 1; i <= iterations; ++i) {
            flag_a.store(i, std::memory_order_release);
            while (flag_b.load(std::memory_order_acquire) != i) {
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
#endif
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
