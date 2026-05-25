// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <numeric>
#include <vector>

#include "../core/TickableUnit.hpp"

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

namespace chronon::sender {

/** @brief Trimmed-mean and median tick time for one unit. */
struct UnitTickProfile {
    double mean_ns;  ///< Trimmed mean (top/bottom 10% removed).
    double median_ns;
    uint64_t sample_count;
};

/**
 * @brief Profiles per-unit tick() cost via rdtsc (x86_64) or steady_clock.
 *
 * Runs warmup → measurement; trims top/bottom 10% to remove outliers. Results feed the
 * weighted partitioner for cost-aware thread assignment.
 */
class TickCostProfiler {
public:
    static std::vector<UnitTickProfile> profile(std::vector<TickableUnit*>& units,
                                                double rdtsc_to_ns, uint64_t warmup_cycles = 512,
                                                uint64_t measurement_cycles = 1024) {
        const size_t num_units = units.size();
        if (num_units == 0) {
            return {};
        }

        // Warmup: caches, branch predictors, data caches.
        for (uint64_t cycle = 0; cycle < warmup_cycles; ++cycle) {
            for (size_t u = 0; u < num_units; ++u) {
                units[u]->executeTick();
            }
        }

        std::vector<std::vector<uint64_t>> samples(num_units);
        for (size_t u = 0; u < num_units; ++u) {
            samples[u].reserve(measurement_cycles);
        }

        for (uint64_t cycle = 0; cycle < measurement_cycles; ++cycle) {
            for (size_t u = 0; u < num_units; ++u) {
                uint64_t t0 = readTimestamp_();
                units[u]->executeTick();
                uint64_t t1 = readTimestamp_();
                samples[u].push_back(t1 - t0);
            }
        }

        std::vector<UnitTickProfile> profiles(num_units);
        for (size_t u = 0; u < num_units; ++u) {
            profiles[u] = aggregateSamples_(samples[u], rdtsc_to_ns);
        }

        return profiles;
    }

private:
    /// rdtsc on x86_64 (~1 ns resolution); steady_clock fallback elsewhere.
    static uint64_t readTimestamp_() {
#if defined(__x86_64__) || defined(_M_X64)
        return __rdtsc();
#else
        auto now = std::chrono::steady_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
#endif
    }

    static UnitTickProfile aggregateSamples_(std::vector<uint64_t>& raw_samples,
                                             double rdtsc_to_ns) {
        UnitTickProfile profile{};
        if (raw_samples.empty()) {
            return profile;
        }

        profile.sample_count = raw_samples.size();

        std::sort(raw_samples.begin(), raw_samples.end());

        size_t trim_count = raw_samples.size() / 10;
        size_t trimmed_start = trim_count;
        size_t trimmed_end = raw_samples.size() - trim_count;
        if (trimmed_start >= trimmed_end) {
            trimmed_start = 0;
            trimmed_end = raw_samples.size();
        }

        double sum = 0.0;
        for (size_t i = trimmed_start; i < trimmed_end; ++i) {
            sum += static_cast<double>(raw_samples[i]);
        }
        size_t trimmed_count = trimmed_end - trimmed_start;
        double mean_ticks = sum / static_cast<double>(trimmed_count);

        size_t mid = raw_samples.size() / 2;
        double median_ticks;
        if (raw_samples.size() % 2 == 0) {
            median_ticks = static_cast<double>(raw_samples[mid - 1] + raw_samples[mid]) / 2.0;
        } else {
            median_ticks = static_cast<double>(raw_samples[mid]);
        }

#if defined(__x86_64__) || defined(_M_X64)
        if (rdtsc_to_ns > 0) {
            profile.mean_ns = mean_ticks / rdtsc_to_ns;
            profile.median_ns = median_ticks / rdtsc_to_ns;
        } else {
            profile.mean_ns = mean_ticks;
            profile.median_ns = median_ticks;
        }
#else
        // On non-x86, timestamps are already in nanoseconds.
        (void)rdtsc_to_ns;
        profile.mean_ns = mean_ticks;
        profile.median_ns = median_ticks;
#endif

        return profile;
    }
};

}  // namespace chronon::sender
