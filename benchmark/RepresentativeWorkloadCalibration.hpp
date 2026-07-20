// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "RepresentativeWorkloadOptions.hpp"

namespace chronon::benchmark {

struct CalibrationResult {
    uint64_t measured_cycles = 0;
    double fastest_cycles_per_second = 0.0;
};

template <typename RunProbe>
[[nodiscard]] CalibrationResult calibrateMeasuredCycles(const std::vector<size_t>& workers,
                                                        const RunOptions& options, bool verbose,
                                                        RunProbe&& run_probe) {
    if (!options.target_seconds) {
        return {options.measured_cycles, 0.0};
    }

    constexpr uint64_t INITIAL_PROBE_CYCLES = 256;
    constexpr uint32_t MAX_PROBE_ATTEMPTS = 3;
    constexpr double TARGET_HEADROOM = 1.10;
    const double sample_seconds = std::clamp(*options.target_seconds * 0.10, 0.01, 0.10);
    double fastest_cycles_per_second = 0.0;

    for (size_t worker_count : workers) {
        uint64_t probe_cycles = INITIAL_PROBE_CYCLES;
        double wall_seconds = 0.0;
        for (uint32_t attempt = 0; attempt < MAX_PROBE_ATTEMPTS; ++attempt) {
            RunOptions probe_options = options;
            probe_options.target_seconds.reset();
            probe_options.measured_cycles = probe_cycles;
            wall_seconds = run_probe(worker_count, probe_options);
            if (wall_seconds >= sample_seconds || probe_cycles == MAX_BENCHMARK_CYCLES) break;

            const double scale =
                std::max(2.0, sample_seconds / std::max(wall_seconds, 1e-9) * 1.10);
            const long double next = std::ceil(static_cast<long double>(probe_cycles) * scale);
            probe_cycles = static_cast<uint64_t>(
                std::min<long double>(MAX_BENCHMARK_CYCLES, std::max<long double>(1.0, next)));
        }
        const double cycles_per_second =
            static_cast<double>(probe_cycles) / std::max(wall_seconds, 1e-9);
        fastest_cycles_per_second = std::max(fastest_cycles_per_second, cycles_per_second);
        if (verbose) {
            std::cout << "  calibration workers=" << worker_count
                      << " probe-cycles=" << probe_cycles << " wall=" << wall_seconds
                      << "s rate=" << cycles_per_second / 1e6 << " Mcycles/s\n";
        }
    }

    const long double estimated_cycles =
        std::ceil(static_cast<long double>(fastest_cycles_per_second) * *options.target_seconds *
                  TARGET_HEADROOM);
    if (estimated_cycles > MAX_BENCHMARK_CYCLES) {
        throw std::runtime_error(
            "target duration exceeds the measured-cycle limit for this workload");
    }
    uint64_t measured_cycles = static_cast<uint64_t>(std::max<long double>(1.0, estimated_cycles));
    constexpr uint64_t CYCLE_GRANULARITY = 256;
    if (measured_cycles < MAX_BENCHMARK_CYCLES) {
        measured_cycles = std::min(MAX_BENCHMARK_CYCLES, (measured_cycles + CYCLE_GRANULARITY - 1) /
                                                             CYCLE_GRANULARITY * CYCLE_GRANULARITY);
    }
    return {measured_cycles, fastest_cycles_per_second};
}

}  // namespace chronon::benchmark
