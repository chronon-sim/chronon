// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstdint>

namespace chronon::sender::detail {

inline constexpr uint64_t kDynamicTickSampleInterval = 256;

struct DynamicTickSamplingSchedule {
    uint64_t period = kDynamicTickSampleInterval;
    uint64_t phase = kDynamicTickSampleInterval - 1;
};

/// Keep approximately one sample per target window while guaranteeing that a
/// periodic unit is active at every sample. Intervals below the target are
/// rounded up to the nearest multiple spanning a complete window; longer
/// intervals sample every actual periodic execution.
inline constexpr DynamicTickSamplingSchedule dynamicTickSamplingSchedule(
    uint32_t tick_interval) noexcept {
    if (tick_interval <= 1) return {};
    const uint64_t interval = tick_interval;
    const uint64_t periods_per_sample = (kDynamicTickSampleInterval + interval - 1) / interval;
    return {.period = interval * periods_per_sample, .phase = 0};
}

/// Ordinary clusters sample at the end of a complete warm window. Periodic
/// clusters use a non-zero multiple of their real tick interval, so cost
/// calibration cannot accept four phase-locked idle samples as active work.
inline constexpr bool shouldSampleDynamicTick(uint64_t cycle,
                                              DynamicTickSamplingSchedule schedule = {}) noexcept {
    static_assert((kDynamicTickSampleInterval & (kDynamicTickSampleInterval - 1)) == 0);
    if (schedule.period == 0 || cycle < schedule.phase) return false;
    if (schedule.phase == 0 && cycle == 0) return false;
    return (cycle - schedule.phase) % schedule.period == 0;
}

inline uint64_t nextPeriodicCycle(uint64_t cycle, uint64_t period) noexcept {
    if (period == 0) return UINT64_MAX;
    const uint64_t quotient = cycle / period;
    if (quotient >= UINT64_MAX / period) return UINT64_MAX;
    return (quotient + 1) * period;
}

}  // namespace chronon::sender::detail
