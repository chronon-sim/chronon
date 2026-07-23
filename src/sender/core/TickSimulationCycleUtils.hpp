// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cstdint>

namespace chronon::sender::detail {

inline constexpr uint64_t kDynamicTickSampleInterval = 256;
inline constexpr uint64_t kNoDynamicTickSample = UINT64_MAX;
inline constexpr uint64_t kDynamicClusterBurstCycles = 4;

/// Start after a complete warm window, then leave a full window between
/// accepted samples. The runtime additionally requires every cluster member
/// to execute at the sampled cycle, so periodic and externally woken units
/// cannot be measured through permanently idle phases.
inline constexpr bool shouldSampleDynamicTick(uint64_t cycle, uint64_t last_sample) noexcept {
    if (cycle < kDynamicTickSampleInterval - 1) return false;
    if (last_sample == kNoDynamicTickSample) return true;
    return cycle >= last_sample && cycle - last_sample >= kDynamicTickSampleInterval;
}

inline uint64_t nextPeriodicCycle(uint64_t cycle, uint64_t period) noexcept {
    if (period == 0) return UINT64_MAX;
    const uint64_t quotient = cycle / period;
    if (quotient >= UINT64_MAX / period) return UINT64_MAX;
    return (quotient + 1) * period;
}

/// Bound a dynamic cluster's local burst by every externally visible
/// scheduler frontier. The dependency proof is exclusive: a cluster proven
/// ready through N may execute cycles [cycle, N).
inline uint64_t dynamicClusterBurstEnd(uint64_t cycle, uint64_t end_cycle,
                                       uint64_t ready_through_cycle,
                                       uint64_t next_counter_cycle = UINT64_MAX) noexcept {
    const uint64_t burst_limit = cycle > UINT64_MAX - kDynamicClusterBurstCycles
                                     ? UINT64_MAX
                                     : cycle + kDynamicClusterBurstCycles;
    const uint64_t frontier = std::min({end_cycle, ready_through_cycle, next_counter_cycle});
    if (burst_limit <= frontier) return burst_limit;
    return std::min(frontier, cycle == UINT64_MAX ? UINT64_MAX : cycle + 1);
}

}  // namespace chronon::sender::detail
