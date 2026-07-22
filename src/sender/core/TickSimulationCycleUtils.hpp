// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstdint>

namespace chronon::sender::detail {

inline constexpr uint64_t kDynamicTickSampleInterval = 256;
inline constexpr uint64_t kNoDynamicTickSample = UINT64_MAX;

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

}  // namespace chronon::sender::detail
