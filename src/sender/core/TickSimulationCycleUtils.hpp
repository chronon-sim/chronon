// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstdint>

namespace chronon::sender::detail {

inline constexpr uint64_t kDynamicTickSampleInterval = 256;

/// Sample after a complete window so cycle-zero initialization and cold-cache
/// work cannot dominate the runtime rebalance cost estimate.
inline constexpr bool shouldSampleDynamicTick(uint64_t cycle) noexcept {
    static_assert((kDynamicTickSampleInterval & (kDynamicTickSampleInterval - 1)) == 0);
    return (cycle & (kDynamicTickSampleInterval - 1)) == kDynamicTickSampleInterval - 1;
}

inline uint64_t nextPeriodicCycle(uint64_t cycle, uint64_t period) noexcept {
    if (period == 0) return UINT64_MAX;
    const uint64_t quotient = cycle / period;
    if (quotient >= UINT64_MAX / period) return UINT64_MAX;
    return (quotient + 1) * period;
}

}  // namespace chronon::sender::detail
