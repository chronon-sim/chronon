// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstdint>

namespace chronon::sender::detail {

inline constexpr uint64_t kFloorWaitThreadYieldSpinMask = 0xFF;
inline constexpr uint64_t kDependencyWaitThreadYieldSpinMask = 0xFFF;

[[nodiscard]] constexpr uint64_t dynamicWaitThreadYieldSpinMask(
    bool lookahead_floor_wait) noexcept {
    return lookahead_floor_wait ? kFloorWaitThreadYieldSpinMask
                                : kDependencyWaitThreadYieldSpinMask;
}

[[nodiscard]] constexpr bool shouldYieldDynamicWaitThread(uint64_t spin_iteration,
                                                          uint64_t spin_mask) noexcept {
    return (spin_iteration & spin_mask) == spin_mask;
}

}  // namespace chronon::sender::detail
