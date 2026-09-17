// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>

namespace chronon::sender::detail {

/// Shared proof for topology planning and installed queue adapters. A zero
/// result requires atomic cluster execution; it never changes modeled delay,
/// capacity, rate, or the cycle at which receiver credit becomes visible.
inline size_t connectionHeadroom(size_t storage, size_t capacity, std::optional<size_t> rate,
                                 uint32_t delay, bool cycle_strict) noexcept {
    if (delay == 1 && capacity == 1 && rate == 1) return 1;
    if (!rate || *rate == 0) return 0;
    const size_t buffered_cycles = storage / *rate;
    // The receiver can only drain due entries; delay cycles remain in flight.
    if (buffered_cycles < delay) return 0;
    if (!cycle_strict) return buffered_cycles - delay + 1;
    // A pop at C becomes credit at C+1. Physical storage alone cannot prove
    // that the producer has observed all receiver credit through C-1, so a
    // bounded edge also caps the reverse progress dependency at delay 1.
    if (buffered_cycles == delay) return 0;
    const size_t headroom = buffered_cycles - delay;
    return capacity == std::numeric_limits<size_t>::max() ? headroom
                                                          : std::min<size_t>(headroom, 2);
}

}  // namespace chronon::sender::detail
