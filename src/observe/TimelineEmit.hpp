// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
#include "Types.hpp"

namespace chronon::observe::timeline_detail {

template <typename... Items>
bool emitEventWithItems(ObservationContext* ctx, CategoryMask category, TimelineEventKind kind,
                        uint32_t track_id, uint16_t slot, EventNameRef name,
                        Items&&... items) noexcept {
    constexpr size_t item_count = sizeof...(Items);
    constexpr size_t flow_items =
        (static_cast<size_t>(std::is_same_v<std::decay_t<Items>, Flow>) + ... + 0);
    static_assert(flow_items <= 1, "at most one flow() per timeline event");
    static_assert(item_count - flow_items <= MAX_TIMELINE_ARGS,
                  "too many typed timeline args (max 8)");

    if (!ctx || track_id == 0) {
        return false;
    }

    if constexpr (item_count == 0) {
        return ctx->timelineEvent(category, kind, track_id, slot, name.id, /*payload=*/0, nullptr,
                                  0);
    } else {
        TimelineArgValue args[item_count];
        size_t arg_count = 0;
        uint64_t flow_id = 0;
        (foldTimelineItem(args, arg_count, flow_id, std::forward<Items>(items)), ...);
        return ctx->timelineEvent(category, kind, track_id, slot, name.id, flow_id, args,
                                  arg_count);
    }
}

inline bool emitCounterSample(ObservationContext* ctx, CategoryMask category, uint32_t track_id,
                              int64_t value) noexcept {
    if (!ctx || track_id == 0) {
        return false;
    }
    return ctx->timelineEvent(category, TimelineEventKind::CounterSample, track_id, /*slot=*/0,
                              /*name_id=*/0, std::bit_cast<uint64_t>(value), nullptr, 0);
}

}  // namespace chronon::observe::timeline_detail
