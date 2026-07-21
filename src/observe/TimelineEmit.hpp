// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
#include "Types.hpp"

namespace chronon::observe::timeline_detail {

/// Folds one begin()/instant() pack item into the arg array or the flow id.
template <typename T>
inline void foldTimelineItem(ObservationContext* ctx, TimelineArgValue* args, size_t& arg_count,
                             uint64_t& flow_id, const T& item) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, Flow>) {
        flow_id = item.id;
    } else {
        args[arg_count++] = ctx->normalizeTimelineArgForEmit(item);
    }
}

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
        if (!ctx->shouldEmitTimelineEvent(category, kind)) {
            return false;
        }
        const size_t string_checkpoint = ctx->checkpointPendingTimelineStrings();
        TimelineArgValue args[item_count];
        size_t arg_count = 0;
        uint64_t flow_id = 0;
        (foldTimelineItem(ctx, args, arg_count, flow_id, std::forward<Items>(items)), ...);
        const bool emitted =
            ctx->timelineEvent(category, kind, track_id, slot, name.id, flow_id, args, arg_count);
        if (!emitted || !ctx->isLookaheadMode()) {
            ctx->restorePendingTimelineStrings(string_checkpoint);
        }
        return emitted;
    }
}

}  // namespace chronon::observe::timeline_detail
