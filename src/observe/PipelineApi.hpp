// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// PipelineApi.hpp
//
// First-class pipeline trace API. Pipe/stage identity is encoded in template
// parameters, so the producer hot path does not format and then have the
// backend parse strings back into structured pipeline metadata.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "FixedString.hpp"
#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
#include "Types.hpp"

namespace chronon::observe {

namespace pipeline_detail {

template <uint16_t Pipe, uint16_t Stage>
struct NumericPipelineStage {
    static std::string trackName() {
        return "pipe " + std::to_string(Pipe) + " stage " + std::to_string(Stage);
    }
};

template <uint16_t Pipe, FixedString Stage>
struct NamedPipelineStage {
    static std::string trackName() {
        std::string name;
        name.reserve(16 + Stage.size());
        name.append("pipe ");
        name.append(std::to_string(Pipe));
        name.append(" stage ");
        name.append(std::string_view(Stage));
        return name;
    }
};

template <typename Site>
uint32_t resolvePipelineTrack(ObservationContext* ctx) {
    if (!ctx) {
        return 0;
    }
    return timeline_detail::resolveTrackForSource<Site>(
        ctx->sourceId(), TimelineTrackInfo::Kind::Lane, /*lanes=*/1, /*unit=*/{},
        TimelineTrackInfo::Layout::Pipeline);
}

template <typename... Items>
void emitPipelineSliceWithFlags(ObservationContext* ctx, CategoryMask category, uint32_t track_id,
                                uint8_t flags, uint64_t id, Items&&... items) {
    constexpr size_t item_count = sizeof...(Items);
    static_assert(item_count <= MAX_TIMELINE_ARGS, "too many typed pipeline args (max 8)");
    static_assert((!std::is_same_v<std::decay_t<Items>, Flow> && ...),
                  "pipeline event flow id is the item id; do not pass flow()");

    if (!ctx || track_id == 0) {
        return;
    }

    if constexpr (item_count == 0) {
        ctx->timelineEvent(category, TimelineEventKind::PipelineSlice, track_id, /*slot=*/0,
                           /*name_id=*/0, id, nullptr, 0, flags);
    } else {
        TimelineArgValue args[item_count];
        size_t arg_count = 0;
        ((args[arg_count++] = normalizeTimelineArg(std::forward<Items>(items))), ...);
        ctx->timelineEvent(category, TimelineEventKind::PipelineSlice, track_id, /*slot=*/0,
                           /*name_id=*/0, id, args, arg_count, flags);
    }
}

template <typename... Items>
void emitPipelineSlice(ObservationContext* ctx, CategoryMask category, uint32_t track_id,
                       uint64_t id, Items&&... items) {
    emitPipelineSliceWithFlags(ctx, category, track_id, /*flags=*/0, id,
                               std::forward<Items>(items)...);
}

template <typename... Items>
void emitPipelineSliceHex(ObservationContext* ctx, CategoryMask category, uint32_t track_id,
                          uint64_t id, Items&&... items) {
    emitPipelineSliceWithFlags(ctx, category, track_id, TIMELINE_FLAG_NAME_HEX, id,
                               std::forward<Items>(items)...);
}

}  // namespace pipeline_detail

template <uint16_t Pipe>
struct PipelinePipe {
    template <uint16_t Stage, typename Cat, typename... Items>
    void stage(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx || !ctx->shouldTrace(cat_mask)) {
            return;
        }
        using Site = pipeline_detail::NumericPipelineStage<Pipe, Stage>;
        const uint32_t track_id = pipeline_detail::resolvePipelineTrack<Site>(ctx);
        pipeline_detail::emitPipelineSlice(ctx, cat_mask, track_id, id,
                                           std::forward<Items>(items)...);
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void stageName(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx || !ctx->shouldTrace(cat_mask)) {
            return;
        }
        using Site = pipeline_detail::NamedPipelineStage<Pipe, Stage>;
        const uint32_t track_id = pipeline_detail::resolvePipelineTrack<Site>(ctx);
        pipeline_detail::emitPipelineSlice(ctx, cat_mask, track_id, id,
                                           std::forward<Items>(items)...);
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void stageNameHex(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx || !ctx->shouldTrace(cat_mask)) {
            return;
        }
        using Site = pipeline_detail::NamedPipelineStage<Pipe, Stage>;
        const uint32_t track_id = pipeline_detail::resolvePipelineTrack<Site>(ctx);
        pipeline_detail::emitPipelineSliceHex(ctx, cat_mask, track_id, id,
                                              std::forward<Items>(items)...);
    }
};

template <uint16_t Pipe>
constexpr PipelinePipe<Pipe> pipe() noexcept {
    return {};
}

template <uint16_t Pipe, uint16_t Stage, typename Cat, typename... Items>
inline void pipe(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) {
    PipelinePipe<Pipe>{}.template stage<Stage>(ctx, category, id, std::forward<Items>(items)...);
}

template <uint16_t Pipe, FixedString Stage, typename Cat, typename... Items>
inline void pipeStage(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) {
    PipelinePipe<Pipe>{}.template stageName<Stage>(ctx, category, id,
                                                   std::forward<Items>(items)...);
}

template <uint16_t Pipe, FixedString Stage, typename Cat, typename... Items>
inline void pipeStageHex(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) {
    PipelinePipe<Pipe>{}.template stageNameHex<Stage>(ctx, category, id,
                                                      std::forward<Items>(items)...);
}

}  // namespace chronon::observe
