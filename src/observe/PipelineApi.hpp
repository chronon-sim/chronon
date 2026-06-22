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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

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
uint32_t resolvePipelineTrackSlow(uint16_t source_id, std::atomic<uint16_t>& cached_source_id,
                                  std::atomic<uint32_t>& cached_track_id) {
    struct Entry {
        uint16_t source_id;
        uint32_t track_id;
    };

    static std::mutex mutex;
    static std::vector<Entry> entries;
    static const std::string track_name = Site::trackName();

    std::lock_guard<std::mutex> lock(mutex);
    for (const Entry& entry : entries) {
        if (entry.source_id == source_id) {
            cached_track_id.store(entry.track_id, std::memory_order_relaxed);
            cached_source_id.store(source_id, std::memory_order_relaxed);
            return entry.track_id;
        }
    }

    const uint32_t track_id = TimelineTrackRegistry::instance().registerTrack(
        {track_name, /*unit=*/{}, source_id, /*lanes=*/1, TimelineTrackInfo::Kind::Lane,
         TimelineTrackInfo::Layout::Pipeline});
    entries.push_back({source_id, track_id});
    cached_track_id.store(track_id, std::memory_order_relaxed);
    cached_source_id.store(source_id, std::memory_order_relaxed);
    return track_id;
}

template <typename Site>
uint32_t resolvePipelineTrack(ObservationContext* ctx) {
    if (!ctx) {
        return 0;
    }

    static std::atomic<uint16_t> cached_source_id{0};
    static std::atomic<uint32_t> cached_track_id{0};

    const uint16_t source_id = ctx->sourceId();
    const uint32_t track_id = cached_track_id.load(std::memory_order_relaxed);
    if (OBSERVE_LIKELY(track_id != 0 &&
                       cached_source_id.load(std::memory_order_relaxed) == source_id)) {
        return track_id;
    }

    return resolvePipelineTrackSlow<Site>(source_id, cached_source_id, cached_track_id);
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
