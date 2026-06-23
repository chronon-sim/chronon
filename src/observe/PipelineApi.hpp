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

#include <array>
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

struct RuntimePipelineStageEntry {
    uint16_t source_id;
    uint16_t pipe;
    uint32_t track_id;
};

inline constexpr size_t MAX_RUNTIME_PIPELINE_SLOTS = 16;

template <FixedString Stage>
uint32_t resolveRuntimePipelineTrackSlow(uint16_t source_id, uint16_t pipe,
                                         std::atomic<uint64_t>* cached_entry) {
    static std::mutex mutex;
    static std::vector<RuntimePipelineStageEntry> entries;

    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& entry : entries) {
        if (entry.source_id == source_id && entry.pipe == pipe) {
            if (cached_entry) {
                cached_entry->store((static_cast<uint64_t>(source_id) << 32) | entry.track_id,
                                    std::memory_order_relaxed);
            }
            return entry.track_id;
        }
    }

    std::string name;
    name.reserve(16 + Stage.size());
    name.append("pipe ");
    name.append(std::to_string(pipe));
    name.append(" stage ");
    name.append(std::string_view(Stage));

    const uint32_t track_id = TimelineTrackRegistry::instance().registerTrack(
        {name, /*unit=*/{}, source_id, /*lanes=*/1, TimelineTrackInfo::Kind::Lane,
         TimelineTrackInfo::Layout::Pipeline});
    entries.push_back({source_id, pipe, track_id});
    if (cached_entry) {
        cached_entry->store((static_cast<uint64_t>(source_id) << 32) | track_id,
                            std::memory_order_relaxed);
    }
    return track_id;
}

template <FixedString Stage>
uint32_t resolveRuntimePipelineTrack(ObservationContext* ctx, uint16_t pipe) {
    if (!ctx) {
        return 0;
    }

    const uint16_t source_id = ctx->sourceId();
    if (pipe >= MAX_RUNTIME_PIPELINE_SLOTS) {
        return resolveRuntimePipelineTrackSlow<Stage>(source_id, pipe, nullptr);
    }

    static std::array<std::atomic<uint64_t>, MAX_RUNTIME_PIPELINE_SLOTS> cached_entries{};
    std::atomic<uint64_t>& cached_entry = cached_entries[pipe];
    const uint64_t entry = cached_entry.load(std::memory_order_relaxed);
    const uint32_t track_id = static_cast<uint32_t>(entry);
    if (track_id != 0 && static_cast<uint16_t>(entry >> 32) == source_id) {
        return track_id;
    }

    return resolveRuntimePipelineTrackSlow<Stage>(source_id, pipe, &cached_entry);
}

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

template <FixedString Stage, typename Cat, typename... Items>
inline void emitRuntimePipelineSlice(ObservationContext* ctx, uint16_t pipe, Cat category,
                                     uint8_t flags, uint64_t id, Items&&... items) {
    const CategoryMask cat_mask = static_cast<CategoryMask>(category);
    if (!ctx || !ctx->shouldTrace(cat_mask)) {
        return;
    }
    const uint32_t track_id = resolveRuntimePipelineTrack<Stage>(ctx, pipe);
    emitPipelineSliceWithFlags(ctx, cat_mask, track_id, flags, id, std::forward<Items>(items)...);
}

}  // namespace pipeline_detail

template <uint16_t Pipe>
struct PipelinePipe {
    template <uint16_t Stage, typename Cat, typename... Items>
    void stage(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        using Site = pipeline_detail::NumericPipelineStage<Pipe, Stage>;
        stageWithFlags_<Site>(ctx, category, /*flags=*/0, id, std::forward<Items>(items)...);
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void stageName(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        using Site = pipeline_detail::NamedPipelineStage<Pipe, Stage>;
        stageWithFlags_<Site>(ctx, category, /*flags=*/0, id, std::forward<Items>(items)...);
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void stageNameHex(ObservationContext* ctx, Cat category, uint64_t id, Items&&... items) const {
        using Site = pipeline_detail::NamedPipelineStage<Pipe, Stage>;
        stageWithFlags_<Site>(ctx, category, TIMELINE_FLAG_NAME_HEX, id,
                              std::forward<Items>(items)...);
    }

private:
    template <typename Site, typename Cat, typename... Items>
    void stageWithFlags_(ObservationContext* ctx, Cat category, uint8_t flags, uint64_t id,
                         Items&&... items) const {
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx || !ctx->shouldTrace(cat_mask)) {
            return;
        }
        const uint32_t track_id = pipeline_detail::resolvePipelineTrack<Site>(ctx);
        pipeline_detail::emitPipelineSliceWithFlags(ctx, cat_mask, track_id, flags, id,
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

template <FixedString Stage, typename Cat, typename... Items>
inline void pipeStage(ObservationContext* ctx, uint16_t pipe, Cat category, uint64_t id,
                      Items&&... items) {
    pipeline_detail::emitRuntimePipelineSlice<Stage>(ctx, pipe, category, /*flags=*/0, id,
                                                     std::forward<Items>(items)...);
}

template <FixedString Stage, typename Cat, typename... Items>
inline void pipeStageHex(ObservationContext* ctx, uint16_t pipe, Cat category, uint64_t id,
                         Items&&... items) {
    pipeline_detail::emitRuntimePipelineSlice<Stage>(ctx, pipe, category, TIMELINE_FLAG_NAME_HEX,
                                                     id, std::forward<Items>(items)...);
}

}  // namespace chronon::observe
