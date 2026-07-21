// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TimelineObserve.hpp
//
// Convenience layer for first-class Perfetto timeline observations. This keeps
// call sites close to model-level observe::pipeline wrappers while reusing the
// existing TimelineLane wire format.

#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "FixedString.hpp"
#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
#include "TimelineEmit.hpp"
#include "TimelineTrack.hpp"
#include "Types.hpp"

namespace chronon::observe {

namespace timeline_observe_detail {

struct EventTrack {
    static std::string trackName() { return "events"; }
};

template <FixedString Name, bool Slotted = false>
struct NamedLaneTrack {
    static std::string trackName() { return std::string(std::string_view(Name)); }
};

template <typename Site>
uint32_t resolveTrack(ObservationContext* ctx, uint16_t lanes) {
    if (!ctx || !ctx->timelineProducerEnabled()) {
        return 0;
    }
    return timeline_detail::resolveTrackForSource<Site>(ctx->sourceId(), lanes);
}

template <typename Unit>
ObservationContext* contextFor(Unit& unit) {
    ObservationContext* ctx = unit.observationContext();
    if (!ctx || !ctx->timelineProducerEnabled()) return nullptr;
    ctx->setCurrentCycleValue(unit.getObserveCycle());
    return ctx;
}

class LookaheadCacheSync {
public:
    using Transition = ObservationContext::LookaheadTransition;
    using ApplyFn = ObservationContext::LookaheadSyncNode::ApplyFn;

    LookaheadCacheSync(void* owner, ApplyFn apply) noexcept {
        node_.owner = owner;
        node_.apply = apply;
    }

    ~LookaheadCacheSync() noexcept {
        if (node_.context) {
            node_.context->unregisterLookaheadSyncNode(node_);
        }
    }

    LookaheadCacheSync(const LookaheadCacheSync&) = delete;
    LookaheadCacheSync& operator=(const LookaheadCacheSync&) = delete;
    LookaheadCacheSync(LookaheadCacheSync&&) = delete;
    LookaheadCacheSync& operator=(LookaheadCacheSync&&) = delete;

    void sync(ObservationContext* ctx) noexcept {
        if (!ctx || !ctx->isLookaheadMode()) {
            if (node_.context) {
                node_.context->unregisterLookaheadSyncNode(node_);
            }
            return;
        }

        if (node_.context != ctx) {
            ctx->registerLookaheadSyncNode(node_);
        }
    }

private:
    ObservationContext::LookaheadSyncNode node_;
};

template <FixedString Name, typename Unit, typename Cat, typename... Items>
inline void emitUnitEvent(Unit& unit, Cat category, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Track = EventTrack;
    const uint32_t track_id = resolveTrack<Track>(ctx, /*lanes=*/1);
    timeline_detail::emitEventWithItems(
        ctx, static_cast<CategoryMask>(category), TimelineEventKind::Instant, track_id,
        /*slot=*/0, EventName<Name>::ref(), std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void emitUnitInstant(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, /*lanes=*/1);
    timeline_detail::emitEventWithItems(ctx, static_cast<CategoryMask>(category),
                                        TimelineEventKind::Instant, track_id, /*slot=*/0, name,
                                        std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void emitUnitSpanBegin(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, /*lanes=*/1);
    timeline_detail::emitEventWithItems(ctx, static_cast<CategoryMask>(category),
                                        TimelineEventKind::SpanBegin, track_id, /*slot=*/0, name,
                                        std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void emitUnitSpanBegin(Unit& unit, uint16_t slot, Cat category, EventNameRef name,
                              Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track, true>;
    const uint32_t track_id = resolveTrack<Site>(ctx, std::numeric_limits<uint16_t>::max());
    timeline_detail::emitEventWithItems(ctx, static_cast<CategoryMask>(category),
                                        TimelineEventKind::SpanBegin, track_id, slot, name,
                                        std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit>
inline void emitUnitSpanEnd(Unit& unit) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, /*lanes=*/1);
    if (ctx && track_id != 0) {
        ctx->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id, /*slot=*/0,
                           /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }
}

template <FixedString Track, typename Unit>
inline void emitUnitSpanEnd(Unit& unit, uint16_t slot) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track, true>;
    const uint32_t track_id = resolveTrack<Site>(ctx, std::numeric_limits<uint16_t>::max());
    if (ctx && track_id != 0) {
        ctx->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id, slot,
                           /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }
}

}  // namespace timeline_observe_detail

/**
 * @brief Stateful helper for boolean occupancy/stall spans.
 */
class TimelineSpan {
public:
    TimelineSpan(ObservableUnit* owner, std::string_view name, uint16_t lanes = 1)
        : lane_(owner, name, lanes),
          open_(lanes, false),
          committed_open_(lanes, false),
          lookahead_sync_(this, &TimelineSpan::applyLookaheadTransition_) {}

    template <typename Cat, typename... Items>
    bool begin(uint16_t slot, Cat category, EventNameRef name, Items&&... items) noexcept {
        syncLookaheadCache_();
        if (!validSlot_(slot)) {
            return false;
        }
        if (lane_.begin(slot, category, name, std::forward<Items>(items)...)) {
            open_[slot] = true;
            updateCommittedOpen_(slot);
            return true;
        }
        return false;
    }

    template <typename Cat, typename... Items>
    bool begin(Cat category, EventNameRef name, Items&&... items) noexcept {
        return begin(/*slot=*/0, category, name, std::forward<Items>(items)...);
    }

    bool end(uint16_t slot = 0) noexcept {
        syncLookaheadCache_();
        if (!validSlot_(slot) || !open_[slot]) {
            return false;
        }
        if (lane_.end(slot)) {
            open_[slot] = false;
            updateCommittedOpen_(slot);
            return true;
        }
        return false;
    }

    template <typename Cat, typename... Items>
    bool instant(uint16_t slot, Cat category, EventNameRef name, Items&&... items) noexcept {
        if (!validSlot_(slot)) {
            return false;
        }
        return lane_.instant(slot, category, name, std::forward<Items>(items)...);
    }

    template <typename Cat, typename... Items>
    void update(bool active, Cat category, EventNameRef name, Items&&... items) noexcept {
        update(/*slot=*/0, active, category, name, std::forward<Items>(items)...);
    }

    template <typename Cat, typename... Items>
    void update(uint16_t slot, bool active, Cat category, EventNameRef name,
                Items&&... items) noexcept {
        syncLookaheadCache_();
        if (!validSlot_(slot)) {
            return;
        }
        if (active) {
            if (!open_[slot]) {
                begin(slot, category, name, std::forward<Items>(items)...);
            }
        } else {
            end(slot);
        }
    }

    [[nodiscard]] bool isOpen(uint16_t slot = 0) const noexcept {
        const_cast<TimelineSpan*>(this)->syncLookaheadCache_();
        return slot < open_.size() && open_[slot];
    }

private:
    [[nodiscard]] bool validSlot_(uint16_t slot) const noexcept { return slot < open_.size(); }

    void syncLookaheadCache_() noexcept { lookahead_sync_.sync(lane_.observationContext()); }

    void updateCommittedOpen_(uint16_t slot) noexcept {
        auto* ctx = lane_.observationContext();
        if (!ctx || !ctx->isLookaheadMode()) {
            committed_open_[slot] = open_[slot];
        }
    }

    static void applyLookaheadTransition_(
        void* owner, ObservationContext::LookaheadTransition transition) noexcept {
        auto* self = static_cast<TimelineSpan*>(owner);
        switch (transition) {
            case ObservationContext::LookaheadTransition::Commit:
                self->committed_open_ = self->open_;
                break;
            case ObservationContext::LookaheadTransition::Rollback:
                self->open_ = self->committed_open_;
                break;
            case ObservationContext::LookaheadTransition::None:
                break;
        }
    }

    TimelineLane lane_;
    std::vector<bool> open_;
    std::vector<bool> committed_open_;
    timeline_observe_detail::LookaheadCacheSync lookahead_sync_;
};

}  // namespace chronon::observe
