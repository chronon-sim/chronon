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
// existing TimelineLane / TimelineCounter wire format.

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

enum class TrackSuffix : uint8_t { None, Used, Free };

struct EventTrack {
    static std::string trackName() { return "events"; }
};

template <FixedString Name, bool Slotted = false>
struct NamedLaneTrack {
    static std::string trackName() { return std::string(std::string_view(Name)); }
};

template <FixedString Name, TrackSuffix Suffix = TrackSuffix::None>
struct CounterTrack {
    static std::string trackName() {
        std::string track_name{std::string_view(Name)};
        if constexpr (Suffix == TrackSuffix::Used) {
            track_name.append(".used");
        } else if constexpr (Suffix == TrackSuffix::Free) {
            track_name.append(".free");
        }
        return track_name;
    }
};

template <typename Site>
uint32_t resolveTrack(ObservationContext* ctx, TimelineTrackInfo::Kind kind, uint16_t lanes,
                      std::string_view unit = {}) {
    if (!ctx || !ctx->timelineProducerEnabled()) {
        return 0;
    }
    return timeline_detail::resolveTrackForSource<Site>(ctx->sourceId(), kind, lanes, unit);
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

template <typename T>
class LookaheadValueCache {
public:
    LookaheadValueCache() noexcept
        : lookahead_sync_(this, &LookaheadValueCache::applyLookaheadTransition_) {}

    LookaheadValueCache(const LookaheadValueCache&) = delete;
    LookaheadValueCache& operator=(const LookaheadValueCache&) = delete;

    void sync(ObservationContext* ctx) noexcept { lookahead_sync_.sync(ctx); }

    [[nodiscard]] bool differsFromLast(ObservationContext* ctx, const T& value) noexcept {
        sync(ctx);
        return !has_last_ || !(value == last_);
    }

    void recordSample(ObservationContext* ctx, const T& value) noexcept {
        last_ = value;
        has_last_ = true;
        if (!ctx || !ctx->isLookaheadMode()) {
            committed_last_ = last_;
            committed_has_last_ = has_last_;
        }
    }

private:
    static void applyLookaheadTransition_(
        void* owner, ObservationContext::LookaheadTransition transition) noexcept {
        auto* self = static_cast<LookaheadValueCache*>(owner);
        switch (transition) {
            case ObservationContext::LookaheadTransition::Commit:
                self->committed_last_ = self->last_;
                self->committed_has_last_ = self->has_last_;
                break;
            case ObservationContext::LookaheadTransition::Rollback:
                self->last_ = self->committed_last_;
                self->has_last_ = self->committed_has_last_;
                break;
            case ObservationContext::LookaheadTransition::None:
                break;
        }
    }

    T last_{};
    T committed_last_{};
    bool has_last_ = false;
    bool committed_has_last_ = false;
    LookaheadCacheSync lookahead_sync_;
};

template <typename T>
int64_t normalizeIntegral(T value) noexcept {
    static_assert(std::is_integral_v<std::decay_t<T>>, "timeline value must be integral");
    return static_cast<int64_t>(value);
}

struct CapacityValues {
    int64_t used;
    int64_t free;

    bool operator==(const CapacityValues&) const = default;
};

template <typename Used, typename Capacity>
CapacityValues normalizeCapacity(Used used, Capacity total) noexcept {
    static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
    static_assert(std::is_integral_v<std::decay_t<Capacity>>, "total capacity must be integral");

    const int64_t used_value = static_cast<int64_t>(used);
    const int64_t total_value = static_cast<int64_t>(total);
    return {.used = used_value, .free = total_value > used_value ? total_value - used_value : 0};
}

template <FixedString Name, TrackSuffix Suffix = TrackSuffix::None, typename T>
void emitNamedCounterSample(ObservationContext* ctx, CategoryMask category, T value,
                            std::string_view unit_name) {
    using Site = CounterTrack<Name, Suffix>;
    const uint32_t track_id =
        resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);
    timeline_detail::emitCounterSample(ctx, category, track_id, normalizeIntegral(value));
}

template <FixedString Name, typename Used, typename Capacity>
void emitCapacitySamples(ObservationContext* ctx, CategoryMask category, Used used, Capacity total,
                         std::string_view unit_name) {
    const CapacityValues values = normalizeCapacity(used, total);
    emitNamedCounterSample<Name, TrackSuffix::Used>(ctx, category, values.used, unit_name);
    emitNamedCounterSample<Name, TrackSuffix::Free>(ctx, category, values.free, unit_name);
}

template <FixedString Name, typename Unit, typename Cat, typename... Items>
inline void emitUnitEvent(Unit& unit, Cat category, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Track = EventTrack;
    const uint32_t track_id = resolveTrack<Track>(ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    timeline_detail::emitEventWithItems(
        ctx, static_cast<CategoryMask>(category), TimelineEventKind::Instant, track_id,
        /*slot=*/0, EventName<Name>::ref(), std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void emitUnitInstant(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    timeline_detail::emitEventWithItems(ctx, static_cast<CategoryMask>(category),
                                        TimelineEventKind::Instant, track_id, /*slot=*/0, name,
                                        std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void emitUnitSpanBegin(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
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
    const uint32_t track_id = resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Lane,
                                                 std::numeric_limits<uint16_t>::max());
    timeline_detail::emitEventWithItems(ctx, static_cast<CategoryMask>(category),
                                        TimelineEventKind::SpanBegin, track_id, slot, name,
                                        std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit>
inline void emitUnitSpanEnd(Unit& unit) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    using Site = NamedLaneTrack<Track>;
    const uint32_t track_id = resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
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
    const uint32_t track_id = resolveTrack<Site>(ctx, TimelineTrackInfo::Kind::Lane,
                                                 std::numeric_limits<uint16_t>::max());
    if (ctx && track_id != 0) {
        ctx->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id, slot,
                           /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }
}

template <FixedString Name, typename Unit, typename T>
inline void emitUnitGauge(Unit& unit, CategoryMask category, T value, std::string_view unit_name) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    emitNamedCounterSample<Name>(ctx, category, value, unit_name);
}

template <FixedString Name, typename Unit, typename Used, typename Capacity>
inline void emitUnitCapacity(Unit& unit, CategoryMask category, Used used, Capacity total,
                             std::string_view unit_name) {
    auto* ctx = contextFor(unit);
    if (!ctx) return;
    emitCapacitySamples<Name>(ctx, category, used, total, unit_name);
}

}  // namespace timeline_observe_detail

/**
 * @brief Emit a low-cardinality instant event on the unit's shared "events" track.
 *
 * The event name is a compile-time literal and details are typed annotations:
 * observe::event<"flush">(*this, CAT, arg<"removed">(n)).
 */
template <FixedString Name, typename Unit, typename Cat, typename... Items>
[[deprecated(
    "free observe::event() is deprecated; use ObservableUnit::event() or EventCounter::mark()")]]
inline void event(Unit& unit, Cat category, Items&&... items) {
    timeline_observe_detail::emitUnitEvent<Name>(unit, category, std::forward<Items>(items)...);
}

/**
 * @brief Emit an instant event on a named track.
 */
template <FixedString Track, typename Unit, typename Cat, typename... Items>
[[deprecated(
    "free observe::instant() is deprecated; use ObservableUnit::instant() or a TimelineLane "
    "member")]]
inline void instant(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    timeline_observe_detail::emitUnitInstant<Track>(unit, category, name,
                                                    std::forward<Items>(items)...);
}

/**
 * @brief Begin/end named occupancy spans without declaring a TimelineLane member.
 */
template <FixedString Track, typename Unit, typename Cat, typename... Items>
[[deprecated(
    "free observe::spanBegin() is deprecated; use ObservableUnit::spanBegin() or a TimelineSpan "
    "member")]]
inline void spanBegin(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    timeline_observe_detail::emitUnitSpanBegin<Track>(unit, category, name,
                                                      std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
[[deprecated(
    "free observe::spanBegin() is deprecated; use ObservableUnit::spanBegin() or a TimelineSpan "
    "member")]]
inline void spanBegin(Unit& unit, uint16_t slot, Cat category, EventNameRef name,
                      Items&&... items) {
    timeline_observe_detail::emitUnitSpanBegin<Track>(unit, slot, category, name,
                                                      std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit>
[[deprecated(
    "free observe::spanEnd() is deprecated; use ObservableUnit::spanEnd() or a TimelineSpan "
    "member")]]
inline void spanEnd(Unit& unit) {
    timeline_observe_detail::emitUnitSpanEnd<Track>(unit);
}

template <FixedString Track, typename Unit>
[[deprecated(
    "free observe::spanEnd() is deprecated; use ObservableUnit::spanEnd() or a TimelineSpan "
    "member")]]
inline void spanEnd(Unit& unit, uint16_t slot) {
    timeline_observe_detail::emitUnitSpanEnd<Track>(unit, slot);
}

/**
 * @brief Emit one push-model counter sample on a named Perfetto counter track.
 */
template <FixedString Name, typename Unit, typename T>
[[deprecated(
    "free observe::gauge() is deprecated; use EventCounter for aggregate metrics or "
    "ObservableUnit::gauge() during migration")]]
inline void gauge(Unit& unit, T value, std::string_view unit_name = {}) {
    timeline_observe_detail::emitUnitGauge<Name>(unit, category::NONE, value, unit_name);
}

template <FixedString Name, typename Unit, typename Cat, typename T>
    requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
[[deprecated(
    "free observe::gauge() is deprecated; use EventCounter for aggregate metrics or "
    "ObservableUnit::gauge() during migration")]]
inline void gauge(Unit& unit, Cat category, T value, std::string_view unit_name = {}) {
    timeline_observe_detail::emitUnitGauge<Name>(unit, static_cast<CategoryMask>(category), value,
                                                 unit_name);
}

/**
 * @brief Emit used/free capacity samples on sibling counter tracks.
 */
template <FixedString Name, typename Unit, typename Used, typename Capacity>
[[deprecated(
    "free observe::capacity() is deprecated; use EventCounter for aggregate metrics or "
    "ObservableUnit::capacity() during migration")]]
inline void capacity(Unit& unit, Used used, Capacity total, std::string_view unit_name = {}) {
    timeline_observe_detail::emitUnitCapacity<Name>(unit, category::NONE, used, total, unit_name);
}

template <FixedString Name, typename Unit, typename Cat, typename Used, typename Capacity>
    requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
[[deprecated(
    "free observe::capacity() is deprecated; use EventCounter for aggregate metrics or "
    "ObservableUnit::capacity() during migration")]]
inline void capacity(Unit& unit, Cat category, Used used, Capacity total,
                     std::string_view unit_name = {}) {
    timeline_observe_detail::emitUnitCapacity<Name>(unit, static_cast<CategoryMask>(category), used,
                                                    total, unit_name);
}

template <FixedString Name, typename Unit, typename Port>
[[deprecated(
    "observe::portRemaining() is deprecated; sample the metric through EventCounter or a "
    "unit-level helper")]]
inline void portRemaining(Unit& unit, const Port& port, std::string_view unit_name = "sends") {
    timeline_observe_detail::emitUnitGauge<Name>(unit, category::NONE, port.remainingThisCycle(),
                                                 unit_name);
}

template <FixedString Name, typename Unit, typename Port>
[[deprecated(
    "observe::portAvailable() is deprecated; sample the metric through EventCounter or a "
    "unit-level helper")]]
inline void portAvailable(Unit& unit, const Port& port, std::string_view unit_name = "entries") {
    timeline_observe_detail::emitUnitGauge<Name>(unit, category::NONE, port.available(), unit_name);
}

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

/**
 * @brief Push-model gauge with optional sample-on-change filtering.
 */
class TimelineGauge {
public:
    [[deprecated(
        "TimelineGauge is deprecated for user code; use EventCounter::add() for aggregate metrics "
        "or EventCounter::mark() for timeline-visible events")]]
    TimelineGauge(ObservableUnit* owner, std::string_view name, std::string_view unit = {})
        : counter_(timeline_detail::InternalTimelineCounterTag{}, owner, name, unit) {}

    template <typename T>
    void sample(T value) noexcept {
        sample_(category::NONE, value);
    }

    template <typename Cat, typename T>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sample(Cat category, T value) noexcept {
        sample_(static_cast<CategoryMask>(category), value);
    }

    template <typename T>
    void sampleOnChange(T value) noexcept {
        sampleOnChange_(category::NONE, value);
    }

    template <typename Cat, typename T>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sampleOnChange(Cat category, T value) noexcept {
        sampleOnChange_(static_cast<CategoryMask>(category), value);
    }

private:
    template <typename T>
    void sample_(CategoryMask category, T value) noexcept {
        const int64_t normalized = timeline_observe_detail::normalizeIntegral(value);
        auto* ctx = counter_.observationContext();
        cache_.sync(ctx);
        if (counter_.sample(category, normalized)) {
            cache_.recordSample(ctx, normalized);
        }
    }

    template <typename T>
    void sampleOnChange_(CategoryMask category, T value) noexcept {
        const int64_t normalized = timeline_observe_detail::normalizeIntegral(value);
        if (cache_.differsFromLast(counter_.observationContext(), normalized)) {
            sample_(category, normalized);
        }
    }

    TimelineCounter counter_;
    timeline_observe_detail::LookaheadValueCache<int64_t> cache_;
};

/**
 * @brief Used/free capacity helper backed by two counter tracks.
 */
class TimelineCapacity {
public:
    [[deprecated(
        "TimelineCapacity is deprecated for user code; use EventCounter-based metrics instead of "
        "push-model timeline counters")]]
    TimelineCapacity(ObservableUnit* owner, std::string_view name, std::string_view unit = {})
        : used_(timeline_detail::InternalTimelineCounterTag{}, owner, suffixedName_(name, ".used"),
                unit),
          free_(timeline_detail::InternalTimelineCounterTag{}, owner, suffixedName_(name, ".free"),
                unit) {}

    template <typename Used, typename Capacity>
    void sample(Used used, Capacity total) noexcept {
        sample_(category::NONE, used, total);
    }

    template <typename Cat, typename Used, typename Capacity>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sample(Cat category, Used used, Capacity total) noexcept {
        sample_(static_cast<CategoryMask>(category), used, total);
    }

    template <typename Used, typename Capacity>
    void sampleOnChange(Used used, Capacity total) noexcept {
        sampleOnChange_(category::NONE, used, total);
    }

    template <typename Cat, typename Used, typename Capacity>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sampleOnChange(Cat category, Used used, Capacity total) noexcept {
        sampleOnChange_(static_cast<CategoryMask>(category), used, total);
    }

private:
    template <typename Used, typename Capacity>
    void sample_(CategoryMask category, Used used, Capacity total) noexcept {
        const auto values = timeline_observe_detail::normalizeCapacity(used, total);
        sampleValues_(category, values);
    }

    void sampleValues_(CategoryMask category,
                       timeline_observe_detail::CapacityValues values) noexcept {
        auto* ctx = used_.observationContext();
        cache_.sync(ctx);
        const bool used_sampled = used_.sample(category, values.used);
        const bool free_sampled = free_.sample(category, values.free);
        if (used_sampled && free_sampled) {
            cache_.recordSample(ctx, values);
        }
    }

    template <typename Used, typename Capacity>
    void sampleOnChange_(CategoryMask category, Used used, Capacity total) noexcept {
        const auto values = timeline_observe_detail::normalizeCapacity(used, total);
        if (cache_.differsFromLast(used_.observationContext(), values)) {
            sampleValues_(category, values);
        }
    }

    static std::string suffixedName_(std::string_view name, std::string_view suffix) {
        std::string out(name);
        out.append(suffix);
        return out;
    }

    TimelineCounter used_;
    TimelineCounter free_;
    timeline_observe_detail::LookaheadValueCache<timeline_observe_detail::CapacityValues> cache_;
};

}  // namespace chronon::observe
