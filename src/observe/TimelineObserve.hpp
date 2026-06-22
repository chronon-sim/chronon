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

#include <atomic>
#include <bit>
#include <cstdint>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "FixedString.hpp"
#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
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
uint32_t resolveTrackSlow(ObservationContext* ctx, std::atomic<uint16_t>& cached_source_id,
                          std::atomic<uint32_t>& cached_track_id, TimelineTrackInfo::Kind kind,
                          uint16_t lanes, std::string_view unit) {
    struct Entry {
        uint16_t source_id;
        uint32_t track_id;
    };

    static std::mutex mutex;
    static std::vector<Entry> entries;
    static const std::string track_name = Site::trackName();

    const uint16_t source_id = ctx->sourceId();

    std::lock_guard<std::mutex> lock(mutex);
    for (const Entry& entry : entries) {
        if (entry.source_id == source_id) {
            cached_track_id.store(entry.track_id, std::memory_order_relaxed);
            cached_source_id.store(source_id, std::memory_order_relaxed);
            return entry.track_id;
        }
    }

    const uint32_t track_id = TimelineTrackRegistry::instance().registerTrack(
        {track_name, std::string(unit), source_id, lanes, kind});
    entries.push_back({source_id, track_id});
    cached_track_id.store(track_id, std::memory_order_relaxed);
    cached_source_id.store(source_id, std::memory_order_relaxed);
    return track_id;
}

template <typename Site>
uint32_t resolveTrack(ObservationContext* ctx, TimelineTrackInfo::Kind kind, uint16_t lanes,
                      std::string_view unit = {}) {
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

    return resolveTrackSlow<Site>(ctx, cached_source_id, cached_track_id, kind, lanes, unit);
}

template <typename Unit>
ObservationContext* contextFor(Unit& unit) {
    ObservationContext* ctx = unit.observationContext();
    if (ctx) {
        ctx->setCurrentCycleValue(unit.getObserveCycle());
    }
    return ctx;
}

struct LookaheadCacheSync {
    template <typename CommitFn, typename RollbackFn>
    void sync(ObservationContext* ctx, CommitFn&& on_commit, RollbackFn&& on_rollback) noexcept {
        if (!ctx) {
            return;
        }

        const uint64_t epoch = ctx->lookaheadEpochGeneration();
        if (!initialized_) {
            observed_epoch_ = epoch;
            initialized_ = true;
            return;
        }
        if (epoch == observed_epoch_) {
            return;
        }

        switch (ctx->firstLookaheadTransitionAfter(observed_epoch_)) {
            case ObservationContext::LookaheadTransition::Commit:
                on_commit();
                break;
            case ObservationContext::LookaheadTransition::Rollback:
                on_rollback();
                break;
            case ObservationContext::LookaheadTransition::None:
                break;
        }
        observed_epoch_ = epoch;
    }

private:
    uint64_t observed_epoch_ = 0;
    bool initialized_ = false;
};

template <typename... Items>
void emitTimelineWithItems(ObservationContext* ctx, CategoryMask category, TimelineEventKind kind,
                           uint32_t track_id, uint16_t slot, EventNameRef name, Items&&... items) {
    constexpr size_t item_count = sizeof...(Items);
    constexpr size_t flow_items =
        (static_cast<size_t>(std::is_same_v<std::decay_t<Items>, Flow>) + ... + 0);
    static_assert(flow_items <= 1, "at most one flow() per timeline event");
    static_assert(item_count - flow_items <= MAX_TIMELINE_ARGS,
                  "too many typed timeline args (max 8)");

    if (!ctx || track_id == 0) {
        return;
    }

    if constexpr (item_count == 0) {
        ctx->timelineEvent(category, kind, track_id, slot, name.id, /*payload=*/0, nullptr, 0);
    } else {
        TimelineArgValue args[item_count];
        size_t arg_count = 0;
        uint64_t flow_id = 0;
        (timeline_detail::foldTimelineItem(args, arg_count, flow_id, std::forward<Items>(items)),
         ...);
        ctx->timelineEvent(category, kind, track_id, slot, name.id, flow_id, args, arg_count);
    }
}

inline void emitCounterSample(ObservationContext* ctx, CategoryMask category, uint32_t track_id,
                              int64_t value) {
    if (!ctx || track_id == 0) {
        return;
    }
    ctx->timelineEvent(category, TimelineEventKind::CounterSample, track_id, /*slot=*/0,
                       /*name_id=*/0, std::bit_cast<uint64_t>(value), nullptr, 0);
}

}  // namespace timeline_observe_detail

/**
 * @brief Emit a low-cardinality instant event on the unit's shared "events" track.
 *
 * The event name is a compile-time literal and details are typed annotations:
 * observe::event<"flush">(*this, CAT, arg<"removed">(n)).
 */
template <FixedString Name, typename Unit, typename Cat, typename... Items>
inline void event(Unit& unit, Cat category, Items&&... items) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Track = timeline_observe_detail::EventTrack;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Track>(
        ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    timeline_observe_detail::emitTimelineWithItems(
        ctx, static_cast<CategoryMask>(category), TimelineEventKind::Instant, track_id,
        /*slot=*/0, EventName<Name>::ref(), std::forward<Items>(items)...);
}

/**
 * @brief Emit an instant event on a named track.
 */
template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void instant(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::NamedLaneTrack<Track>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    timeline_observe_detail::emitTimelineWithItems(ctx, static_cast<CategoryMask>(category),
                                                   TimelineEventKind::Instant, track_id,
                                                   /*slot=*/0, name, std::forward<Items>(items)...);
}

/**
 * @brief Begin/end named occupancy spans without declaring a TimelineLane member.
 */
template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void spanBegin(Unit& unit, Cat category, EventNameRef name, Items&&... items) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::NamedLaneTrack<Track>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    timeline_observe_detail::emitTimelineWithItems(ctx, static_cast<CategoryMask>(category),
                                                   TimelineEventKind::SpanBegin, track_id,
                                                   /*slot=*/0, name, std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit, typename Cat, typename... Items>
inline void spanBegin(Unit& unit, uint16_t slot, Cat category, EventNameRef name,
                      Items&&... items) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::NamedLaneTrack<Track, true>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Lane, std::numeric_limits<uint16_t>::max());
    timeline_observe_detail::emitTimelineWithItems(ctx, static_cast<CategoryMask>(category),
                                                   TimelineEventKind::SpanBegin, track_id, slot,
                                                   name, std::forward<Items>(items)...);
}

template <FixedString Track, typename Unit>
inline void spanEnd(Unit& unit) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::NamedLaneTrack<Track>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Lane, /*lanes=*/1);
    if (ctx && track_id != 0) {
        ctx->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id, /*slot=*/0,
                           /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }
}

template <FixedString Track, typename Unit>
inline void spanEnd(Unit& unit, uint16_t slot) {
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::NamedLaneTrack<Track, true>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Lane, std::numeric_limits<uint16_t>::max());
    if (ctx && track_id != 0) {
        ctx->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id, slot,
                           /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }
}

/**
 * @brief Emit one push-model counter sample on a named Perfetto counter track.
 */
template <FixedString Name, typename Unit, typename T>
inline void gauge(Unit& unit, T value, std::string_view unit_name = {}) {
    static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::CounterTrack<Name>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);
    timeline_observe_detail::emitCounterSample(ctx, category::NONE, track_id,
                                               static_cast<int64_t>(value));
}

template <FixedString Name, typename Unit, typename Cat, typename T>
    requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
inline void gauge(Unit& unit, Cat category, T value, std::string_view unit_name = {}) {
    static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
    auto* ctx = timeline_observe_detail::contextFor(unit);
    using Site = timeline_observe_detail::CounterTrack<Name>;
    const uint32_t track_id = timeline_observe_detail::resolveTrack<Site>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);
    timeline_observe_detail::emitCounterSample(ctx, static_cast<CategoryMask>(category), track_id,
                                               static_cast<int64_t>(value));
}

/**
 * @brief Emit used/free capacity samples on sibling counter tracks.
 */
template <FixedString Name, typename Unit, typename Used, typename Capacity>
inline void capacity(Unit& unit, Used used, Capacity total, std::string_view unit_name = {}) {
    static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
    static_assert(std::is_integral_v<std::decay_t<Capacity>>, "total capacity must be integral");

    const int64_t used_value = static_cast<int64_t>(used);
    const int64_t total_value = static_cast<int64_t>(total);
    const int64_t free_value = total_value > used_value ? total_value - used_value : 0;

    auto* ctx = timeline_observe_detail::contextFor(unit);
    using UsedSite =
        timeline_observe_detail::CounterTrack<Name, timeline_observe_detail::TrackSuffix::Used>;
    using FreeSite =
        timeline_observe_detail::CounterTrack<Name, timeline_observe_detail::TrackSuffix::Free>;
    const uint32_t used_track = timeline_observe_detail::resolveTrack<UsedSite>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);
    const uint32_t free_track = timeline_observe_detail::resolveTrack<FreeSite>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);

    timeline_observe_detail::emitCounterSample(ctx, category::NONE, used_track, used_value);
    timeline_observe_detail::emitCounterSample(ctx, category::NONE, free_track, free_value);
}

template <FixedString Name, typename Unit, typename Cat, typename Used, typename Capacity>
    requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
inline void capacity(Unit& unit, Cat category, Used used, Capacity total,
                     std::string_view unit_name = {}) {
    static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
    static_assert(std::is_integral_v<std::decay_t<Capacity>>, "total capacity must be integral");

    const int64_t used_value = static_cast<int64_t>(used);
    const int64_t total_value = static_cast<int64_t>(total);
    const int64_t free_value = total_value > used_value ? total_value - used_value : 0;

    auto* ctx = timeline_observe_detail::contextFor(unit);
    using UsedSite =
        timeline_observe_detail::CounterTrack<Name, timeline_observe_detail::TrackSuffix::Used>;
    using FreeSite =
        timeline_observe_detail::CounterTrack<Name, timeline_observe_detail::TrackSuffix::Free>;
    const uint32_t used_track = timeline_observe_detail::resolveTrack<UsedSite>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);
    const uint32_t free_track = timeline_observe_detail::resolveTrack<FreeSite>(
        ctx, TimelineTrackInfo::Kind::Counter, /*lanes=*/1, unit_name);

    const CategoryMask cat_mask = static_cast<CategoryMask>(category);
    timeline_observe_detail::emitCounterSample(ctx, cat_mask, used_track, used_value);
    timeline_observe_detail::emitCounterSample(ctx, cat_mask, free_track, free_value);
}

template <FixedString Name, typename Unit, typename Port>
inline void portRemaining(Unit& unit, const Port& port, std::string_view unit_name = "sends") {
    gauge<Name>(unit, port.remainingThisCycle(), unit_name);
}

template <FixedString Name, typename Unit, typename Port>
inline void portAvailable(Unit& unit, const Port& port, std::string_view unit_name = "entries") {
    gauge<Name>(unit, port.available(), unit_name);
}

/**
 * @brief Stateful helper for boolean occupancy/stall spans.
 */
class TimelineSpan {
public:
    TimelineSpan(ObservableUnit* owner, std::string_view name, uint16_t lanes = 1)
        : lane_(owner, name, lanes), open_(lanes, false), committed_open_(lanes, false) {}

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
        return slot < open_.size() && open_[slot];
    }

private:
    [[nodiscard]] bool validSlot_(uint16_t slot) const noexcept { return slot < open_.size(); }

    void syncLookaheadCache_() noexcept {
        lookahead_sync_.sync(
            lane_.observationContext(), [this]() { committed_open_ = open_; },
            [this]() { open_ = committed_open_; });
    }

    void updateCommittedOpen_(uint16_t slot) noexcept {
        auto* ctx = lane_.observationContext();
        if (!ctx || !ctx->isLookaheadMode()) {
            committed_open_[slot] = open_[slot];
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
    TimelineGauge(ObservableUnit* owner, std::string_view name, std::string_view unit = {})
        : counter_(owner, name, unit) {}

    template <typename T>
    void sample(T value) noexcept {
        static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
        syncLookaheadCache_();
        const int64_t normalized = static_cast<int64_t>(value);
        if (counter_.sample(normalized)) {
            last_ = normalized;
            has_last_ = true;
            updateCommittedCache_();
        }
    }

    template <typename Cat, typename T>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sample(Cat category, T value) noexcept {
        static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
        syncLookaheadCache_();
        const int64_t normalized = static_cast<int64_t>(value);
        if (counter_.sample(category, normalized)) {
            last_ = normalized;
            has_last_ = true;
            updateCommittedCache_();
        }
    }

    template <typename T>
    void sampleOnChange(T value) noexcept {
        static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
        syncLookaheadCache_();
        const int64_t normalized = static_cast<int64_t>(value);
        if (!has_last_ || normalized != last_) {
            sample(normalized);
        }
    }

    template <typename Cat, typename T>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sampleOnChange(Cat category, T value) noexcept {
        static_assert(std::is_integral_v<std::decay_t<T>>, "gauge value must be integral");
        syncLookaheadCache_();
        const int64_t normalized = static_cast<int64_t>(value);
        if (!has_last_ || normalized != last_) {
            sample(category, normalized);
        }
    }

private:
    void syncLookaheadCache_() noexcept {
        lookahead_sync_.sync(
            counter_.observationContext(),
            [this]() {
                committed_last_ = last_;
                committed_has_last_ = has_last_;
            },
            [this]() {
                last_ = committed_last_;
                has_last_ = committed_has_last_;
            });
    }

    void updateCommittedCache_() noexcept {
        auto* ctx = counter_.observationContext();
        if (!ctx || !ctx->isLookaheadMode()) {
            committed_last_ = last_;
            committed_has_last_ = has_last_;
        }
    }

    TimelineCounter counter_;
    int64_t last_ = 0;
    int64_t committed_last_ = 0;
    bool has_last_ = false;
    bool committed_has_last_ = false;
    timeline_observe_detail::LookaheadCacheSync lookahead_sync_;
};

/**
 * @brief Used/free capacity helper backed by two counter tracks.
 */
class TimelineCapacity {
public:
    TimelineCapacity(ObservableUnit* owner, std::string_view name, std::string_view unit = {})
        : used_(owner, suffixedName_(name, ".used"), unit),
          free_(owner, suffixedName_(name, ".free"), unit) {}

    template <typename Used, typename Capacity>
    void sample(Used used, Capacity total) noexcept {
        static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
        static_assert(std::is_integral_v<std::decay_t<Capacity>>,
                      "total capacity must be integral");
        syncLookaheadCache_();
        const int64_t used_value = static_cast<int64_t>(used);
        const int64_t total_value = static_cast<int64_t>(total);
        const int64_t free_value = total_value > used_value ? total_value - used_value : 0;
        const bool used_sampled = used_.sample(used_value);
        const bool free_sampled = free_.sample(free_value);
        if (used_sampled && free_sampled) {
            last_used_ = used_value;
            last_free_ = free_value;
            has_last_ = true;
            updateCommittedCache_();
        }
    }

    template <typename Cat, typename Used, typename Capacity>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sample(Cat category, Used used, Capacity total) noexcept {
        static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
        static_assert(std::is_integral_v<std::decay_t<Capacity>>,
                      "total capacity must be integral");
        syncLookaheadCache_();
        const int64_t used_value = static_cast<int64_t>(used);
        const int64_t total_value = static_cast<int64_t>(total);
        const int64_t free_value = total_value > used_value ? total_value - used_value : 0;
        const bool used_sampled = used_.sample(category, used_value);
        const bool free_sampled = free_.sample(category, free_value);
        if (used_sampled && free_sampled) {
            last_used_ = used_value;
            last_free_ = free_value;
            has_last_ = true;
            updateCommittedCache_();
        }
    }

    template <typename Used, typename Capacity>
    void sampleOnChange(Used used, Capacity total) noexcept {
        static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
        static_assert(std::is_integral_v<std::decay_t<Capacity>>,
                      "total capacity must be integral");
        syncLookaheadCache_();
        const int64_t used_value = static_cast<int64_t>(used);
        const int64_t total_value = static_cast<int64_t>(total);
        const int64_t free_value = total_value > used_value ? total_value - used_value : 0;
        if (!has_last_ || used_value != last_used_ || free_value != last_free_) {
            sample(used_value, total_value);
        }
    }

    template <typename Cat, typename Used, typename Capacity>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    void sampleOnChange(Cat category, Used used, Capacity total) noexcept {
        static_assert(std::is_integral_v<std::decay_t<Used>>, "used capacity must be integral");
        static_assert(std::is_integral_v<std::decay_t<Capacity>>,
                      "total capacity must be integral");
        syncLookaheadCache_();
        const int64_t used_value = static_cast<int64_t>(used);
        const int64_t total_value = static_cast<int64_t>(total);
        const int64_t free_value = total_value > used_value ? total_value - used_value : 0;
        if (!has_last_ || used_value != last_used_ || free_value != last_free_) {
            sample(category, used_value, total_value);
        }
    }

private:
    static std::string suffixedName_(std::string_view name, std::string_view suffix) {
        std::string out(name);
        out.append(suffix);
        return out;
    }

    void syncLookaheadCache_() noexcept {
        lookahead_sync_.sync(
            used_.observationContext(),
            [this]() {
                committed_last_used_ = last_used_;
                committed_last_free_ = last_free_;
                committed_has_last_ = has_last_;
            },
            [this]() {
                last_used_ = committed_last_used_;
                last_free_ = committed_last_free_;
                has_last_ = committed_has_last_;
            });
    }

    void updateCommittedCache_() noexcept {
        auto* ctx = used_.observationContext();
        if (!ctx || !ctx->isLookaheadMode()) {
            committed_last_used_ = last_used_;
            committed_last_free_ = last_free_;
            committed_has_last_ = has_last_;
        }
    }

    TimelineCounter used_;
    TimelineCounter free_;
    int64_t last_used_ = 0;
    int64_t last_free_ = 0;
    int64_t committed_last_used_ = 0;
    int64_t committed_last_free_ = 0;
    bool has_last_ = false;
    bool committed_has_last_ = false;
    timeline_observe_detail::LookaheadCacheSync lookahead_sync_;
};

}  // namespace chronon::observe
