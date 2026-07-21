// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TimelineApi.hpp
//
// Wire records and producer-side vocabulary for first-class timeline events
// (occupancy spans and lane instants): low-cardinality
// event names ("miss"_ev), typed annotation args (arg<"addr">(v)), and flow
// ids (flow(uid)). See TimelineTrack.hpp for the declarative unit members.

#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "FixedString.hpp"
#include "Types.hpp"

namespace chronon::observe {

// ---------------------------------------------------------------------------
// Wire format (queue protocol)
// ---------------------------------------------------------------------------

enum class TimelineEventKind : uint8_t {
    Instant = 0,
    SpanBegin = 1,
    SpanEnd = 2,
    PipelineSlice = 3,  ///< One-cycle pipeline occupancy slice; payload is item id.
};

/**
 * @brief Fixed-size queue record for timeline events.
 *
 * Layout in queue: [RecordHeader][TimelineRecord][arg0]...[argN-1], where each
 * arg is TIMELINE_ARG_SIZE bytes (see packTimelineArg). `cycle` MUST stay the
 * first uint64_t: ReorderBuffer::extractCycle and the drain loop's watermark
 * peek read it positionally.
 */
/// Sentinel for TimelineRecord::category_bit: no user category.
constexpr uint8_t TIMELINE_NO_CATEGORY = 0xFF;
constexpr uint8_t TIMELINE_FLAG_NAME_HEX = 1u << 0;

struct TimelineRecord {
    uint64_t cycle;
    /// Flow id for Instant/SpanBegin or item id for PipelineSlice.
    uint64_t payload;
    uint32_t track_id;  ///< TimelineTrackRegistry id.
    uint16_t name_id;   ///< EventNameRegistry id (0 = none, e.g. SpanEnd).
    uint16_t slot;      ///< Lane slot; spans are addressed by (track_id, slot).
    uint8_t kind;       ///< TimelineEventKind.
    uint8_t arg_count;
    /// Lowest user category bit (0..63) for backend name resolution, or
    /// TIMELINE_NO_CATEGORY. Filtering already happened producer-side on the
    /// full 64-bit mask, so the record carries only what the backend needs —
    /// this keeps categories at bits ≥ 32 intact (unlike a truncated mask).
    uint8_t category_bit;
    uint8_t padding[5];
};

static_assert(sizeof(TimelineRecord) == 32, "TimelineRecord size mismatch");

/// Value categories for typed annotation args (normalized at the producer).
enum class TimelineArgKind : uint8_t {
    Uint = 0,
    Int = 1,
    Double = 2,
    Bool = 3,
    Pointer = 4,
    String = 5
};

/// Producer-side normalized arg, serialized via packTimelineArg().
struct TimelineArgValue {
    uint64_t bits;
    uint16_t key_id;
    TimelineArgKind kind;
};

/// On-the-wire arg size: [bits u64][key_id u16][kind u8][pad u8].
constexpr size_t TIMELINE_ARG_SIZE = 12;
constexpr size_t MAX_TIMELINE_ARGS = 8;
constexpr uint64_t TIMELINE_LOCAL_STRING_ID_BIT = 1ULL << 63;

inline uint64_t makeTimelineLocalStringId(uint64_t id) noexcept {
    return id | TIMELINE_LOCAL_STRING_ID_BIT;
}

inline bool isTimelineLocalStringId(uint64_t id) noexcept {
    return (id & TIMELINE_LOCAL_STRING_ID_BIT) != 0;
}

inline uint64_t timelineLocalStringId(uint64_t id) noexcept {
    return id & ~TIMELINE_LOCAL_STRING_ID_BIT;
}

inline void packTimelineArg(std::byte* dest, const TimelineArgValue& arg) noexcept {
    std::memcpy(dest, &arg.bits, 8);
    std::memcpy(dest + 8, &arg.key_id, 2);
    dest[10] = static_cast<std::byte>(arg.kind);
    dest[11] = std::byte{0};
}

inline TimelineArgValue unpackTimelineArg(const std::byte* src) noexcept {
    TimelineArgValue arg;
    std::memcpy(&arg.bits, src, 8);
    std::memcpy(&arg.key_id, src + 8, 2);
    arg.kind = static_cast<TimelineArgKind>(src[10]);
    return arg;
}

// ---------------------------------------------------------------------------
// String registries (event names, annotation keys, track metadata)
// ---------------------------------------------------------------------------

/**
 * @brief Append-only id ↔ string registry for compile-time literals.
 *
 * Registration is mutex-guarded; lookups lock too (the backend caches resolved
 * views per id, so the lock is off any per-event path). std::deque keeps
 * element addresses stable, so returned string_views never dangle.
 */
class TimelineStringRegistry {
public:
    uint16_t registerString(std::string_view s) {
        std::lock_guard<std::mutex> lock(mutex_);
        strings_.emplace_back(s);
        return static_cast<uint16_t>(strings_.size());  // 1-based; 0 = none.
    }

    std::string_view get(uint16_t id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (id > 0 && id <= strings_.size()) {
            return strings_[id - 1];
        }
        return {};
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return strings_.size();
    }

private:
    mutable std::mutex mutex_;
    std::deque<std::string> strings_;
};

/// Global registry of timeline event names (low-cardinality, "miss"_ev).
class EventNameRegistry : public TimelineStringRegistry {
public:
    static EventNameRegistry& instance() {
        static EventNameRegistry registry;
        return registry;
    }
};

/// Global registry of typed-annotation key names (arg<"addr">).
class AnnotationKeyRegistry : public TimelineStringRegistry {
public:
    static AnnotationKeyRegistry& instance() {
        static AnnotationKeyRegistry registry;
        return registry;
    }
};

/// Global registry of typed-annotation string values.
class AnnotationValueRegistry {
public:
    uint64_t registerString(std::string_view s) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = ids_.find(s); it != ids_.end()) {
            return it->second;
        }

        strings_.emplace_back(s);
        const uint64_t id = static_cast<uint64_t>(strings_.size());  // 1-based; 0 = none.
        ids_.emplace(std::string_view(strings_.back()), id);
        return id;
    }

    std::string_view get(uint64_t id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (id > 0 && id <= strings_.size()) {
            return strings_[static_cast<size_t>(id - 1)];
        }
        return {};
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return strings_.size();
    }

    static AnnotationValueRegistry& instance() {
        static AnnotationValueRegistry registry;
        return registry;
    }

private:
    mutable std::mutex mutex_;
    std::deque<std::string> strings_;
    std::unordered_map<std::string_view, uint64_t> ids_;
};

/** @brief Metadata for one declared timeline lane group. */
struct TimelineTrackInfo {
    enum class Layout : uint8_t {
        Normal = 0,
        Pipeline = 1,
    };

    std::string name;
    uint16_t source_id;  ///< Owning unit in the source-name registry.
    uint16_t lanes;      ///< Declared sub-lane count (1 = single track).
    Layout layout = Layout::Normal;
};

/**
 * @brief Global registry mapping track ids to their declaration metadata.
 *
 * Tracks register when a unit's observation context attaches (before the
 * simulation runs); the backend resolves ids lazily and caches per id.
 */
class TimelineTrackRegistry {
public:
    static TimelineTrackRegistry& instance() {
        static TimelineTrackRegistry registry;
        return registry;
    }

    uint32_t registerTrack(TimelineTrackInfo info) {
        std::lock_guard<std::mutex> lock(mutex_);
        tracks_.push_back(std::move(info));
        return static_cast<uint32_t>(tracks_.size());  // 1-based; 0 = invalid.
    }

    /// @return Stable reference (deque storage); empty-name sentinel when unknown.
    const TimelineTrackInfo& get(uint32_t id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (id > 0 && id <= tracks_.size()) {
            return tracks_[id - 1];
        }
        static const TimelineTrackInfo unknown{};
        return unknown;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tracks_.size();
    }

private:
    mutable std::mutex mutex_;
    std::deque<TimelineTrackInfo> tracks_;
};

namespace timeline_detail {

struct CachedTrackEntry {
    uint16_t source_id;
    uint32_t track_id;
};

template <typename Site>
uint32_t resolveTrackForSourceSlow(uint16_t source_id, std::atomic<uint64_t>& cached_entry,
                                   uint16_t lanes, TimelineTrackInfo::Layout layout) {
    static std::mutex mutex;
    static std::vector<CachedTrackEntry> entries;
    static const std::string track_name = Site::trackName();

    std::lock_guard<std::mutex> lock(mutex);
    for (const CachedTrackEntry& entry : entries) {
        if (entry.source_id == source_id) {
            cached_entry.store((static_cast<uint64_t>(source_id) << 32) | entry.track_id,
                               std::memory_order_relaxed);
            return entry.track_id;
        }
    }

    const uint32_t track_id =
        TimelineTrackRegistry::instance().registerTrack({track_name, source_id, lanes, layout});
    entries.push_back({source_id, track_id});
    cached_entry.store((static_cast<uint64_t>(source_id) << 32) | track_id,
                       std::memory_order_relaxed);
    return track_id;
}

template <typename Site>
uint32_t resolveTrackForSource(
    uint16_t source_id, uint16_t lanes,
    TimelineTrackInfo::Layout layout = TimelineTrackInfo::Layout::Normal) {
    static std::atomic<uint64_t> cached_entry{0};

    const uint64_t entry = cached_entry.load(std::memory_order_relaxed);
    const uint32_t track_id = static_cast<uint32_t>(entry);
    if (OBSERVE_LIKELY(track_id != 0 && static_cast<uint16_t>(entry >> 32) == source_id)) {
        return track_id;
    }

    return resolveTrackForSourceSlow<Site>(source_id, cached_entry, lanes, layout);
}

}  // namespace timeline_detail

// ---------------------------------------------------------------------------
// Producer vocabulary: "name"_ev, arg<"key">(value), flow(uid)
// ---------------------------------------------------------------------------

/// Resolved event-name handle; obtain via "miss"_ev (or EventName<"miss">::ref()).
struct EventNameRef {
    uint16_t id;
};

/// Each distinct name literal registers once (function-local static).
template <FixedString Name>
struct EventName {
    static uint16_t id() {
        static uint16_t value = EventNameRegistry::instance().registerString(Name);
        return value;
    }
    static EventNameRef ref() { return {id()}; }
};

template <FixedString Name>
inline EventNameRef operator""_ev() {
    return EventName<Name>::ref();
}

/// Each distinct annotation key registers once (function-local static).
template <FixedString Key>
struct AnnotationKey {
    static uint16_t id() {
        static uint16_t value = AnnotationKeyRegistry::instance().registerString(Key);
        return value;
    }
};

/// Flow id linking events across tracks (e.g. an instruction uid). Pass to
/// TimelineLane::begin()/instant() alongside typed args.
struct Flow {
    uint64_t id;
};

inline Flow flow(uint64_t id) noexcept { return {id}; }

/// Typed annotation argument; construct via arg<"key">(value).
template <typename T>
struct TypedArg {
    uint16_t key_id;
    T value;
};

template <FixedString Key, typename T>
inline TypedArg<T> arg(T value) {
    using V = std::decay_t<T>;
    static_assert(std::is_arithmetic_v<V> || std::is_pointer_v<V> ||
                      std::is_same_v<V, std::string> || std::is_same_v<V, std::string_view>,
                  "typed timeline args support arithmetic, pointer, and string values");
    return {AnnotationKey<Key>::id(), value};
}

namespace timeline_arg_detail {

template <typename T>
inline constexpr bool is_string_arg_v =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>;

inline std::string_view stringArgView(const std::string& s) noexcept { return s; }
inline std::string_view stringArgView(std::string_view s) noexcept { return s; }

}  // namespace timeline_arg_detail

/// Normalizes a typed arg to its wire representation.
template <typename T>
inline TimelineArgValue normalizeTimelineArg(const TypedArg<T>& a) noexcept {
    using V = std::decay_t<T>;
    if constexpr (timeline_arg_detail::is_string_arg_v<V>) {
        const uint64_t id = AnnotationValueRegistry::instance().registerString(
            timeline_arg_detail::stringArgView(a.value));
        return {id, a.key_id, TimelineArgKind::String};
    } else if constexpr (std::is_same_v<V, bool>) {
        return {a.value ? 1ULL : 0ULL, a.key_id, TimelineArgKind::Bool};
    } else if constexpr (std::is_floating_point_v<V>) {
        return {std::bit_cast<uint64_t>(static_cast<double>(a.value)), a.key_id,
                TimelineArgKind::Double};
    } else if constexpr (std::is_pointer_v<V>) {
        return {static_cast<uint64_t>(reinterpret_cast<uintptr_t>(a.value)), a.key_id,
                TimelineArgKind::Pointer};
    } else if constexpr (std::is_signed_v<V>) {
        return {std::bit_cast<uint64_t>(static_cast<int64_t>(a.value)), a.key_id,
                TimelineArgKind::Int};
    } else {
        return {static_cast<uint64_t>(a.value), a.key_id, TimelineArgKind::Uint};
    }
}

namespace timeline_detail {

/// Folds one begin()/instant() pack item into the arg array or the flow id.
template <typename T>
inline void foldTimelineItem(TimelineArgValue* args, size_t& arg_count, uint64_t& flow_id,
                             const T& item) noexcept {
    if constexpr (std::is_same_v<std::decay_t<T>, Flow>) {
        flow_id = item.id;
    } else {
        args[arg_count++] = normalizeTimelineArg(item);
    }
}

}  // namespace timeline_detail

}  // namespace chronon::observe
