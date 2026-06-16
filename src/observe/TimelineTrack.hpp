// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TimelineTrack.hpp
//
// Declarative timeline members for ObservableUnit: occupancy lanes (multi-cycle
// spans + instants) and push-model counter tracks. Same registration pattern as
// Counter — declare as a member, no macros, no manual registration.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "ObservationContext.hpp"
#include "TimelineApi.hpp"
#include "Types.hpp"

namespace chronon::observe {

class ObservableUnit;

/**
 * @brief Base for declarative timeline tracks; handles owner registration and
 * context attach (track-id assignment).
 */
class TimelineTrackBase {
public:
    TimelineTrackBase(ObservableUnit* owner, std::string_view name, std::string_view unit,
                      uint16_t lanes, TimelineTrackInfo::Kind kind);

    TimelineTrackBase(const TimelineTrackBase&) = delete;
    TimelineTrackBase& operator=(const TimelineTrackBase&) = delete;

    [[nodiscard]] bool isRegistered() const noexcept { return registered_; }
    [[nodiscard]] uint32_t trackId() const noexcept { return track_id_; }
    [[nodiscard]] const std::string& name() const noexcept { return name_; }

private:
    friend class ObservableUnit;

    /// Called by ObservableUnit when the observation context attaches:
    /// registers the track and caches its id for the hot path.
    void onContextAttached(ObservationContext* ctx);

protected:
    /// Stamps the owner's cycle into the context (same as ObservableUnit::trace).
    void stampCycle_() noexcept;

    ObservableUnit* owner_ = nullptr;
    ObservationContext* ctx_ = nullptr;
    std::string name_;
    std::string unit_;
    uint32_t track_id_ = 0;
    uint16_t lanes_ = 1;
    TimelineTrackInfo::Kind kind_;
    bool registered_ = false;
};

/**
 * @brief Occupancy lane group: hardware-shaped span and instant events.
 *
 * Spans are addressed by (lane, slot) tokens — begin and end are separate
 * calls that may land in different tick() invocations and epochs (no RAII).
 * Event names must be low-cardinality compile-time literals ("miss"_ev);
 * per-event details go into typed args (arg<"addr">(paddr)) so the data stays
 * SQL-aggregatable. An instruction/transaction uid passed as flow(uid) links
 * this slice to the same uid's slices on other lanes.
 *
 * @code
 *   class LSU : public Unit, public ObservableUnit {
 *       TimelineLane mshr_{this, "mshr", 8};   // one sub-lane per slot
 *
 *       inline static const auto MISS = Category<"dcache_miss">{};
 *
 *       void tick() override {
 *           mshr_.begin(slot, MISS, "miss"_ev, flow(instr.uid), arg<"addr">(paddr));
 *           ...
 *           mshr_.end(slot);   // possibly many cycles later
 *       }
 *   };
 * @endcode
 *
 * Filter semantics: begin/instant pass the standard trace-channel category and
 * temporal filters. end() is only gated on the trace channel itself — a span
 * begun inside an observation window still closes when its end falls outside;
 * ends whose begin was suppressed are dropped by the backend's open-span
 * table. Speculative (lookahead) events buffer locally and vanish on epoch
 * rollback like any other observation.
 */
class TimelineLane : public TimelineTrackBase {
public:
    /// @param lanes Number of sub-lanes (slots); 1 renders as a single track,
    ///              N > 1 as a group with one child track per slot.
    TimelineLane(ObservableUnit* owner, std::string_view name, uint16_t lanes = 1)
        : TimelineTrackBase(owner, name, /*unit=*/{}, lanes, TimelineTrackInfo::Kind::Lane) {}

    /// Open the (this lane, @p slot) span. Items: at most one flow() plus up
    /// to MAX_TIMELINE_ARGS typed args, in any order.
    template <typename Cat, typename... Items>
    void begin(uint16_t slot, Cat category, EventNameRef name, Items... items) noexcept {
        emit_(TimelineEventKind::SpanBegin, slot, category, name, items...);
    }

    /// Close the (this lane, @p slot) span at the current cycle.
    void end(uint16_t slot) noexcept {
        if (!registered_ || !ctx_->filter().shouldObserve(category::TRACE)) {
            return;
        }
        stampCycle_();  // Ends skip temporal filters; the cycle only stamps the record.
        ctx_->timelineEvent(category::NONE, TimelineEventKind::SpanEnd, track_id_, slot,
                            /*name_id=*/0, /*payload=*/0, nullptr, 0);
    }

    /// Point event on the (this lane, @p slot) track.
    template <typename Cat, typename... Items>
    void instant(uint16_t slot, Cat category, EventNameRef name, Items... items) noexcept {
        emit_(TimelineEventKind::Instant, slot, category, name, items...);
    }

private:
    template <typename Cat, typename... Items>
    void emit_(TimelineEventKind kind, uint16_t slot, Cat category, EventNameRef name,
               Items... items) noexcept {
        constexpr size_t MAX_ITEMS = sizeof...(Items);
        constexpr size_t FLOW_ITEMS =
            (static_cast<size_t>(std::is_same_v<std::decay_t<Items>, Flow>) + ... + 0);
        static_assert(FLOW_ITEMS <= 1, "at most one flow() per timeline event");
        static_assert(MAX_ITEMS - FLOW_ITEMS <= MAX_TIMELINE_ARGS,
                      "too many typed timeline args (max 8)");

        if (!registered_) {
            return;
        }
        // Stamp the owner's cycle BEFORE the filter check: temporal filters
        // evaluate against the context's current cycle, which is a shared
        // thread-local that may still hold another unit's value.
        stampCycle_();
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx_->shouldTrace(cat_mask)) {
            return;
        }

        if constexpr (MAX_ITEMS == 0) {
            ctx_->timelineEvent(cat_mask, kind, track_id_, slot, name.id, /*payload=*/0, nullptr,
                                0);
        } else {
            TimelineArgValue args[MAX_ITEMS];
            size_t arg_count = 0;
            uint64_t flow_id = 0;
            (timeline_detail::foldTimelineItem(args, arg_count, flow_id, items), ...);
            ctx_->timelineEvent(cat_mask, kind, track_id_, slot, name.id, flow_id, args, arg_count);
        }
    }
};

/**
 * @brief Push-model counter track: explicit samples on the Perfetto timeline.
 *
 * Independent of the pull-model Counter/counters.csv machinery — sample() is
 * an event through the trace channel, so temporal filters apply and lookahead
 * rollback discards speculative samples.
 *
 * @code
 *   TimelineCounter occ_{this, "lsq_occupancy", "entries"};
 *   occ_.sample(lsq_.size());
 * @endcode
 */
class TimelineCounter : public TimelineTrackBase {
public:
    TimelineCounter(ObservableUnit* owner, std::string_view name, std::string_view unit = {})
        : TimelineTrackBase(owner, name, unit, /*lanes=*/1, TimelineTrackInfo::Kind::Counter) {}

    void sample(int64_t value) noexcept { sample(category::NONE, value); }

    template <typename Cat>
    void sample(Cat category, int64_t value) noexcept {
        if (!registered_) {
            return;
        }
        stampCycle_();  // Before the filter check: temporal filters read the current cycle.
        const CategoryMask cat_mask = static_cast<CategoryMask>(category);
        if (!ctx_->shouldTrace(cat_mask)) {
            return;
        }
        ctx_->timelineEvent(cat_mask, TimelineEventKind::CounterSample, track_id_,
                            /*slot=*/0, /*name_id=*/0, std::bit_cast<uint64_t>(value), nullptr, 0);
    }
};

}  // namespace chronon::observe
