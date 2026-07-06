// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// ObservableUnit.hpp
//
// Mixin class providing observability to simulation units.

#pragma once

#include <string_view>
#include <vector>

#include "Counter.hpp"
#include "ObservationContext.hpp"
#include "ObserveApi.hpp"
#include "PipelineApi.hpp"
#include "TimelineObserve.hpp"
#include "Types.hpp"

namespace chronon::observe {

// Forward declarations
class Counter;
class DerivedCounter;
class TimelineTrackBase;

/**
 * ObservableUnit - Mixin providing observability to simulation units.
 *
 * Inherit from this class (in addition to Unit) to add observation capabilities.
 *
 * Usage:
 * @code
 *   inline const auto CACHE_HIT = Category<"cache_hit", "Cache hit">{};
 *
 *   class MyUnit : public Unit, public ObservableUnit {
 *       EventCounter ops_{this, "ops", "Operations executed"};
 *
 *   public:
 *       void tick() override {
 *           ++ops_;
 *           event<"cache_hit">(CACHE_HIT, arg<"addr">(addr));
 *           debug<"Processing cycle {}">(cycle);
 *       }
 *   };
 * @endcode
 */
class ObservableUnit {
public:
    virtual ~ObservableUnit() = default;

    /**
     * Get the observation context.
     *
     * Returns nullptr if not set (observability disabled).
     */
    ObservationContext* observationContext() noexcept { return observe_ctx_; }
    const ObservationContext* observationContext() const noexcept { return observe_ctx_; }

    /**
     * Set the observation context.
     *
     * Called by Simulation during unit initialization.
     * Initializes all pending counters.
     */
    void setObservationContext(ObservationContext* ctx) noexcept {
        observe_ctx_ = ctx;
        initializePendingCounters();
        initializePendingDerivedCounters();
        initializePendingTimelineTracks();
    }

    /**
     * Register a counter for initialization when context is attached.
     *
     * Called by Counter constructor.
     */
    void registerCounter(Counter* counter) { pending_counters_.push_back(counter); }

    /**
     * Register a derived counter for initialization when context is attached.
     *
     * Called by DerivedCounter constructor.
     */
    void registerDerivedCounter(DerivedCounter* dc) { pending_derived_counters_.push_back(dc); }

    /**
     * Register a timeline lane/counter for initialization when context is attached.
     *
     * Called by TimelineTrackBase constructor.
     */
    void registerTimelineTrack(TimelineTrackBase* track) {
        pending_timeline_tracks_.push_back(track);
    }

    /**
     * Get the current cycle for observation timestamps.
     *
     * Override this in derived classes to return the unit's local cycle.
     * Default returns 0.
     */
    virtual uint64_t getObserveCycle() const noexcept { return 0; }

    /**
     * Check if observability is enabled.
     */
    bool observabilityEnabled() const noexcept { return observe_ctx_ != nullptr; }

    /**
     * Get per-unit observation stats (emitted/dropped per channel).
     */
    const ObservationStats& observationStats() const noexcept {
        static const ObservationStats empty{};
        return observe_ctx_ ? observe_ctx_->observationStats() : empty;
    }

    /**
     * Get stats for a specific observation channel.
     */
    template <ObservationChannel Ch>
    const ObservationChannelStats& channelStats() const noexcept {
        static const ObservationChannelStats empty{};
        return observe_ctx_ ? observe_ctx_->observationStats().get<Ch>() : empty;
    }

    // =========================================================================
    // Modern Template-Based API (no macros)
    // =========================================================================

    /**
     * Emit a deprecated text trace event with compile-time format string.
     *
     * Usage:
     *   trace<"I-Cache HIT: pc=0x{:x}">(ICACHE_HIT, pc);
     *
     * @tparam Fmt Format string (compile-time)
     * @param category Trace category
     * @param args Format arguments
     */
    template <FixedString Fmt, typename Cat, typename... Args>
    [[deprecated(
        "trace<> text events are deprecated and will be removed after 0.4; use timeline events for "
        "structured Perfetto data or debug/info/warn/error for text logs")]]
    void trace(Cat category, Args&&... args) {
        if (observe_ctx_ && observe_ctx_->shouldTrace(category)) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            observe_detail::emitTrace<Fmt>(observe_ctx_, category, std::forward<Args>(args)...);
        }
    }

    /**
     * Emit a first-class pipeline slice whose pipe/stage are compile-time constants.
     * The item id is both the visible Perfetto slice name and the flow id.
     */
    template <uint16_t Pipe, uint16_t Stage, typename Cat, typename... Items>
    void pipe(Cat category, uint64_t id, Items&&... items) {
        if (observe_ctx_) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::pipe<Pipe, Stage>(observe_ctx_, category, id,
                                                std::forward<Items>(items)...);
        }
    }

    template <uint16_t Pipe, FixedString Stage, typename Cat, typename... Items>
    void pipeStage(Cat category, uint64_t id, Items&&... items) {
        if (observe_ctx_) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::pipeStage<Pipe, Stage>(observe_ctx_, category, id,
                                                     std::forward<Items>(items)...);
        }
    }

    template <uint16_t Pipe, FixedString Stage, typename Cat, typename... Items>
    void pipeStageHex(Cat category, uint64_t id, Items&&... items) {
        if (observe_ctx_) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::pipeStageHex<Pipe, Stage>(observe_ctx_, category, id,
                                                        std::forward<Items>(items)...);
        }
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void pipeStage(uint16_t pipe, Cat category, uint64_t id, Items&&... items) {
        if (observe_ctx_) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::pipeStage<Stage>(observe_ctx_, pipe, category, id,
                                               std::forward<Items>(items)...);
        }
    }

    template <FixedString Stage, typename Cat, typename... Items>
    void pipeStageHex(uint16_t pipe, Cat category, uint64_t id, Items&&... items) {
        if (observe_ctx_) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::pipeStageHex<Stage>(observe_ctx_, pipe, category, id,
                                                  std::forward<Items>(items)...);
        }
    }

    /**
     * Emit a low-cardinality instant event on this unit's shared "events" track.
     */
    template <FixedString Name, typename Cat, typename... Items>
    void event(Cat category, Items&&... items) {
        timeline_observe_detail::emitUnitEvent<Name>(*this, category,
                                                     std::forward<Items>(items)...);
    }

    /**
     * Emit an instant event on a named track under this unit.
     */
    template <FixedString Track, typename Cat, typename... Items>
    void instant(Cat category, EventNameRef name, Items&&... items) {
        timeline_observe_detail::emitUnitInstant<Track>(*this, category, name,
                                                        std::forward<Items>(items)...);
    }

    /**
     * Open/close an occupancy span on a named track under this unit.
     */
    template <FixedString Track, typename Cat, typename... Items>
    void spanBegin(Cat category, EventNameRef name, Items&&... items) {
        timeline_observe_detail::emitUnitSpanBegin<Track>(*this, category, name,
                                                          std::forward<Items>(items)...);
    }

    template <FixedString Track, typename Cat, typename... Items>
    void spanBegin(uint16_t slot, Cat category, EventNameRef name, Items&&... items) {
        timeline_observe_detail::emitUnitSpanBegin<Track>(*this, slot, category, name,
                                                          std::forward<Items>(items)...);
    }

    template <FixedString Track>
    void spanEnd() {
        timeline_observe_detail::emitUnitSpanEnd<Track>(*this);
    }

    template <FixedString Track>
    void spanEnd(uint16_t slot) {
        timeline_observe_detail::emitUnitSpanEnd<Track>(*this, slot);
    }

    /**
     * Emit push-model counter samples under this unit.
     */
    template <FixedString Name, typename T>
    [[deprecated(
        "ObservableUnit::gauge() is deprecated; use EventCounter::add() for aggregate metrics or "
        "EventCounter::mark() when a timeline instant is needed")]]
    void gauge(T value, std::string_view unit_name = {}) {
        timeline_observe_detail::emitUnitGauge<Name>(*this, category::NONE, value, unit_name);
    }

    template <FixedString Name, typename Used, typename Capacity>
    [[deprecated(
        "ObservableUnit::capacity() is deprecated; use EventCounter-based metrics instead of "
        "push-model timeline counters")]]
    void capacity(Used used, Capacity total, std::string_view unit_name = {}) {
        timeline_observe_detail::emitUnitCapacity<Name>(*this, category::NONE, used, total,
                                                        unit_name);
    }

    /**
     * Emit a debug log message.
     *
     * Usage:
     *   debug<"Processing request id={}">(request_id);
     */
    template <FixedString Fmt, typename... Args>
    void debug(Args&&... args) {
        emitLog_<LogLevel::Debug, Fmt>(std::forward<Args>(args)...);
    }

    /**
     * Emit an info log message.
     */
    template <FixedString Fmt, typename... Args>
    void info(Args&&... args) {
        emitLog_<LogLevel::Info, Fmt>(std::forward<Args>(args)...);
    }

    /**
     * Emit a warning log message.
     */
    template <FixedString Fmt, typename... Args>
    void warn(Args&&... args) {
        emitLog_<LogLevel::Warn, Fmt>(std::forward<Args>(args)...);
    }

    /**
     * Emit an error log message.
     */
    template <FixedString Fmt, typename... Args>
    void error(Args&&... args) {
        emitLog_<LogLevel::Error, Fmt>(std::forward<Args>(args)...);
    }

protected:
    ObservationContext* observe_ctx_ = nullptr;

private:
    template <LogLevel Level, FixedString Fmt, typename... Args>
    void emitLog_(Args&&... args) {
        if (observe_ctx_ && observe_ctx_->template shouldLog<Level>()) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::log<Level, Fmt>(observe_ctx_, std::forward<Args>(args)...);
        }
    }

    template <typename T>
    void attachPending_(std::vector<T*>& pending) {
        if (!observe_ctx_) {
            return;
        }
        for (auto* item : pending) {
            if (item) {
                item->onContextAttached(observe_ctx_);
            }
        }
        pending.clear();
    }

    // Pending counters to be initialized when context is attached
    std::vector<Counter*> pending_counters_;

    // Pending derived counters to be initialized when context is attached
    std::vector<DerivedCounter*> pending_derived_counters_;

    // Pending timeline lanes/counters to be initialized when context is attached
    std::vector<TimelineTrackBase*> pending_timeline_tracks_;

    /**
     * Initialize all pending counters.
     *
     * Called when observation context is set.
     */
    void initializePendingCounters();

    /**
     * Initialize all pending derived counters.
     *
     * Called when observation context is set.
     * Defined in DerivedCounter.cpp (needs DerivedCounter to be complete).
     */
    void initializePendingDerivedCounters();

    /**
     * Initialize all pending timeline lanes/counters.
     *
     * Called when observation context is set.
     * Defined in TimelineTrack.cpp (needs TimelineTrackBase to be complete).
     */
    void initializePendingTimelineTracks();
};

}  // namespace chronon::observe
