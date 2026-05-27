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

#include <vector>

#include "Counter.hpp"
#include "ObservationContext.hpp"
#include "ObserveApi.hpp"
#include "Types.hpp"

namespace chronon::observe {

// Forward declarations
class Counter;
class DerivedCounter;

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
 *       Counter ops_{this, "ops", "Operations executed"};
 *
 *   public:
 *       void tick() override {
 *           ++ops_;
 *           trace<"Hit at 0x{:x}">(CACHE_HIT, addr);
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
     * Emit a trace event with compile-time format string.
     *
     * Usage:
     *   trace<"I-Cache HIT: pc=0x{:x}">(ICACHE_HIT, pc);
     *
     * @tparam Fmt Format string (compile-time)
     * @param category Trace category
     * @param args Format arguments
     */
    template <FixedString Fmt, typename Cat, typename... Args>
    void trace(Cat category, Args&&... args) {
        if (observe_ctx_ && observe_ctx_->shouldTrace(category)) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            chronon::observe::trace<Fmt>(observe_ctx_, category, std::forward<Args>(args)...);
        }
    }

    /**
     * Emit a debug log message.
     *
     * Usage:
     *   debug<"Processing request id={}">(request_id);
     */
    template <FixedString Fmt, typename... Args>
    void debug(Args&&... args) {
        if (observe_ctx_ && observe_ctx_->template shouldLog<LogLevel::Debug>()) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            log_debug<Fmt>(observe_ctx_, std::forward<Args>(args)...);
        }
    }

    /**
     * Emit an info log message.
     */
    template <FixedString Fmt, typename... Args>
    void info(Args&&... args) {
        if (observe_ctx_ && observe_ctx_->template shouldLog<LogLevel::Info>()) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            log_info<Fmt>(observe_ctx_, std::forward<Args>(args)...);
        }
    }

    /**
     * Emit a warning log message.
     */
    template <FixedString Fmt, typename... Args>
    void warn(Args&&... args) {
        if (observe_ctx_ && observe_ctx_->template shouldLog<LogLevel::Warn>()) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            log_warn<Fmt>(observe_ctx_, std::forward<Args>(args)...);
        }
    }

    /**
     * Emit an error log message.
     */
    template <FixedString Fmt, typename... Args>
    void error(Args&&... args) {
        if (observe_ctx_ && observe_ctx_->template shouldLog<LogLevel::Error>()) {
            observe_ctx_->setCurrentCycleValue(getObserveCycle());
            log_error<Fmt>(observe_ctx_, std::forward<Args>(args)...);
        }
    }

protected:
    ObservationContext* observe_ctx_ = nullptr;

private:
    // Pending counters to be initialized when context is attached
    std::vector<Counter*> pending_counters_;

    // Pending derived counters to be initialized when context is attached
    std::vector<DerivedCounter*> pending_derived_counters_;

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
};

}  // namespace chronon::observe
