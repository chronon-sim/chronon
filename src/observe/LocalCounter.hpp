// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// LocalCounter.hpp
//
// Per-instance counter for hierarchical counter system.
// Allows counter declarations as unit members with automatic registration.

#pragma once

#include <string>
#include <string_view>

#include "Counter.hpp"
#include "ObservationContext.hpp"
#include "Types.hpp"

namespace chronon::observe {

// Forward declaration
class ObservableUnit;

namespace counter_detail {

struct InternalConstructionTag {
    explicit InternalConstructionTag() = default;
};

}  // namespace counter_detail

/**
 * Counter - Internal per-instance counter storage used by EventCounter and
 * DerivedCounter.
 *
 * Each unit instance has its own counter with separate values:
 * - Counter names include the unit's hierarchical path (e.g., "cpu0.alu0.ops")
 * - Fast increment path (~2-3ns) via direct context access
 *
 * Output (CSV):
 * @code
 *   cycle,unit_name,counter_name,value
 *   10000,cpu0.alu0,ops,2500
 *   10000,cpu0.alu1,ops,2400
 *   10000,cpu1.alu0,ops,2550
 * @endcode
 */
class Counter {
public:
    Counter(counter_detail::InternalConstructionTag, ObservableUnit* owner, std::string_view name,
            std::string_view description = "", std::string_view unit = "");

    // Non-copyable (tied to specific unit instance)
    Counter(const Counter&) = delete;
    Counter& operator=(const Counter&) = delete;

    // Movable
    Counter(Counter&& other) noexcept;
    Counter& operator=(Counter&& other) noexcept;

    ~Counter() = default;

    [[gnu::always_inline]] Counter& operator++() noexcept {
        if (registered_ && ctx_) {
            ctx_->count(id_, 1);
        }
        return *this;
    }

    [[gnu::always_inline]] Counter& operator+=(uint64_t delta) noexcept {
        if (registered_ && ctx_) {
            ctx_->count(id_, delta);
        }
        return *this;
    }

    [[gnu::always_inline]] uint64_t get() const noexcept {
        if (registered_ && ctx_) {
            return ctx_->counters().getUnchecked(id_).get();
        }
        return 0;
    }

    void reset() noexcept {
        if (registered_ && ctx_) {
            ctx_->counters().getUnchecked(id_).reset();
        }
    }

    const std::string& name() const noexcept { return name_; }
    const std::string& description() const noexcept { return description_; }
    const std::string& unit() const noexcept { return unit_; }
    bool isRegistered() const noexcept { return registered_; }
    CounterId id() const noexcept { return id_; }

private:
    friend class ObservableUnit;

    /**
     * Called by ObservableUnit when observation context is attached.
     *
     * Registers the counter with the context and caches the ID for fast access.
     */
    void onContextAttached(ObservationContext* ctx);

    ObservableUnit* owner_ = nullptr;
    std::string name_;
    std::string description_;
    std::string unit_;
    ObservationContext* ctx_ = nullptr;
    CounterId id_{};
    bool registered_ = false;
};

}  // namespace chronon::observe
