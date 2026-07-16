// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// EventCounter.hpp
//
// Counter wrapper for observations that are both aggregate metrics and
// timestamped events. It keeps the counter hot path identical to Counter while
// giving call sites a single semantic operation when a timeline instant is
// also useful.

#pragma once

#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>

#include "FixedString.hpp"
#include "LocalCounter.hpp"
#include "ObservableUnit.hpp"

namespace chronon::observe {

/**
 * @brief Counter plus optional first-class timeline instant emission.
 *
 * Use this for aggregate metrics that may also need timestamped samples.
 * add()/operator++/operator+= update the aggregate counter only; mark() updates
 * the counter and emits a first-class timeline instant.
 * Aggregate updates are compiled out when CHRONON_ENABLE_COUNTER_UPDATES is 0;
 * mark() still emits its timeline event.
 *
 * @code
 *   EventCounter uart_tx_{this, "chars_tx", "Characters transmitted", "chars"};
 *
 *   uart_tx_.mark<"uart_tx">(UART, arg<"byte">(byte));
 *   flushed_instrs_.add(removed);
 *   flushed_instrs_.mark<"decode_flush">(removed, FLUSH, arg<"remaining">(n));
 * @endcode
 */
class EventCounter {
public:
    EventCounter(ObservableUnit* owner, std::string_view name, std::string_view description = "",
                 std::string_view unit = "")
        : owner_(owner),
          counter_(counter_detail::InternalConstructionTag{}, owner, name, description, unit) {}

    EventCounter(const EventCounter&) = delete;
    EventCounter& operator=(const EventCounter&) = delete;
    EventCounter(EventCounter&&) noexcept = default;
    EventCounter& operator=(EventCounter&&) noexcept = default;

    [[gnu::always_inline]] EventCounter& operator++() noexcept {
        add(1);
        return *this;
    }

    [[gnu::always_inline]] EventCounter& operator+=(uint64_t delta) noexcept {
        add(delta);
        return *this;
    }

    [[gnu::always_inline]] void add(uint64_t delta = 1) noexcept { counter_ += delta; }

    template <FixedString Name, typename Cat, typename... Items>
        requires(!std::is_arithmetic_v<std::decay_t<Cat>>)
    [[gnu::always_inline]] void mark(Cat category, Items&&... items) {
        markWithDelta_<Name>(1, category, std::forward<Items>(items)...);
    }

    template <FixedString Name, typename Cat, typename... Items>
    [[gnu::always_inline]] void mark(uint64_t delta, Cat category, Items&&... items) {
        markWithDelta_<Name>(delta, category, std::forward<Items>(items)...);
    }

    template <FixedString Name, typename Cat, typename... Items>
    [[deprecated(
        "EventCounter::add<Name>(...) is deprecated; use add(delta) for counter-only updates or "
        "mark<Name>(delta, category, ...) for timeline events")]] [[gnu::always_inline]] void
    add(uint64_t delta, Cat category, Items&&... items) {
        mark<Name>(delta, category, std::forward<Items>(items)...);
    }

    [[gnu::always_inline]] uint64_t get() const noexcept { return counter_.get(); }

    void reset() noexcept { counter_.reset(); }

    [[deprecated("Direct Counter access is deprecated; use EventCounter methods instead")]]
    Counter& counter() noexcept {
        return counter_;
    }
    [[deprecated("Direct Counter access is deprecated; use EventCounter methods instead")]]
    const Counter& counter() const noexcept {
        return counter_;
    }
    [[deprecated("Implicit EventCounter-to-Counter conversion is deprecated")]]
    operator Counter&() noexcept {
        return counter_;
    }
    [[deprecated("Implicit EventCounter-to-Counter conversion is deprecated")]]
    operator const Counter&() const noexcept {
        return counter_;
    }

    const std::string& name() const noexcept { return counter_.name(); }
    const std::string& description() const noexcept { return counter_.description(); }
    const std::string& unit() const noexcept { return counter_.unit(); }
    bool isRegistered() const noexcept { return counter_.isRegistered(); }
    CounterId id() const noexcept { return counter_.id(); }

private:
    template <FixedString Name, typename Cat, typename... Items>
    [[gnu::always_inline]] void markWithDelta_(uint64_t delta, Cat category, Items&&... items) {
        add(delta);
        if (owner_) {
            owner_->template event<Name>(category, std::forward<Items>(items)...);
        }
    }

    ObservableUnit* owner_ = nullptr;
    Counter counter_;
};

}  // namespace chronon::observe
