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
#include <utility>

#include "FixedString.hpp"
#include "LocalCounter.hpp"
#include "ObservableUnit.hpp"

namespace chronon::observe {

/**
 * @brief Counter plus optional first-class timeline instant emission.
 *
 * Use this when the same model fact should be visible as both an aggregate
 * counter and a timestamped event. Plain operator++/operator+= remain counter
 * only; mark()/add() are the combined operations.
 *
 * @code
 *   EventCounter uart_tx_{this, "chars_tx", "Characters transmitted", "chars"};
 *
 *   uart_tx_.mark<"uart_tx">(UART, arg<"byte">(byte));
 *   flushed_instrs_.add<"decode_flush">(removed, FLUSH, arg<"remaining">(n));
 * @endcode
 */
class EventCounter {
public:
    EventCounter(ObservableUnit* owner, std::string_view name, std::string_view description = "",
                 std::string_view unit = "")
        : owner_(owner), counter_(owner, name, description, unit) {}

    EventCounter(const EventCounter&) = delete;
    EventCounter& operator=(const EventCounter&) = delete;
    EventCounter(EventCounter&&) noexcept = default;
    EventCounter& operator=(EventCounter&&) noexcept = default;

    [[gnu::always_inline]] EventCounter& operator++() noexcept {
        ++counter_;
        return *this;
    }

    [[gnu::always_inline]] EventCounter& operator+=(uint64_t delta) noexcept {
        counter_ += delta;
        return *this;
    }

    template <FixedString Name, typename Cat, typename... Items>
    [[gnu::always_inline]] void mark(Cat category, Items&&... items) {
        add<Name>(1, category, std::forward<Items>(items)...);
    }

    template <FixedString Name, typename Cat, typename... Items>
    [[gnu::always_inline]] void add(uint64_t delta, Cat category, Items&&... items) {
        counter_ += delta;
        if (owner_) {
            owner_->template event<Name>(category, std::forward<Items>(items)...);
        }
    }

    [[gnu::always_inline]] uint64_t get() const noexcept { return counter_.get(); }

    void reset() noexcept { counter_.reset(); }

    Counter& counter() noexcept { return counter_; }
    const Counter& counter() const noexcept { return counter_; }
    operator Counter&() noexcept { return counter_; }
    operator const Counter&() const noexcept { return counter_; }

    const std::string& name() const noexcept { return counter_.name(); }
    const std::string& description() const noexcept { return counter_.description(); }
    const std::string& unit() const noexcept { return counter_.unit(); }
    bool isRegistered() const noexcept { return counter_.isRegistered(); }
    CounterId id() const noexcept { return counter_.id(); }

private:
    ObservableUnit* owner_ = nullptr;
    Counter counter_;
};

}  // namespace chronon::observe
