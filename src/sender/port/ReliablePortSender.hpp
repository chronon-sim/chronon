// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>

#include "../core/Unit.hpp"
#include "PortTransaction.hpp"

namespace chronon::sender {

/**
 * Producer-owned, bounded retry state for one logical port publication.
 *
 * A failed send retains exactly one payload tuple. Calling send() again in a
 * later producer cycle retries that tuple before observing the new arguments,
 * which supports the usual stall-until-delivered pattern without a model-owned
 * optional/deque. Multi-port instances publish through PortTransaction, so a
 * retry is visible at every destination or at none of them.
 *
 * The object is cache-line aligned and accessed only by its owning Unit. It
 * contains no lock, atomic, heap allocation, or receiver-shared mutable state.
 * Ordinary OutPort::send()/canSend() do not know that reliable senders exist.
 *
 * @code
 * OutPort<Message> out{this, "out"};
 * ReliablePortSender reliable{out};
 *
 * void tick() override {
 *     // Do not advance architectural state while false is returned. On the
 *     // next call reliable retries its retained payload, not this argument.
 *     if (!reliable.send(Message{next_sequence_})) return;
 *     ++next_sequence_;
 * }
 * @endcode
 */
template <typename... Ts>
class alignas(64) ReliablePortSender {
    static_assert(sizeof...(Ts) != 0, "a reliable sender needs at least one OutPort");
    static_assert((std::is_nothrow_move_constructible_v<Ts> && ...),
                  "reliable payloads must be nothrow move constructible");
    static_assert((std::is_nothrow_move_assignable_v<Ts> && ...),
                  "reliable payloads must be nothrow move assignable");

    using Ports = std::tuple<OutPort<Ts>*...>;
    using Payloads = std::tuple<Ts...>;
    using FirstPayload = std::tuple_element_t<0, Payloads>;

public:
    static constexpr size_t PENDING_CAPACITY = 1;
    static constexpr uint64_t NO_CYCLE = std::numeric_limits<uint64_t>::max();

    explicit ReliablePortSender(OutPort<Ts>&... ports) noexcept : ports_(&ports...) {}

    ReliablePortSender(const ReliablePortSender&) = delete;
    ReliablePortSender& operator=(const ReliablePortSender&) = delete;
    ReliablePortSender(ReliablePortSender&&) = delete;
    ReliablePortSender& operator=(ReliablePortSender&&) = delete;

    /**
     * Publish a new logical payload, or retry the retained payload.
     *
     * When a payload is pending, the supplied arguments are deliberately not
     * consumed. A true result always completes the same logical operation that
     * previously returned false, so callers must not advance its source state
     * while stalled.
     */
    template <typename... Values>
        requires(sizeof...(Values) == sizeof...(Ts)) &&
                (std::constructible_from<Ts, Values &&> && ...)
    [[nodiscard]] bool send(Values&&... values) {
        if (pending_) return retry();
        return submit(std::forward<Values>(values)...);
    }

    /**
     * Submit a new logical payload. Calling submit() while the one framework
     * slot is occupied is a protocol error and reports the unit, ports, payload
     * types, and the cycle at which the retained request began.
     */
    template <typename... Values>
        requires(sizeof...(Values) == sizeof...(Ts)) &&
                (std::constructible_from<Ts, Values &&> && ...)
    [[nodiscard]] bool submit(Values&&... values) {
        if (pending_) throwProtocolError_("submit called while the pending slot is occupied");

        Payloads payloads(std::forward<Values>(values)...);
        const uint64_t cycle = currentCycle_();
        if (tryPublish_(payloads)) return true;

        pending_.emplace(std::move(payloads));
        pending_since_cycle_ = cycle;
        last_attempt_cycle_ = cycle;
        return false;
    }

    /** Retry the retained payload at most once per producer cycle. */
    [[nodiscard]] bool retry() {
        if (!pending_) throwProtocolError_("retry called without a pending payload");

        const uint64_t cycle = currentCycle_();
        if (cycle == last_attempt_cycle_) return false;
        last_attempt_cycle_ = cycle;
        if (!tryPublish_(*pending_)) return false;

        pending_.reset();
        pending_since_cycle_ = NO_CYCLE;
        last_attempt_cycle_ = NO_CYCLE;
        return true;
    }

    [[nodiscard]] bool pending() const noexcept { return pending_.has_value(); }
    [[nodiscard]] size_t pendingCount() const noexcept { return pending_ ? 1 : 0; }
    [[nodiscard]] static constexpr size_t pendingCapacity() noexcept { return PENDING_CAPACITY; }
    [[nodiscard]] uint64_t pendingSinceCycle() const noexcept { return pending_since_cycle_; }

    /** Cancel the retained, not-yet-published payload tuple. */
    bool cancelPending() noexcept {
        if (!pending_) return false;
        pending_.reset();
        pending_since_cycle_ = NO_CYCLE;
        last_attempt_cycle_ = NO_CYCLE;
        return true;
    }

    /**
     * Cancel the pending tuple when an inline, non-throwing predicate accepts
     * it. The predicate receives one const payload reference per OutPort.
     */
    template <typename Predicate>
        requires std::is_nothrow_invocable_r_v<bool, std::remove_reference_t<Predicate>&,
                                               const Ts&...>
    bool cancelPendingIf(Predicate&& predicate) noexcept {
        if (!pending_) return false;
        const bool cancel = std::apply(
            [&](const auto&... payloads) noexcept { return std::invoke(predicate, payloads...); },
            std::as_const(*pending_));
        return cancel && cancelPending();
    }

    /**
     * One-port convenience matching InPort::flush<KeyFn> keep-range semantics.
     * Only the retained payload is inspected; already-published messages remain
     * governed by InPort selective flush or cancelInFlight().
     */
    template <auto KeyFn>
        requires(sizeof...(Ts) == 1) &&
                std::is_nothrow_invocable_v<decltype(KeyFn), const FirstPayload&> &&
                std::convertible_to<std::invoke_result_t<decltype(KeyFn), const FirstPayload&>,
                                    uint64_t>
    bool flushPending(FlushRange keep_range) noexcept {
        return cancelPendingIf([keep_range](const FirstPayload& payload) noexcept {
            const auto key = static_cast<uint64_t>(std::invoke(KeyFn, payload));
            return !keep_range.keeps(key);
        });
    }

    /** Cancel the retained request and every already-published message. */
    void cancelInFlight() noexcept {
        (void)cancelPending();
        std::apply([](auto*... ports) noexcept { (ports->cancelInFlight(), ...); }, ports_);
    }

private:
    [[nodiscard]] uint64_t currentCycle_() const noexcept {
        return std::get<0>(ports_)->getCurrentCycle();
    }

    [[nodiscard]] bool tryPublish_(Payloads& payloads) {
        auto transaction = std::apply([](auto*... ports) { return reserve(*ports...); }, ports_);
        if (!transaction.supported()) {
            throwProtocolError_("port ownership, topology, or payload type is unsupported");
        }
        if (!transaction) return false;
        return transaction.commitOwned_(payloads);
    }

    [[noreturn, gnu::cold, gnu::noinline]] void throwProtocolError_(const char* reason) const {
        const auto* first = std::get<0>(ports_);
        std::string message = "ReliablePortSender unit='";
        message += first->owner() ? first->owner()->name() : "<none>";
        message += "' ports=[";

        bool first_port = true;
        std::apply(
            [&](const auto*... ports) {
                auto append = [&](const auto* port) {
                    if (!first_port) message += ',';
                    first_port = false;
                    message += port->name();
                };
                (append(ports), ...);
            },
            ports_);

        message += "] payload_types=[";
        bool first_type = true;
        auto append_type = [&](const std::type_info& type) {
            if (!first_type) message += ',';
            first_type = false;
            message += type.name();
        };
        (append_type(typeid(Ts)), ...);
        message += "] ";
        message += reason;
        if (pending_) {
            message += " pending_since_cycle=";
            message += std::to_string(pending_since_cycle_);
        }
        throw std::logic_error(message);
    }

    Ports ports_;
    std::optional<Payloads> pending_;
    uint64_t pending_since_cycle_ = NO_CYCLE;
    uint64_t last_attempt_cycle_ = NO_CYCLE;
};

template <typename... Ts>
ReliablePortSender(OutPort<Ts>&...) -> ReliablePortSender<Ts...>;

}  // namespace chronon::sender
