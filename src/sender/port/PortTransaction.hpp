// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <concepts>
#include <cstddef>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "OutPort.hpp"

namespace chronon::sender {

/**
 * Stack-only, producer-owned reservation spanning independent OutPorts.
 *
 * Construction claims every participating port before any payload is visible.
 * commit() validates the complete claim set before starting publication, then
 * consumes the already-accounted slots.  Dropping or canceling the handle
 * releases every claim.  The owning Unit remains the sole producer, so the
 * implementation needs no lock, shared reservation counter, heap allocation,
 * or additional branch in OutPort::send().
 *
 * A transaction is intentionally cycle-local and all ports must belong to the
 * same Unit.  Payloads can be committed positionally:
 *
 *   auto tx = reserve(selected_iq, rob);
 *   if (tx && tx.commit(inst, inst)) { ... }
 *
 * or staged by port identity before a no-argument commit():
 *
 *   if (!tx.send(selected_iq, inst) || !tx.send(rob, inst) || !tx.commit()) {
 *       return;
 *   }
 */
template <typename... Ts>
class PortTransaction<OutPort<Ts>...> {
    static_assert(sizeof...(Ts) != 0, "a port transaction needs at least one OutPort");

    using Ports = std::tuple<OutPort<Ts>*...>;
    using Reservations = std::tuple<typename OutPort<Ts>::TransactionReservation...>;
    using StagedPayloads = std::tuple<std::optional<Ts>...>;

public:
    explicit PortTransaction(OutPort<Ts>&... ports) : ports_(&ports...) {
        reserved_ = contextValid_() && reserveFrom_<0>();
    }

    ~PortTransaction() { releaseAll_(); }

    PortTransaction(const PortTransaction&) = delete;
    PortTransaction& operator=(const PortTransaction&) = delete;

    PortTransaction(PortTransaction&& other) noexcept((std::is_nothrow_move_constructible_v<Ts> &&
                                                       ...))
        : ports_(other.ports_),
          reservations_(other.reservations_),
          staged_(std::move(other.staged_)),
          reserved_(std::exchange(other.reserved_, false)) {}

    PortTransaction& operator=(PortTransaction&&) = delete;

    /** True when every claim still belongs to this cycle and remains valid. */
    [[nodiscard]] explicit operator bool() const { return valid(); }

    [[nodiscard]] bool valid() const { return reserved_ && validateFrom_<0>(); }

    /** Release all claims without publishing. Safe to call more than once. */
    void cancel() noexcept { releaseAll_(); }

    /**
     * Stage one payload by participating port identity. This operation never
     * publishes. Staging the same port again replaces its previous value.
     */
    template <typename T, typename U>
        requires std::constructible_from<T, U&&>
    [[nodiscard]] bool stage(OutPort<T>& port, U&& value) {
        if (!reserved_) return false;
        return stageFrom_<0>(port, std::forward<U>(value));
    }

    /** Issue-style spelling retained for the transaction example API. */
    template <typename T, typename U>
        requires std::constructible_from<T, U&&>
    [[nodiscard]] bool send(OutPort<T>& port, U&& value) {
        return stage(port, std::forward<U>(value));
    }

    /** Commit payloads positionally, in the same order as reserve(). */
    template <typename... Values>
        requires(sizeof...(Values) == sizeof...(Ts)) &&
                (std::constructible_from<Ts, Values &&> && ...)
    [[nodiscard]] bool commit(Values&&... values) {
        if (!reserved_) return false;
        // Complete all potentially-throwing caller-to-payload construction
        // before validating or making the first delivery visible.
        std::tuple<Ts...> payloads(std::forward<Values>(values)...);
        if (!validateForCommit_()) return false;
        commitTupleFrom_<0>(payloads);
        reserved_ = false;
        return true;
    }

    /** Commit the values previously supplied through stage()/send(). */
    [[nodiscard]] bool commit() {
        if (!reserved_ || !allStagedFrom_<0>()) return false;
        if (!validateForCommit_()) return false;
        commitStagedFrom_<0>();
        reserved_ = false;
        return true;
    }

private:
    [[nodiscard]] bool contextValid_() const noexcept {
        constexpr size_t count = sizeof...(Ts);
        const auto* first = std::get<0>(ports_);
        Unit* owner = first->owner();
        if (!owner) return false;

        const uint64_t cycle = first->getCurrentCycle();
        bool same_context = true;
        std::apply(
            [&](const auto*... ports) {
                same_context =
                    ((ports->owner() == owner && ports->getCurrentCycle() == cycle) && ...);
            },
            ports_);
        if (!same_context) return false;

        std::array<const void*, count> identities{};
        size_t identity_index = 0;
        std::apply(
            [&](const auto*... ports) {
                ((identities[identity_index++] = static_cast<const void*>(ports)), ...);
            },
            ports_);
        for (size_t i = 0; i < count; ++i) {
            for (size_t j = i + 1; j < count; ++j) {
                if (identities[i] == identities[j]) return false;
            }
        }
        return true;
    }

    template <size_t I>
    [[nodiscard]] bool reserveFrom_() {
        if constexpr (I == sizeof...(Ts)) {
            return true;
        } else {
            auto* port = std::get<I>(ports_);
            auto& reservation = std::get<I>(reservations_);
            if (!port->tryReserveTransaction_(reservation)) return false;
            if (reserveFrom_<I + 1>()) return true;
            port->releaseTransaction_(reservation);
            return false;
        }
    }

    template <size_t I>
    [[nodiscard]] bool validateFrom_() const {
        if constexpr (I == sizeof...(Ts)) {
            return true;
        } else {
            return std::get<I>(ports_)->transactionReservationValid_(std::get<I>(reservations_)) &&
                   validateFrom_<I + 1>();
        }
    }

    template <size_t I>
    void releaseReverse_() noexcept {
        if constexpr (I != 0) {
            constexpr size_t index = I - 1;
            std::get<index>(ports_)->releaseTransaction_(std::get<index>(reservations_));
            releaseReverse_<index>();
        }
    }

    void releaseAll_() noexcept {
        if (!reserved_) return;
        releaseReverse_<sizeof...(Ts)>();
        reserved_ = false;
    }

    [[nodiscard]] bool validateForCommit_() {
        if (valid()) return true;
        releaseAll_();
        return false;
    }

    template <size_t I, typename T, typename U>
    [[nodiscard]] bool stageFrom_(OutPort<T>& port, U&& value) {
        if constexpr (I == sizeof...(Ts)) {
            return false;
        } else {
            using CandidatePort = std::remove_pointer_t<std::tuple_element_t<I, Ports>>;
            if constexpr (std::is_same_v<CandidatePort, OutPort<T>>) {
                if (std::get<I>(ports_) == &port) {
                    std::get<I>(staged_).emplace(std::forward<U>(value));
                    return true;
                }
            }
            return stageFrom_<I + 1>(port, std::forward<U>(value));
        }
    }

    template <size_t I>
    [[nodiscard]] bool allStagedFrom_() const noexcept {
        if constexpr (I == sizeof...(Ts)) {
            return true;
        } else {
            return std::get<I>(staged_).has_value() && allStagedFrom_<I + 1>();
        }
    }

    template <size_t I, typename PayloadTuple>
    void commitTupleFrom_(PayloadTuple& payloads) {
        if constexpr (I != sizeof...(Ts)) {
            std::get<I>(ports_)->commitTransaction_(std::get<I>(reservations_),
                                                    std::move(std::get<I>(payloads)));
            commitTupleFrom_<I + 1>(payloads);
        }
    }

    template <size_t I>
    void commitStagedFrom_() {
        if constexpr (I != sizeof...(Ts)) {
            auto& payload = std::get<I>(staged_);
            std::get<I>(ports_)->commitTransaction_(std::get<I>(reservations_),
                                                    std::move(*payload));
            commitStagedFrom_<I + 1>();
        }
    }

    Ports ports_;
    Reservations reservations_;
    StagedPayloads staged_;
    bool reserved_ = false;
};

/** Acquire an all-or-none, cycle-local reservation over the supplied ports. */
template <typename... Ts>
[[nodiscard]] PortTransaction<OutPort<Ts>...> reserve(OutPort<Ts>&... ports) {
    return PortTransaction<OutPort<Ts>...>(ports...);
}

}  // namespace chronon::sender
