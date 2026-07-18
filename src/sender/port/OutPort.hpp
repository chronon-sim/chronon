// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "Connection.hpp"
#include "Port.hpp"
#include "PortDirectory.hpp"

namespace chronon::sender {

template <typename... Ports>
class PortTransaction;

/**
 * OutPort - Sends data to connected InPorts.
 *
 * Features:
 * - Connects to multiple InPorts with different delays
 * - Synchronous send for tick-based units
 *
 * Usage:
 *   OutPort<int> out{this, "out"};
 *
 *   // In tick() method. canSend() is only a preflight check; callers
 *   // must still handle send() returning false.
 *   if (out.canSend() && out.send(42)) {
 *       // local state can now advance
 *   }
 */
template <typename T>
class OutPort : public PortBase {
public:
    using value_type = T;

    /**
     * Create an output port.
     *
     * Automatically registers with PortDirectory when owner's TreeNode is set.
     *
     * @param owner The unit that owns this port
     * @param name The port name (for debugging)
     * @param per_cycle_capacity Max sends per cycle. Defaults to one registered-edge entry per
     *                           cycle. UNLIMITED_CAPACITY or 0 explicitly opts out.
     */
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    OutPort(Unit* owner, std::string name, size_t per_cycle_capacity = 1)
        : PortBase(owner, std::move(name)),
          per_cycle_capacity_(normalizeCapacity_(per_cycle_capacity)) {
        if (owner_) {
            addPortRegistrationToUnit(owner_, [this](const std::string& prefix) {
                std::string full_path = prefix + "." + name_;
                PortDirectory::instance().registerPort(
                    full_path, std::make_unique<OutPortHandle<T>>(this, owner_, name_, full_path));
            });
        }
    }

    /**
     * Connect to an input port with specified delay.
     *
     * @param to The destination input port
     * @param delay The delivery delay in cycles
     * @return The created connection
     */
    Connection<T>* connect(InPort<T>* to, uint32_t delay) {
        auto conn = std::make_unique<Connection<T>>(this, to, delay);
        conn->setDependencyOnlyTransport(dependency_only_transport_, dependency_only_headroom_);
        auto* ptr = conn.get();
        connections_.push_back(std::move(conn));
        invalidateTransaction_();
        return ptr;
    }

    /**
     * Send data through all connections.
     *
     * The current cycle is obtained from the owner unit.
     *
     * @param data The data to send
     * @return true if all sends succeeded, false if any destination was full.
     *         Multi-destination sends are preflighted before transfer so
     *         normal back-pressure failure is all-or-none.
     */
    [[nodiscard]]
    bool send(const T& data) {
        if (connections_.empty()) return true;

        if (per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }

        bool result;
        const uint64_t current = getCurrentCycle();
        if (shared_broadcast_) {
            result = publishTransparentBroadcast_(T{data}, current);
        } else if (dependency_only_transport_) {
            result = true;
        } else if (connections_.size() == 1) {
            result = connections_[0]->transfer(T{data}, current);
        } else {
            // Multi-connection fanout: preflight every destination before
            // mutating any queue. Prevents partial delivery when a later
            // destination is back-pressured.
            if (!allConnectionsCanTransfer_()) {
                return false;
            }
            result = true;
            for (auto& conn : connections_) {
                if (!conn->transfer(T{data}, current)) {
                    result = false;
                }
            }
        }

        if (result && per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            ++sent_this_cycle_;
        }
        return result;
    }

    /**
     * Send data with move semantics.
     *
     * @return true if all sends succeeded, false if any destination was full.
     *         Multi-destination sends are preflighted before transfer so
     *         normal back-pressure failure is all-or-none.
     */
    [[nodiscard]]
    bool send(T&& data) {
        if (connections_.empty()) return true;

        if (per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }

        bool result;
        const uint64_t current = getCurrentCycle();
        if (shared_broadcast_) {
            result = publishTransparentBroadcast_(std::move(data), current);
        } else if (dependency_only_transport_) {
            result = true;
        } else if (connections_.size() == 1) {
            result = connections_[0]->transfer(std::move(data), current);
        } else {
            if constexpr (!std::is_copy_constructible_v<T>) {
                // A move-only payload cannot be replicated across multiple
                // physical queues. Preserve the valid single-connection move
                // path above and report unsupported fanout as backpressure.
                return false;
            }
            // Multi-connection fanout: preflight before mutating any queue
            // (see overload above).
            if (!allConnectionsCanTransfer_()) {
                return false;
            }
            result = true;

            if constexpr (std::is_copy_constructible_v<T>) {
                for (size_t i = 0; i + 1 < connections_.size(); ++i) {
                    if (!connections_[i]->transfer(T{data}, current)) {
                        result = false;
                    }
                }
                if (!connections_.back()->transfer(std::move(data), current)) {
                    result = false;
                }
            }
        }

        if (result && per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            ++sent_this_cycle_;
        }
        return result;
    }

    [[nodiscard]]
    bool sendImmediate(const T& data) {
        return send(data);
    }

    [[nodiscard]]
    bool sendImmediate(T&& data) {
        return send(std::move(data));
    }

    /**
     * Check if all connected destinations can accept data.
     *
     * Use this to check before sending when back pressure is important.
     */
    bool canSend() const {
        if (per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }
        if (shared_broadcast_) return shared_broadcast_->canPublish();
        if (dependency_only_transport_) return true;
        for (const auto& conn : connections_) {
            if (!conn->canTransfer()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cancel all in-flight messages previously sent on this OutPort.
     *
     * Advances a per-connection cancellation epoch. Messages already
     * enqueued at destination ports will be dropped on receive.
     */
    void cancelInFlight() {
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        invalidateTransaction_();
#endif
        if (dependency_only_transport_ && !shared_broadcast_) return;
        for (auto& conn : connections_) {
            conn->cancelInFlight();
        }
    }

    const std::vector<std::unique_ptr<Connection<T>>>& connections() const { return connections_; }
    bool isConnected() const { return !connections_.empty(); }
    size_t connectionCount() const { return connections_.size(); }

    /// Disable physical payload transport while retaining all connections as
    /// scheduler dependency edges. Configure this before simulation
    /// initialization. send()/canSend() still enforce the OutPort's per-cycle
    /// capacity without walking the fanout. A finite cross_thread_headroom is
    /// exported through every connection for scheduler reverse dependencies.
    void setDependencyOnlyTransport(
        bool enabled = true,
        size_t cross_thread_headroom = std::numeric_limits<size_t>::max()) noexcept {
        invalidateTransaction_();
        dependency_only_transport_ = enabled;
        dependency_only_headroom_ =
            enabled ? cross_thread_headroom : std::numeric_limits<size_t>::max();
        for (auto& conn : connections_) {
            conn->setDependencyOnlyTransport(enabled, dependency_only_headroom_);
        }
    }

    bool dependencyOnlyTransport() const noexcept { return dependency_only_transport_; }

    [[nodiscard]] bool transparentBroadcastCapacityEligible(size_t headroom_cycles) const noexcept {
        return !dependency_only_transport_ && connections_.size() >= 2 &&
               per_cycle_capacity_ != UNLIMITED_CAPACITY &&
               detail::SharedBroadcastTransport<T>::automaticAllocationEligible(
                   headroom_cycles, per_cycle_capacity_);
    }

    /// Install a Port-internal one-write/many-reader transport. TickSimulation
    /// calls this only after validating the complete connected component, so
    /// every destination InPort switches atomically from physical queues to
    /// shared replay while the declared connections remain dependency edges.
    bool enableTransparentBroadcast(size_t headroom_cycles) {
        if (shared_broadcast_) return true;
        if (!transparentBroadcastCapacityEligible(headroom_cycles)) {
            return false;
        }
        for (const auto& connection : connections_) {
            if (!connection->transparentBroadcastEligible(headroom_cycles)) {
                return false;
            }
        }

        auto transport = std::make_unique<detail::SharedBroadcastTransport<T>>(headroom_cycles,
                                                                               per_cycle_capacity_);
        for (auto& connection : connections_) {
            connection->attachTransparentBroadcast(transport.get());
        }
        shared_broadcast_ = std::move(transport);
        invalidateTransaction_();
        return true;
    }

    [[nodiscard]] bool transparentBroadcastEnabled() const noexcept {
        return shared_broadcast_ != nullptr;
    }

    /// Find the first connection targeting @p to, or nullptr if not connected.
    Connection<T>* connectionTo(const InPort<T>* to) noexcept {
        for (auto& conn : connections_) {
            if (conn->to() == to) {
                return conn.get();
            }
        }
        return nullptr;
    }

    /// Find the first connection targeting @p to, or nullptr if not connected.
    const Connection<T>* connectionTo(const InPort<T>* to) const noexcept {
        for (const auto& conn : connections_) {
            if (conn->to() == to) {
                return conn.get();
            }
        }
        return nullptr;
    }

    /// Set the per-cycle send capacity at runtime (UNLIMITED_CAPACITY = unlimited).
    /// Passing 0 is accepted as the legacy unlimited spelling.
    void setPerCycleCapacity(size_t cap) {
        const size_t normalized = normalizeCapacity_(cap);
        if (normalized != per_cycle_capacity_) {
            per_cycle_capacity_ = normalized;
            invalidateTransaction_();
        }
    }

    /// @return Max sends per cycle (UNLIMITED_CAPACITY = unlimited).
    size_t perCycleCapacity() const { return per_cycle_capacity_; }

    size_t sentThisCycle() const {
        if (per_cycle_capacity_ == UNLIMITED_CAPACITY) return 0;
        updateCycleCounter_();
        return sent_this_cycle_;
    }

    /// @return Remaining sends, or UNLIMITED_CAPACITY if unlimited.
    size_t remainingThisCycle() const {
        if (per_cycle_capacity_ == UNLIMITED_CAPACITY) return UNLIMITED_CAPACITY;
        updateCycleCounter_();
        return (sent_this_cycle_ < per_cycle_capacity_) ? (per_cycle_capacity_ - sent_this_cycle_)
                                                        : 0;
    }

    /// Read the owning Unit's localCycle. Public so Connection<T> (which
    /// holds a back-reference to its source OutPort) can drive its own
    /// pushes_this_cycle_ reset off the producer's tick boundary.
    uint64_t getCurrentCycle() const;

private:
    template <typename... Ports>
    friend class PortTransaction;

    struct PackedTransactionState {
        static constexpr uint64_t MASK = (uint64_t{1} << 56) - 1;

        [[nodiscard]] uint64_t load() const noexcept {
            uint64_t value = 0;
            for (size_t i = 0; i < sizeof(bytes); ++i) {
                value |= static_cast<uint64_t>(bytes[i]) << (i * 8);
            }
            return value;
        }

        void store(uint64_t value) noexcept {
            value &= MASK;
            for (size_t i = 0; i < sizeof(bytes); ++i) {
                bytes[i] = static_cast<uint8_t>(value >> (i * 8));
            }
        }

        uint8_t bytes[7]{};
    };
    static_assert(sizeof(PackedTransactionState) == 7);

    struct TransactionReservation {
        uint64_t token = 0;
        uint64_t cycle = 0;
        size_t connection_count = 0;
        bool counted_cycle_capacity = false;
    };

    /**
     * Visit every physical bounded destination claimed by a transaction on
     * this port. Dependency-only and shared-broadcast edges do not consume a
     * destination queue slot and are intentionally omitted.
     */
    template <typename Visitor>
    void visitBoundedTransactionDestinations_(Visitor&& visitor) const {
        if (dependency_only_transport_ || shared_broadcast_) return;
        for (const auto& connection : connections_) {
            if (connection->dependencyOnlyTransport()) {
                continue;
            }
            const bool bounded_model_admission =
                connection->edgeAdmissionCapacity_() != UNLIMITED_CAPACITY;
            const bool bounded_shared_transport =
                !connection->hasThreadQueueId() &&
                connection->to()->storageCapacity() != UNLIMITED_CAPACITY;
            if (!bounded_model_admission && !bounded_shared_transport) continue;
            std::forward<Visitor>(visitor)(static_cast<const void*>(connection->to()));
        }
    }

    [[nodiscard]] bool transactionPayloadSupported_() const noexcept {
        if constexpr (!std::is_nothrow_move_constructible_v<T> ||
                      !std::is_nothrow_move_assignable_v<T>) {
            return connections_.empty() || dependency_only_transport_;
        }
        if (!shared_broadcast_ && !dependency_only_transport_ && connections_.size() > 1) {
            if constexpr (!std::is_nothrow_copy_constructible_v<T>) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool tryReserveTransaction_(TransactionReservation& reservation) {
        uint64_t transaction_state = transaction_state_.load();
        if ((transaction_state & 1U) != 0 || !transactionPayloadSupported_()) {
            return false;
        }

        const uint64_t current = getCurrentCycle();
        if (!connections_.empty() && per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }

        size_t connection_count = 0;
        if (!connections_.empty() && !shared_broadcast_ && !dependency_only_transport_) {
            size_t claimed = 0;
            for (; claimed < connections_.size(); ++claimed) {
                if (!connections_[claimed]->tryReserveTransfer_(current)) {
                    while (claimed != 0) {
                        connections_[--claimed]->releaseReservedTransfer_(current);
                    }
                    return false;
                }
            }
            connection_count = claimed;
        } else if (shared_broadcast_ && !shared_broadcast_->canPublish()) {
            return false;
        }

        bool counted_cycle_capacity = false;
        if (!connections_.empty() && per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            ++sent_this_cycle_;
            counted_cycle_capacity = true;
        }

        transaction_state = (transaction_state + 1) & PackedTransactionState::MASK;
        transaction_state_.store(transaction_state);
        reservation = TransactionReservation{.token = transaction_state,
                                             .cycle = current,
                                             .connection_count = connection_count,
                                             .counted_cycle_capacity = counted_cycle_capacity};
        return true;
    }

    [[nodiscard]] bool transactionReservationValid_(
        const TransactionReservation& reservation) const {
        if (reservation.token == 0 || transaction_state_.load() != reservation.token ||
            getCurrentCycle() != reservation.cycle) {
            return false;
        }

        if (reservation.counted_cycle_capacity) {
            updateCycleCounter_();
            if (last_counted_cycle_ != reservation.cycle || sent_this_cycle_ == 0 ||
                per_cycle_capacity_ == UNLIMITED_CAPACITY ||
                sent_this_cycle_ > per_cycle_capacity_) {
                return false;
            }
        }

        if (reservation.connection_count != 0) {
            for (size_t i = 0; i < reservation.connection_count; ++i) {
                if (!connections_[i]->transactionReservationValid_(reservation.cycle)) {
                    return false;
                }
            }
        } else if (shared_broadcast_ && !shared_broadcast_->canPublish()) {
            return false;
        }
        return true;
    }

    void releaseTransaction_(TransactionReservation& reservation) noexcept {
        if (reservation.token == 0) return;
        if (reservation.connection_count != 0) {
            for (size_t i = 0; i < reservation.connection_count; ++i) {
                connections_[i]->releaseReservedTransfer_(reservation.cycle);
            }
        }
        if (reservation.counted_cycle_capacity && last_counted_cycle_ == reservation.cycle &&
            sent_this_cycle_ != 0) {
            --sent_this_cycle_;
        }
        finishTransaction_();
        reservation.token = 0;
    }

    void commitTransaction_(TransactionReservation& reservation, T&& data) {
        if (connections_.empty()) {
            finishTransaction_();
            reservation.token = 0;
            return;
        }

        const uint64_t current = reservation.cycle;
        if (shared_broadcast_) {
            if (!publishTransparentBroadcast_(std::move(data), current)) {
                std::terminate();
            }
        } else if (!dependency_only_transport_) {
            if (connections_.size() == 1) {
                connections_.front()->commitReservedTransfer_(std::move(data), current);
            } else {
                if constexpr (std::is_copy_constructible_v<T>) {
                    for (size_t i = 0; i + 1 < connections_.size(); ++i) {
                        connections_[i]->commitReservedTransfer_(T{data}, current);
                    }
                    connections_.back()->commitReservedTransfer_(std::move(data), current);
                } else {
                    std::terminate();
                }
            }
        }

        finishTransaction_();
        reservation.token = 0;
    }

    static constexpr size_t normalizeCapacity_(size_t cap) noexcept {
        return cap == 0 ? UNLIMITED_CAPACITY : cap;
    }

    void invalidateTransaction_() noexcept {
        transaction_state_.store((transaction_state_.load() + 2) & PackedTransactionState::MASK);
    }

    void finishTransaction_() noexcept {
        const uint64_t state = transaction_state_.load();
        if ((state & 1U) != 0) {
            transaction_state_.store((state + 1) & PackedTransactionState::MASK);
        }
    }

    bool allConnectionsCanTransfer_() const {
        for (const auto& conn : connections_) {
            if (!conn->canTransfer()) {
                return false;
            }
        }
        return true;
    }

    template <typename U>
    bool publishTransparentBroadcast_(U&& data, uint64_t current_cycle) {
        if (!shared_broadcast_->publish(std::forward<U>(data), current_cycle, 1)) {
            return false;
        }
        if (last_broadcast_wakeup_cycle_ != current_cycle) {
            last_broadcast_wakeup_cycle_ = current_cycle;
            for (const auto& connection : connections_) {
                wakeUnitAt(connection->destination(), current_cycle + 1);
            }
        }
        return true;
    }

    void updateCycleCounter_() const {
        uint64_t current = getCurrentCycle();
        if (current != last_counted_cycle_) {
            sent_this_cycle_ = 0;
            last_counted_cycle_ = current;
        }
    }

    std::vector<std::unique_ptr<Connection<T>>> connections_;
    size_t per_cycle_capacity_ = 1;
    mutable size_t sent_this_cycle_ = 0;
    mutable uint64_t last_counted_cycle_ = 0;
    std::unique_ptr<detail::SharedBroadcastTransport<T>> shared_broadcast_;
    uint64_t last_broadcast_wakeup_cycle_ = std::numeric_limits<uint64_t>::max();
    bool dependency_only_transport_ = false;
    // Producer-owned transaction state occupies the existing seven-byte
    // alignment gap before dependency_only_headroom_. Even values are idle;
    // odd values identify the sole active claim. Ordinary send/canSend never
    // read these bytes.
    PackedTransactionState transaction_state_{};
    size_t dependency_only_headroom_ = std::numeric_limits<size_t>::max();
};

template <typename T>
PortBase* OutPortHandle<T>::portBase() const {
    return port_;
}

/**
 * Connect this output port to an input port handle.
 *
 * Performs type-safe connection using OutPort::connect() directly,
 * without requiring Simulation in the dependency chain.
 */
template <typename T>
ConnectionBase* OutPortHandle<T>::connectTo(IPortHandle* other, uint32_t delay) {
    if (!other || !other->isInPort()) {
        return nullptr;
    }
    // Type safety is verified by caller (PortBindingRegistry::bind)
    auto* in_handle = static_cast<InPortHandle<T>*>(other);
    return port_->connect(in_handle->port(), delay);
}

}  // namespace chronon::sender
