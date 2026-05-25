// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "Connection.hpp"
#include "Port.hpp"
#include "PortDirectory.hpp"

namespace chronon::sender {

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
    /**
     * Create an output port.
     *
     * Automatically registers with PortDirectory when owner's TreeNode is set.
     *
     * @param owner The unit that owns this port
     * @param name The port name (for debugging)
     * @param per_cycle_capacity Max sends per cycle (UNLIMITED_CAPACITY = unlimited, default).
     *                           Passing 0 is accepted as the legacy unlimited spelling.
     */
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    OutPort(Unit* owner, std::string name, size_t per_cycle_capacity = UNLIMITED_CAPACITY)
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
        auto* ptr = conn.get();
        connections_.push_back(std::move(conn));
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
    [[nodiscard(
        "send() reports whether the message was accepted; "
        "retain local state and retry on false")]]
    bool send(const T& data) {
        if (connections_.empty()) return true;

        if (per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }

        bool result;
        if (connections_.size() == 1) {
            result = connections_[0]->transfer(T{data}, getCurrentCycle());
        } else {
            // Multi-connection fanout: preflight every destination before
            // mutating any queue. Prevents partial delivery when a later
            // destination is back-pressured.
            if (!allConnectionsCanTransfer_()) {
                return false;
            }
            uint64_t current = getCurrentCycle();
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
    [[nodiscard(
        "send() reports whether the message was accepted; "
        "retain local state and retry on false")]]
    bool send(T&& data) {
        if (connections_.empty()) return true;

        if (per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            updateCycleCounter_();
            if (sent_this_cycle_ >= per_cycle_capacity_) return false;
        }

        bool result;
        if (connections_.size() == 1) {
            result = connections_[0]->transfer(std::move(data), getCurrentCycle());
        } else {
            // Multi-connection fanout: preflight before mutating any queue
            // (see overload above).
            if (!allConnectionsCanTransfer_()) {
                return false;
            }
            uint64_t current = getCurrentCycle();
            result = true;

            for (size_t i = 0; i + 1 < connections_.size(); ++i) {
                if (!connections_[i]->transfer(T{data}, current)) {
                    result = false;
                }
            }
            if (!connections_.back()->transfer(std::move(data), current)) {
                result = false;
            }
        }

        if (result && per_cycle_capacity_ != UNLIMITED_CAPACITY) {
            ++sent_this_cycle_;
        }
        return result;
    }

    [[nodiscard(
        "sendImmediate() is a compatibility alias; use send() and handle false by retrying")]]
    bool sendImmediate(const T& data) {
        return send(data);
    }

    [[nodiscard(
        "sendImmediate() is a compatibility alias; use send() and handle false by retrying")]]
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
        for (auto& conn : connections_) {
            conn->cancelInFlight();
        }
    }

    const std::vector<std::unique_ptr<Connection<T>>>& connections() const { return connections_; }
    bool isConnected() const { return !connections_.empty(); }
    size_t connectionCount() const { return connections_.size(); }

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
    void setPerCycleCapacity(size_t cap) { per_cycle_capacity_ = normalizeCapacity_(cap); }

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
    static constexpr size_t normalizeCapacity_(size_t cap) noexcept {
        return cap == 0 ? UNLIMITED_CAPACITY : cap;
    }

    bool allConnectionsCanTransfer_() const {
        for (const auto& conn : connections_) {
            if (!conn->canTransfer()) {
                return false;
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
    size_t per_cycle_capacity_ = UNLIMITED_CAPACITY;
    mutable size_t sent_this_cycle_ = 0;
    mutable uint64_t last_counted_cycle_ = 0;
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
