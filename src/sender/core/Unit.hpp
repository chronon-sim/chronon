// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Base class for simulation units. Units communicate through typed ports
/// with configurable delays.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../../tree/TreeNode.hpp"
#include "../port/Port.hpp"
#include "Fwd.hpp"

namespace chronon::sender {

class TickSimulation;

enum class UnitState { Created, Initialized };

/**
 * Base class for simulation components.
 *
 * A Unit owns input/output ports and implements tick() to define per-cycle
 * behavior. The scheduler uses dependency analysis on connections to
 * determine safe parallelization.
 *
 * @code
 * class Producer : public TickableUnit {
 *     OutPort<int> out{this, "out"};
 *     int count_ = 0;
 * public:
 *     Producer() : TickableUnit("producer") {}
 *     void tick() override {
 *         if (out.canSend()) out.send(count_++);
 *     }
 *     bool isCompleted() const override { return count_ >= 100; }
 * };
 * @endcode
 */
class Unit {
public:
    explicit Unit(std::string name) : name_(std::move(name)), state_(UnitState::Created), id_(0) {
        // Fixed buffer copy avoids touching std::string in async-signal-safe
        // crash reporting paths.
        crash_name_len_ = static_cast<uint8_t>(std::min(name_.size(), sizeof(crash_name_) - 1));
        std::memcpy(crash_name_, name_.c_str(), crash_name_len_);
        crash_name_[crash_name_len_] = '\0';
    }

    virtual ~Unit() = default;

    Unit(const Unit&) = delete;
    Unit& operator=(const Unit&) = delete;
    Unit(Unit&&) = delete;
    Unit& operator=(Unit&&) = delete;

    /// Called after all connections are made, before run() starts.
    virtual void initialize() {}

    /// Called after run() completes or simulation is stopped.
    virtual void finalize() {}

    const std::string& name() const noexcept { return name_; }
    const char* crashName() const noexcept { return crash_name_; }
    uint8_t crashNameLen() const noexcept { return crash_name_len_; }
    uint32_t id() const noexcept { return id_; }
    UnitState state() const noexcept { return state_; }

    /// Fast non-atomic read. Cross-thread lookahead visibility uses
    /// TickSimulation::unit_progress_ atomics, not this counter.
    uint64_t localCycle() const noexcept { return local_cycle_; }

    /// Default mode. Eliminates atomic overhead (~80% of tight-loop time).
    void useFastCycleCounter() noexcept { use_atomic_cycle_ = false; }

    /// Use when localCycle() may be read from another thread. Not needed for
    /// lookahead — TickSimulation publishes progress explicitly via unit_progress_.
    void useAtomicCycleCounter() noexcept { use_atomic_cycle_ = true; }

    void registerPort(PortBase* port) { ports_.push_back(port); }
    const std::vector<PortBase*>& ports() const noexcept { return ports_; }

    /// Triggers registration of all pending ports to PortDirectory.
    void setTreeNode(tree::TreeNode* node) {
        tree_node_ = node;
        registerAllPendingPorts();
    }

    tree::TreeNode* treeNode() const noexcept { return tree_node_; }

    /// Returns the tree path if a TreeNode is set, else the unit name.
    std::string fullPath() const;

    /**
     * Add a port registration callback to be invoked when setTreeNode() runs.
     * Called automatically by Port constructors for YAML-driven discovery.
     */
    void addPendingPortRegistration(std::function<void(const std::string&)> registration) {
        pending_port_registrations_.push_back(std::move(registration));
    }

protected:
    friend class TickSimulation;

    void setId(uint32_t id) { id_ = id; }

    void setLocalCycle(uint64_t cycle) {
        local_cycle_ = cycle;
        if (use_atomic_cycle_) {
            local_cycle_atomic_.store(cycle, std::memory_order_release);
        }
    }

    /// Fast path: ~0.3ns increment. Slow path (atomic): ~15ns.
    void advanceLocalCycle(uint64_t delta = 1) {
        local_cycle_ += delta;
        if (use_atomic_cycle_) {
            local_cycle_atomic_.store(local_cycle_, std::memory_order_release);
        }
    }

    /// Only valid when useAtomicCycleCounter() was called. Prefer
    /// TickSimulation::unit_progress_ for lookahead.
    uint64_t localCycleAtomic() const noexcept {
        return local_cycle_atomic_.load(std::memory_order_acquire);
    }

private:
    void registerAllPendingPorts() {
        std::string prefix = fullPath();
        for (auto& reg : pending_port_registrations_) {
            reg(prefix);
        }
        pending_port_registrations_.clear();
    }

    std::string name_;
    char crash_name_[64] = {};    ///< fixed buffer for async-signal-safe crash reporting
    uint8_t crash_name_len_ = 0;  ///< precomputed length for memcpy
    UnitState state_;
    uint64_t local_cycle_ = 0;
    std::atomic<uint64_t> local_cycle_atomic_{0};  ///< mirror used only when use_atomic_cycle_
    bool use_atomic_cycle_ = false;
    uint32_t id_;
    std::vector<PortBase*> ports_;
    tree::TreeNode* tree_node_ = nullptr;
    std::vector<std::function<void(const std::string&)>> pending_port_registrations_;
};

template <typename T>
Unit* Connection<T>::source() const noexcept {
    return from_->owner();
}

template <typename T>
Unit* Connection<T>::destination() const noexcept {
    return to_->owner();
}

template <typename T>
uint64_t InPort<T>::getCurrentCycle() const {
    if (owner_) {
        return owner_->localCycle();
    }
    return 0;
}

template <typename T>
uint64_t OutPort<T>::getCurrentCycle() const {
    if (owner_) {
        return owner_->localCycle();
    }
    return 0;
}

inline std::string Unit::fullPath() const {
    if (tree_node_) {
        return tree_node_->path();
    }
    return name_;
}

/// Defined here (free function) to avoid the Port.hpp → Unit.hpp circular
/// dependency: Port constructors only see a forward-declared Unit.
inline void addPortRegistrationToUnit(Unit* unit,
                                      std::function<void(const std::string&)> registration) {
    if (unit) {
        unit->addPendingPortRegistration(std::move(registration));
    }
}

/// Records a PortBase* on its owning Unit so TickableUnit::executeTick can
/// walk the ports to drive consumer-tick-driven MPSC arbitration.
inline void recordPortOnOwnerUnit(Unit* unit, PortBase* port) {
    if (unit && port) {
        unit->registerPort(port);
    }
}

}  // namespace chronon::sender
