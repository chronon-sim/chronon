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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "../../tree/TreeNode.hpp"
#include "../port/Port.hpp"
#include "Fwd.hpp"

namespace chronon::sender {

class TickSimulation;

enum class UnitState { Created, Initialized };

namespace detail {

struct alignas(64) ActivitySchedulingState {
    void publish() noexcept {
        // A cluster leaf owns root; the global root leaves it null. Publishing
        // leaf first makes an acquire of root=true sufficient for leaf lookup.
        enabled.store(true, std::memory_order_release);
        if (root != nullptr) {
            root->store(true, std::memory_order_release);
        }
    }

    std::atomic<bool> enabled{false};
    std::atomic<bool>* root = nullptr;
};
static_assert(sizeof(ActivitySchedulingState) % 64 == 0);

}  // namespace detail

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

    static constexpr uint64_t NEVER_ACTIVE = std::numeric_limits<uint64_t>::max();

    /// Opt in to scheduler-controlled activity before the first tick.
    ///
    /// Units that may sleep after their first poll but need future port arrivals
    /// sent earlier in the same cycle to wake them should call this from their
    /// constructor. sleepUntil(), sleepForever(), and setTickInterval(N > 1)
    /// enable it automatically once they are used.
    void enableActivityScheduling() noexcept {
        enableActivityScheduling_();
        wake_tracking_enabled_.store(true, std::memory_order_release);
        setNextActiveCycleMin_(local_cycle_);
    }

    /// Request that the unit's tick body next runs no earlier than @p cycle.
    /// This is expressed in the global simulation cycle domain.
    void sleepUntil(uint64_t cycle) noexcept {
        enableActivityScheduling_();
        wake_tracking_enabled_.store(true, std::memory_order_release);
        activity_control_used_.store(true, std::memory_order_relaxed);
        setSleepTarget_(cycle);
    }

    /// Disable the tick body until an external wakeAt() or port arrival wakes it.
    void sleepForever() noexcept {
        enableActivityScheduling_();
        wake_tracking_enabled_.store(true, std::memory_order_release);
        activity_control_used_.store(true, std::memory_order_relaxed);
        setSleepTarget_(NEVER_ACTIVE);
    }

    /// Wake the unit no later than @p cycle. Safe for cross-thread producers.
    void wakeAt(uint64_t cycle) noexcept {
        std::lock_guard lock(pending_wake_mutex_);
        pending_wake_cycles_.insert(cycle);
        setNextActiveCycleMin_(cycle);
    }

    uint64_t nextActiveCycle() const noexcept {
        return next_active_cycle_.load(std::memory_order_acquire);
    }

    bool acceptsPortWakeups() const noexcept {
        return port_wake_enabled_.load(std::memory_order_acquire);
    }

    bool usesActivityScheduling() const noexcept {
        return port_wake_enabled_.load(std::memory_order_relaxed);
    }

    void setTickInterval(uint32_t interval) noexcept {
        const uint32_t normalized = interval == 0 ? 1 : interval;
        tick_interval_.store(normalized, std::memory_order_release);
        if (normalized > 1) {
            // Only a sleepUntil()/sleepForever() target should defer interval
            // ticking. An explicit future wakeAt() lowers next_active_cycle_
            // too, but it is an additional wake hint and must not suppress the
            // normal interval cadence.
            const bool has_sleep_target = activity_control_used_.load(std::memory_order_relaxed);
            wake_tracking_enabled_.store(true, std::memory_order_release);
            enableActivityScheduling_();
            if (!has_sleep_target) {
                setNextActiveCycleMin_(local_cycle_);
            }
        }
    }

    uint32_t tickInterval() const noexcept {
        return tick_interval_.load(std::memory_order_acquire);
    }

    bool shouldRunTickAt(uint64_t cycle) const noexcept {
        if (!port_wake_enabled_.load(std::memory_order_relaxed)) {
            return true;
        }
        const uint64_t next = next_active_cycle_.load(std::memory_order_acquire);
        if (cycle < next) {
            return false;
        }
        const uint32_t interval = tick_interval_.load(std::memory_order_acquire);
        return interval <= 1 || (cycle % interval) == 0;
    }

    uint64_t nextRunnableCycleAtOrAfter(uint64_t cycle) const noexcept {
        if (!port_wake_enabled_.load(std::memory_order_relaxed)) {
            return cycle;
        }
        uint64_t base = std::max(cycle, next_active_cycle_.load(std::memory_order_acquire));
        if (base == NEVER_ACTIVE) {
            return NEVER_ACTIVE;
        }

        const uint32_t interval = tick_interval_.load(std::memory_order_acquire);
        if (interval <= 1) {
            return base;
        }

        uint64_t rem = base % interval;
        if (rem == 0) {
            return base;
        }
        uint64_t delta = interval - rem;
        if (base > NEVER_ACTIVE - delta) {
            return NEVER_ACTIVE;
        }
        return base + delta;
    }

    /// Default mode. Eliminates atomic overhead (~80% of tight-loop time).
    void useFastCycleCounter() noexcept { use_atomic_cycle_ = false; }

    /// Use when localCycle() may be read from another thread. Not needed for
    /// lookahead — TickSimulation publishes progress explicitly via unit_progress_.
    void useAtomicCycleCounter() noexcept { use_atomic_cycle_ = true; }

    void registerPort(PortBase* port) { ports_.push_back(port); }
    const std::vector<PortBase*>& ports() const noexcept { return ports_; }

    /** Initialization-only registration for ports with receiver cycle work. */
    void registerCyclePreparedPort(PortBase* port) {
        if (!port) return;
        if (!cycle_prepared_ports_) {
            cycle_prepared_ports_ = std::make_unique<std::vector<PortBase*>>();
        }
        if (std::find(cycle_prepared_ports_->begin(), cycle_prepared_ports_->end(), port) ==
            cycle_prepared_ports_->end()) {
            cycle_prepared_ports_->push_back(port);
        }
    }

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

    void beginActiveTick_() noexcept {
        if (!port_wake_enabled_.load(std::memory_order_relaxed)) {
            return;
        }
        activity_control_used_.store(false, std::memory_order_relaxed);
        consumeCyclesThrough_(next_active_cycle_, local_cycle_);
        if (wake_tracking_enabled_.load(std::memory_order_relaxed)) {
            consumePendingWakesThrough_(local_cycle_);
        }
    }

    void finishActiveTick_() noexcept {
        if (!port_wake_enabled_.load(std::memory_order_relaxed)) {
            return;
        }
        if (!activity_control_used_.load(std::memory_order_relaxed)) {
            setNextActiveCycleMin_(local_cycle_ + 1);
        }
    }

    /**
     * One predictable null check for ordinary Units. The loop is kept out of
     * line and exists only for Units owning a bounded MPSC destination.
     */
    [[gnu::always_inline]] inline void prepareCyclePorts_() {
        if (cycle_prepared_ports_ == nullptr) [[likely]] {
            return;
        }
        prepareCyclePortsSlow_();
    }

private:
    void bindActivitySchedulingState_(detail::ActivitySchedulingState* state) noexcept {
        activity_scheduling_ = state;
        if (port_wake_enabled_.load(std::memory_order_relaxed)) {
            publishActivityScheduling_();
        }
    }

    [[gnu::noinline]] void prepareCyclePortsSlow_() {
        for (PortBase* port : *cycle_prepared_ports_) {
            port->prepareConsumerCycle(local_cycle_);
        }
    }

    void enableActivityScheduling_() noexcept {
        port_wake_enabled_.store(true, std::memory_order_release);
        publishActivityScheduling_();
    }

    void publishActivityScheduling_() noexcept {
        if (activity_scheduling_ != nullptr) {
            activity_scheduling_->publish();
        }
    }

    void setSleepTarget_(uint64_t cycle) noexcept {
        std::lock_guard lock(pending_wake_mutex_);
        uint64_t target = std::min(cycle, earliestPendingWakeLocked_());
        target = std::min(target, earliestPortArrival_());
        next_active_cycle_.store(target, std::memory_order_release);
    }

    void setNextActiveCycleMin_(uint64_t cycle) noexcept {
        setCycleMin_(next_active_cycle_, cycle);
    }

    static void setCycleMin_(std::atomic<uint64_t>& value, uint64_t cycle) noexcept {
        uint64_t current = value.load(std::memory_order_relaxed);
        while (cycle < current &&
               !value.compare_exchange_weak(current, cycle, std::memory_order_release,
                                            std::memory_order_relaxed)) {
        }
    }

    static void consumeCyclesThrough_(std::atomic<uint64_t>& value, uint64_t cycle) noexcept {
        uint64_t current = value.load(std::memory_order_relaxed);
        while (current <= cycle &&
               !value.compare_exchange_weak(current, NEVER_ACTIVE, std::memory_order_release,
                                            std::memory_order_relaxed)) {
        }
    }

    uint64_t earliestPendingWakeLocked_() const noexcept {
        return pending_wake_cycles_.empty() ? NEVER_ACTIVE : *pending_wake_cycles_.begin();
    }

    uint64_t earliestPortArrival_() const {
        uint64_t earliest = NEVER_ACTIVE;
        for (const auto* port : ports_) {
            if (auto arrival = port->minArrivalCycle()) {
                earliest = std::min(earliest, *arrival);
            }
        }
        return earliest;
    }

    void consumePendingWakesThrough_(uint64_t cycle) noexcept {
        std::lock_guard lock(pending_wake_mutex_);
        while (!pending_wake_cycles_.empty() && *pending_wake_cycles_.begin() <= cycle) {
            pending_wake_cycles_.erase(pending_wake_cycles_.begin());
        }
    }

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
    std::atomic<uint64_t> next_active_cycle_{NEVER_ACTIVE};
    std::mutex pending_wake_mutex_;
    std::multiset<uint64_t> pending_wake_cycles_;
    std::atomic<uint32_t> tick_interval_{1};
    std::atomic<bool> activity_control_used_{false};
    /// Single source of truth for activity scheduling and port wakeup opt-in.
    /// Owner-side relaxed reads require only a non-RMW load; cross-thread port
    /// producers use acceptsPortWakeups()'s acquire load.
    std::atomic<bool> port_wake_enabled_{false};
    /// Hierarchical monotone summary bound by TickSimulation. One indirection
    /// publishes the cache-line-isolated cluster leaf before the global root.
    detail::ActivitySchedulingState* activity_scheduling_ = nullptr;
    std::atomic<bool> wake_tracking_enabled_{false};
    uint32_t id_;
    std::vector<PortBase*> ports_;
    /// Lazily allocated: the common Unit has no receiver cycle hook.
    std::unique_ptr<std::vector<PortBase*>> cycle_prepared_ports_;
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

/// Records a PortBase* on its owning Unit for wakeup/readiness bookkeeping.
inline void recordPortOnOwnerUnit(Unit* unit, PortBase* port) {
    if (unit && port) {
        unit->registerPort(port);
    }
}

/// Registers a receiver-cycle hook without exposing Unit's definition to Port.hpp.
inline void recordCyclePreparedPortOnOwnerUnit(Unit* unit, PortBase* port) {
    if (unit && port) {
        unit->registerCyclePreparedPort(port);
    }
}

inline void wakeUnitAt(Unit* unit, uint64_t cycle) {
    if (unit && unit->acceptsPortWakeups()) {
        unit->wakeAt(cycle);
    }
}

}  // namespace chronon::sender
