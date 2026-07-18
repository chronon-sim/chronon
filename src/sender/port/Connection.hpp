// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#ifndef CHRONON_ENABLE_OUTPORT_CANCELLATION
#define CHRONON_ENABLE_OUTPORT_CANCELLATION 1
#endif

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "MessageQueue.hpp"
#include "SharedBroadcastTransport.hpp"

namespace chronon::sender {

template <typename T>
class InPort;
template <typename T>
class OutPort;
class Unit;
class IMultiProducerPort;
void wakeUnitAt(Unit* unit, uint64_t cycle);

/**
 * ConnectionBase - Type-erased base class for connections.
 *
 * Enables storing heterogeneous connections in containers.
 */
class ConnectionBase {
public:
    virtual ~ConnectionBase() = default;

    virtual uint32_t delay() const noexcept = 0;
    virtual Unit* source() const noexcept = 0;
    virtual Unit* destination() const noexcept = 0;
    virtual void* sourcePortPtr() const noexcept { return nullptr; }
    virtual void* destPortPtr() const noexcept = 0;

    /// True when this edge can participate in the automatic delay-one
    /// shared-broadcast transport without changing model-visible semantics.
    virtual bool transparentBroadcastEligible(size_t headroom_cycles) const noexcept {
        (void)headroom_cycles;
        return false;
    }

    /// Enable the shared transport for every connection owned by this edge's
    /// source OutPort. Called once per eligible source during initialize().
    virtual bool enableTransparentBroadcastForSource(size_t headroom_cycles) {
        (void)headroom_cycles;
        return false;
    }

    /// Keep this edge in the scheduler dependency graph without transporting
    /// payloads. Intended for model-owned shared fabrics that carry data once
    /// while declared connections continue to describe ordering and delay.
    /// A finite cross_thread_headroom lets the scheduler constrain producer
    /// run-ahead to the external transport's storage.
    virtual void setDependencyOnlyTransport(
        bool enabled,
        size_t cross_thread_headroom = std::numeric_limits<size_t>::max()) noexcept = 0;
    virtual bool dependencyOnlyTransport() const noexcept = 0;

    /**
     * Optimize destination port for same-thread access.
     *
     * Switches InPort to use the synchronization-free SingleThreadMessageQueue.
     * Call this during initialization when both source and destination
     * are determined to be on the same worker. Epoch-free execution enables
     * cycle-strict admission because separate local clusters can temporarily
     * execute out of their normal sweep order.
     *
     * This keeps intra-cluster registered edges on the cheapest storage.
     */
    virtual void optimizeForSameThread(bool cycle_strict_admission = false) = 0;

    /**
     * Optimize destination port for cross-thread SPSC access.
     *
     * Switches InPort to use lock-free LockFreeMessageQueue.
     * Call this during initialization when there is exactly ONE source thread
     * writing to the destination port on a different thread.
     */
    virtual void optimizeForSPSC() = 0;

    /**
     * Optimize destination port for cross-thread MPSC access.
     *
     * Switches InPort to use MultiProducerQueueAdapter with one producer
     * queue per Connection.
     */
    virtual void optimizeForMPSC() = 0;

    /// Configure the registered edge modeled by this connection. `capacity`
    /// is the number of entries the producer may have in flight on this edge;
    /// `rate` is the maximum entries this edge can accept per producer cycle.
    virtual void configureRegisteredEdge(std::optional<size_t> capacity,
                                         std::optional<size_t> rate) = 0;

    /// Try to grow physical lock-free buffers enough for epoch-free run-ahead
    /// when the model-visible registered edge is unbounded. A finite declared
    /// edge capacity is semantic backpressure and must not be bypassed by
    /// resizing the storage ring.
    virtual bool ensureEpochFreeHeadroom(uint32_t max_lookahead_cycles) = 0;

    /**
     * Register a stable producer key for MPSC mode.
     *
     * @param thread_id Stable producer key. TickSimulation passes conn_id + 1.
     * @return Queue ID for this producer key, or SIZE_MAX on failure
     */
    virtual size_t registerProducerThread(size_t thread_id) = 0;

    /**
     * Set the thread queue ID for multi-producer mode.
     *
     * Called during initialization when the destination InPort is in
     * multi-producer mode.
     *
     * @param queue_id The queue ID for this connection
     */
    virtual void setThreadQueueId(size_t queue_id) = 0;

    /// True if this connection uses thread-specific queue (MPSC mode).
    virtual bool hasThreadQueueId() const noexcept = 0;

    /**
     * Stable connection identifier assigned at simulation build time.
     *
     * Equal to this connection's index in TickSimulation::connections_.
     * Used by MultiProducerQueueAdapter as a cross-num_workers-stable
     * tiebreaker in the k-way merge, replacing the partition-dependent
     * queue_id. The value is deterministic given a fixed topology,
     * regardless of thread count / cluster assignment.
     */
    virtual void setConnId(uint32_t conn_id) noexcept = 0;
    virtual uint32_t connId() const noexcept = 0;

    /**
     * Producer-owned pushes already claimed in @p cycle.
     *
     * Port transactions use this cold-path view to detect an ordinary send
     * through another OutPort owned by the same Unit. Every connection in the
     * queried producer group has the same sole writer, so this remains a
     * synchronization-free read of the existing per-cycle counter.
     */
    virtual size_t transactionPushesAt(uint64_t cycle) const noexcept = 0;

    /**
     * Cancel all in-flight messages on this connection.
     *
     * Advances the cancellation epoch so pending messages are dropped
     * when the receiver tries to consume them.
     */
    virtual void cancelInFlight() noexcept = 0;

    /**
     * Max producer run-ahead (in cycles) this connection's cross-thread buffer
     * (direct MPSC lane or SPSC lock-free ring) can absorb while the epoch-free
     * path keeps results identical to the reference: roughly
     * `threshold / rate - delay + 1`, where `threshold` is the entry count at
     * which transfer() stops accepting (the ring, or the InPort capacity if
     * smaller), `rate` is the source's per-cycle send cap, and `delay` accounts
     * for not-yet-due entries the consumer cannot drain. Returns SIZE_MAX for
     * connections with no bounded cross-thread ring (same-thread / unbounded)
     * and 0 when no finite capacity dependency is provably safe. Used to gate
     * the epoch-free lookahead path, which removes the per-epoch drain.
     */
    virtual size_t crossThreadHeadroom() const noexcept { return SIZE_MAX; }

    /**
     * Register this MPSC connection on its destination InPort and return
     * the InPort's type-erased MPSC metadata interface. TickSimulation uses it
     * for progress-coverage and physical-overflow validation without knowing
     * the Connection's message type. Returns nullptr outside MPSC mode.
     */
    virtual class IMultiProducerPort* registerOnDestMPSC() = 0;

    /// True if this is a zero-delay (tight) connection.
    bool isTight() const noexcept { return delay() == 0; }
};

namespace detail {

/**
 * Transaction control plane shared by one producer Unit's connections to one
 * destination InPort.
 *
 * Each instance and its mutable connection snapshots are cache-line aligned.
 * Only the source Unit's worker mutates them after topology construction, so
 * independent producers never share a writable line; no lock or atomic is
 * required.
 * Ordinary send/canSend do not consult this state.
 */
struct alignas(64) ProducerDestinationTransactionState {
    explicit ProducerDestinationTransactionState(Unit* owner) noexcept : producer(owner) {}

    [[nodiscard]] bool tryClaim(ConnectionBase* connection, uint64_t cycle) noexcept {
        if (claimant != nullptr || connection == nullptr) return false;

        for (auto& entry : connections) {
            entry.pushes_at_claim = entry.connection->transactionPushesAt(cycle);
            if (entry.connection == connection &&
                entry.pushes_at_claim == std::numeric_limits<size_t>::max()) {
                return false;
            }
        }
        claimant = connection;
        claim_cycle = cycle;
        return true;
    }

    [[nodiscard]] bool valid(const ConnectionBase* connection, uint64_t cycle) const noexcept {
        if (claimant != connection || claim_cycle != cycle) return false;
        for (const auto& entry : connections) {
            if (entry.connection->dependencyOnlyTransport()) continue;
            const size_t expected =
                entry.pushes_at_claim + (entry.connection == connection ? 1 : 0);
            if (entry.connection->transactionPushesAt(cycle) != expected) return false;
        }
        return true;
    }

    void release(const ConnectionBase* connection) noexcept {
        if (claimant == connection) claimant = nullptr;
    }

    struct alignas(64) ConnectionEntry {
        ConnectionBase* connection = nullptr;
        size_t pushes_at_claim = 0;
    };
    static_assert(sizeof(ConnectionEntry) == 64);

    Unit* producer = nullptr;
    std::vector<ConnectionEntry> connections;

private:
    ConnectionBase* claimant = nullptr;
    uint64_t claim_cycle = 0;
};

}  // namespace detail

/**
 * Connection - Typed connection between OutPort and InPort.
 *
 * Connections specify the communication delay:
 * - delay=0: Tight coupling, same-cycle delivery on acyclic paths
 * - delay>0: Loose coupling, future delivery (lookahead possible)
 *
 * Usage:
 *   auto conn = sim.connect(producer->out, consumer->in, 5);
 *   // Messages sent at cycle N arrive at cycle N+5
 */
template <typename T>
class Connection : public ConnectionBase {
public:
    using SharedBroadcast = detail::SharedBroadcastTransport<T>;
    using SharedBroadcastView = typename SharedBroadcast::View;
    using SharedBroadcastCursor = typename SharedBroadcast::ConsumerCursor;

    /**
     * Create a connection with specified delay.
     *
     * @param from Source output port
     * @param to Destination input port
     * @param delay Number of cycles for message delivery
     */
    Connection(OutPort<T>* from, InPort<T>* to, uint32_t delay)
        : from_(from), to_(to), delay_(delay) {
        if (to_) {
            to_->registerIncomingDelay(delay_);
            transaction_group_ =
                to_->registerTransactionProducer(from_ ? from_->owner() : nullptr, this);
        }
    }

    bool transparentBroadcastEligible(size_t headroom_cycles) const noexcept override {
        if constexpr (!std::is_copy_constructible_v<T>) {
            return false;
        }
        return from_ && to_ && delay_ == 1 && !dependency_only_transport_ &&
               !registered_capacity_.has_value() && !registered_rate_.has_value() &&
               to_->transparentBroadcastEligible() &&
               from_->transparentBroadcastCapacityEligible(headroom_cycles);
    }

    bool enableTransparentBroadcastForSource(size_t headroom_cycles) override {
        return from_ && from_->enableTransparentBroadcast(headroom_cycles);
    }

    void attachTransparentBroadcast(SharedBroadcast* transport) {
        if (!transport || shared_broadcast_) {
            throw std::logic_error("invalid transparent broadcast attachment");
        }
        shared_broadcast_ = transport;
        shared_broadcast_->registerConsumerCursor(&shared_cursor_);
        to_->registerSharedBroadcastConnection(this);
        // Eligible destinations are semantically unbounded. The segmented
        // transport grows instead of introducing a reverse headroom edge.
        setDependencyOnlyTransport(true, std::numeric_limits<size_t>::max());
    }

    [[nodiscard]] bool transparentBroadcastEnabled() const noexcept {
        return shared_broadcast_ != nullptr;
    }

    [[nodiscard]] std::optional<SharedBroadcastView> peekSharedBroadcast() const noexcept {
        if (!shared_broadcast_) return std::nullopt;
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        uint64_t head = shared_broadcast_->consumerSequence(shared_cursor_);
        const uint64_t cancel_before = shared_cancel_before_.load(std::memory_order_acquire);
        if (head < cancel_before) {
            head = cancel_before;
            shared_broadcast_->advance(shared_cursor_, head);
        }
#endif
        return shared_broadcast_->peek(shared_cursor_);
    }

    void popSharedBroadcast(uint64_t sequence) noexcept {
        if (!shared_broadcast_) return;
        const uint64_t head = shared_broadcast_->consumerSequence(shared_cursor_);
        if (head == sequence) {
            shared_broadcast_->advance(shared_cursor_, sequence + 1);
        }
    }

    void flushSharedBroadcast() noexcept {
        if (!shared_broadcast_) return;
        shared_broadcast_->advance(shared_cursor_, shared_broadcast_->publishedExclusive());
    }

    [[nodiscard]] size_t sharedBroadcastQueuedCount() const noexcept {
        if (!shared_broadcast_) return 0;
        const uint64_t head = shared_broadcast_->consumerSequence(shared_cursor_);
        const uint64_t tail = shared_broadcast_->publishedExclusive();
        return static_cast<size_t>(tail - std::min(head, tail));
    }

    /**
     * Transfer data through the connection.
     *
     * The data will arrive at the destination port after the configured delay.
     * Uses thread-specific queue if in multi-producer mode.
     *
     * @param data The data to transfer
     * @param send_cycle The current cycle when sending
     * @return true if transfer succeeded, false if destination full (back pressure)
     */
    bool transfer(T data, uint64_t send_cycle) {
        maybeResetPushesCycle_(send_cycle);
        if (dependency_only_transport_) {
            if (pushes_this_cycle_ >= edgeCycleRateLimit_()) {
                return false;
            }
            ++pushes_this_cycle_;
            return true;
        }
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        const uint64_t epoch_snapshot = cancel_epoch_.load(std::memory_order_acquire);
#endif
        const uint64_t arrive_cycle = send_cycle + delay_;
        // enqueue_cycle = sender's localCycle at push time. Every PortPolicy
        // uses it as the deterministic selective-flush scope boundary.
        if (thread_queue_id_ != SIZE_MAX) {
            // MPSC mode: publish directly to this Connection's SPSC ingress
            // lane. Unbounded InPorts consume the selected slot in place;
            // bounded InPorts move ready entries into a receiver-owned shared
            // FIFO with deterministic aggregate admission.
            if (pushes_this_cycle_ >= edgeCycleRateLimit_()) {
                return false;
            }
            if (!hasMPSCLaneAdmissionSlot_(send_cycle)) {
                return false;
            }
            if (!to_->pushToThreadQueueCancelable(thread_queue_id_, std::move(data), arrive_cycle,
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
                                                  &cancel_epoch_, epoch_snapshot,
#else
                                                  nullptr, 0,
#endif
                                                  send_cycle, conn_id_)) {
                return false;
            }
            ++pushes_this_cycle_;
            wakeUnitAt(destination(), arrive_cycle);
            return true;
        }
        // SPSC/SingleThread mode: enforce both the per-cycle send bound and the
        // model-visible destination capacity. The backing SPSC ring can be much
        // larger than the architectural FIFO depth, so admission uses a
        // producer-cycle snapshot instead of live target queue fullness.
        if (pushes_this_cycle_ >= edgeCycleRateLimit_()) {
            return false;
        }
        if (!hasDestinationAdmissionSlot_(send_cycle)) {
            return false;
        }
        const bool ok = to_->enqueueCancelable(std::move(data), arrive_cycle,
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
                                               &cancel_epoch_, epoch_snapshot,
#else
                                               nullptr, 0,
#endif
                                               send_cycle, conn_id_);
        if (ok) {
            ++pushes_this_cycle_;
            wakeUnitAt(destination(), arrive_cycle);
        }
        return ok;
    }

    /**
     * Cancel all in-flight messages previously sent on this connection.
     *
     * Only bumps the cancellation epoch. Published entries carry the previous
     * snapshot and are discarded lazily by the receiver; new entries carry the
     * new epoch. No queue mutation crosses producer/consumer ownership.
     */
    void cancelInFlight() noexcept override {
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        if (shared_broadcast_) {
            shared_cancel_before_.store(shared_broadcast_->publishedExclusive(),
                                        std::memory_order_release);
            if (to_) to_->invalidatePortTransactions_();
            return;
        }
        if (dependency_only_transport_) return;
        cancel_epoch_.fetch_add(1, std::memory_order_acq_rel);
        if (to_) to_->invalidatePortTransactions_();
#endif
    }

    /// True if the destination can accept data (back-pressure preflight).
    bool canTransfer() const {
        if (dependency_only_transport_) {
            const uint64_t send_cycle = from_->getCurrentCycle();
            maybeResetPushesCycle_(send_cycle);
            return pushes_this_cycle_ < edgeCycleRateLimit_();
        }
        // MPSC path: producer sees backpressure on its own direct SPSC lane.
        if (thread_queue_id_ != SIZE_MAX) {
            const uint64_t send_cycle = from_->getCurrentCycle();
            maybeResetPushesCycle_(send_cycle);
            if (pushes_this_cycle_ >= edgeCycleRateLimit_()) {
                return false;
            }
            if (!hasMPSCLaneAdmissionSlot_(send_cycle)) {
                return false;
            }
            return to_->canPushToThreadQueue(thread_queue_id_);
        }
        const uint64_t send_cycle = from_->getCurrentCycle();
        maybeResetPushesCycle_(send_cycle);
        return pushes_this_cycle_ < edgeCycleRateLimit_() &&
               hasDestinationAdmissionSlot_(send_cycle);
    }

    bool isDestinationFull() const { return !canTransfer(); }

    uint32_t delay() const noexcept override { return delay_; }
    Unit* source() const noexcept override;
    Unit* destination() const noexcept override;
    void* sourcePortPtr() const noexcept override { return static_cast<void*>(from_); }
    void* destPortPtr() const noexcept override { return static_cast<void*>(to_); }
    IMultiProducerPort* registerOnDestMPSC() override;

    void setDependencyOnlyTransport(
        bool enabled,
        size_t cross_thread_headroom = std::numeric_limits<size_t>::max()) noexcept override {
        dependency_only_transport_ = enabled;
        dependency_only_headroom_ =
            enabled ? cross_thread_headroom : std::numeric_limits<size_t>::max();
        if (enabled) {
            thread_queue_id_ = SIZE_MAX;
        }
        if (to_) to_->invalidatePortTransactions_();
    }

    bool dependencyOnlyTransport() const noexcept override { return dependency_only_transport_; }

    void optimizeForSameThread(bool cycle_strict_admission = false) override {
        if (dependency_only_transport_) return;
        thread_queue_id_ = SIZE_MAX;
        if (registered_capacity_.has_value()) {
            to_->setCapacity(*registered_capacity_);
        }
        to_->useSingleThreadQueue(cycle_strict_admission);
    }

    void optimizeForSPSC() override {
        if (dependency_only_transport_) return;
        thread_queue_id_ = SIZE_MAX;
        if (registered_capacity_.has_value()) {
            to_->setCapacity(*registered_capacity_);
            to_->useLockFreeQueue(*registered_capacity_);
        } else {
            to_->useLockFreeQueue();
        }
    }

    void optimizeForMPSC() override {
        if (dependency_only_transport_) return;
        // One direct SPSC lane per Connection. The InPort owns lane storage
        // and its consumer-only deterministic frontier.
        //
        // A registered edge capacity is the destination depth for every
        // transport selected during initialization.  Propagate it before the
        // adapter is created so an otherwise-unbounded InPort allocates the
        // receiver-owned aggregate FIFO instead of treating the value only as
        // private-lane headroom.  This mirrors the same-thread and SPSC paths
        // above and remains entirely off the steady-state send/receive path.
        if (registered_capacity_.has_value()) {
            to_->setCapacity(*registered_capacity_);
        }
        const size_t user_cap = edgeAdmissionCapacity_();
        to_->useMultiProducerQueue(user_cap == InPort<T>::UNLIMITED_CAPACITY ? 0 : user_cap);
    }

    void configureRegisteredEdge(std::optional<size_t> capacity,
                                 std::optional<size_t> rate) override {
        if (capacity.has_value() && *capacity == 0) {
            throw std::invalid_argument("registered edge capacity must be positive");
        }
        if (rate.has_value() && *rate == 0) {
            throw std::invalid_argument("registered edge rate must be positive");
        }
        if (capacity.has_value()) registered_capacity_ = *capacity;
        if (rate.has_value()) registered_rate_ = *rate;
        if (to_) to_->invalidatePortTransactions_();
    }

    bool ensureEpochFreeHeadroom(uint32_t max_lookahead_cycles) override {
        if (dependency_only_transport_) return dependency_only_headroom_ > 0;
        if (crossThreadHeadroom() > 0) return true;
        if (edgeAdmissionCapacity_() != InPort<T>::UNLIMITED_CAPACITY) {
            return false;
        }
        const auto requested = requiredUsableForHeadroom_(max_lookahead_cycles);
        if (!requested.has_value()) return false;
        try {
            if (thread_queue_id_ != SIZE_MAX) {
                to_->useMultiProducerQueue(*requested);
            } else if (to_->usesLockFreeQueue()) {
                to_->useLockFreeQueue(*requested);
            } else {
                return true;
            }
        } catch (const std::length_error&) {
            return false;
        }
        return crossThreadHeadroom() > 1;
    }

    size_t registerProducerThread(size_t thread_id) override {
        if (dependency_only_transport_) return SIZE_MAX;
        return to_->registerProducerThread(
            thread_id, edgeAdmissionCapacity_() != InPort<T>::UNLIMITED_CAPACITY);
    }

    void setThreadQueueId(size_t queue_id) override {
        if (!dependency_only_transport_) {
            thread_queue_id_ = queue_id;
            if (to_) to_->invalidatePortTransactions_();
        }
    }

    bool hasThreadQueueId() const noexcept override {
        return !dependency_only_transport_ && thread_queue_id_ != SIZE_MAX;
    }

    size_t crossThreadHeadroom() const noexcept override {
        if (dependency_only_transport_) return dependency_only_headroom_;
        // Identify the bounded cross-thread buffer this connection fills:
        //   MPSC (thread_queue_id_ set) -> the per-connection direct lane,
        //   SPSC (lock-free ring)       -> the InPort's lock-free queue (finite
        //                                  even for an unlimited-capacity port),
        //   same-thread / unbounded     -> no ring to overflow (SIZE_MAX).
        const bool dff_style = isDFFStyleEdge_();
        size_t ring_usable;
        bool cycle_strict_admission = false;
        if (thread_queue_id_ != SIZE_MAX) {
            ring_usable = mpscLogicalHeadroomCapacity_();
            // A bounded MPSC lane publishes receiver pops into the same
            // simulated-cycle admission ledger as SPSC. Its reverse scheduler
            // dependency must therefore keep the producer one cycle closer to
            // the consumer than a transport-only, unbounded lane.
            cycle_strict_admission = edgeAdmissionCapacity_() != InPort<T>::UNLIMITED_CAPACITY;
        } else if (to_->usesLockFreeQueue()) {
            ring_usable = spscLogicalHeadroomCapacity_();
            cycle_strict_admission = true;
        } else {
            if (dff_style) {
                // Same-thread DFF-style edges have no physical ring, but they
                // still need a logical dependency so a separate producer cluster
                // cannot run arbitrarily far ahead of its consumer. One cycle of
                // slack lets the event queue represent current output plus next
                // input without creating a zero-delay dependency cycle.
                return 2;
            }
            return SIZE_MAX;  // single-thread queue drains synchronously each tick
        }
        if (dff_style) {
            return 1;
        }
        const auto rate = effectiveHeadroomRate_();
        if (!rate.has_value()) return 0;
        const size_t buffered_cycles = ring_usable / *rate;
        // The consumer drains only *due* entries (arrive_cycle <= k, i.e.
        // send_cycle <= k - delay_), so delay_ cycles of not-yet-due entries always
        // sit buffered. Model-visible SPSC and bounded-MPSC admission is
        // cycle-strict: a consumer pop at cycle k does not free producer
        // capacity for another send in cycle k, so its safe run-ahead window is
        // one cycle smaller than a transport-only unbounded MPSC lane. A
        // delay-1, capacity-1 DFF-style edge is handled above and remains safe
        // with headroom=1.
        if (buffered_cycles < delay_) return 0;
        if (cycle_strict_admission) {
            if (buffered_cycles == delay_) return 0;
            const size_t physical_headroom = buffered_cycles - delay_;
            // A finite architectural capacity snapshot for producer cycle C
            // must observe every receiver pop through C-1. More physical ring
            // slack may prevent overflow, but it cannot make missing simulated
            // credit deterministic. Reverse-delay 1 (headroom 2) is therefore
            // the widest safe epoch-free window for a model-bounded edge.
            if (edgeAdmissionCapacity_() != InPort<T>::UNLIMITED_CAPACITY) {
                return std::min<size_t>(physical_headroom, 2);
            }
            return physical_headroom;
        }
        return buffered_cycles - delay_ + 1;
    }

    void setConnId(uint32_t conn_id) noexcept override {
        if (conn_id_ != conn_id) {
            conn_id_ = conn_id;
            if (to_) to_->invalidatePortTransactions_();
        }
    }
    uint32_t connId() const noexcept override { return conn_id_; }

    size_t transactionPushesAt(uint64_t cycle) const noexcept override {
        maybeResetPushesCycle_(cycle);
        return pushes_this_cycle_;
    }

    OutPort<T>* from() const noexcept { return from_; }
    InPort<T>* to() const noexcept { return to_; }

private:
    friend class OutPort<T>;

    /**
     * Claim one producer-owned transfer slot without publishing a payload.
     *
     * Chronon gives every Connection exactly one producer.  In MPSC mode that
     * producer owns a private SPSC lane; in SPSC/same-thread mode it is the only
     * writer of the destination queue.  A receiver can therefore only preserve
     * or release a successful physical-capacity check.  Reserving the existing
     * producer-cycle push counter also makes ordinary transfer() calls account
     * for this claim without adding a transaction branch to their hot path.
     */
    bool tryReserveTransfer_(uint64_t send_cycle) {
        if (shared_broadcast_ != nullptr) return false;

        maybeResetPushesCycle_(send_cycle);
        if (pushes_this_cycle_ >= edgeCycleRateLimit_()) {
            return false;
        }

        if (dependency_only_transport_) {
            ++pushes_this_cycle_;
            transaction_destination_epoch_ = to_->portTransactionEpoch_();
            return true;
        }

        const bool bounded_destination = boundedTransactionDestination_();
        if (bounded_destination &&
            (!transaction_group_ || !transaction_group_->tryClaim(this, send_cycle))) {
            return false;
        }

        const bool has_admission =
            thread_queue_id_ != SIZE_MAX
                ? (bounded_destination ? hasMPSCLaneAdmissionForPending_(send_cycle, 1)
                                       : hasMPSCLaneAdmissionSlot_(send_cycle)) &&
                      to_->canPushToThreadQueue(thread_queue_id_)
                : (bounded_destination ? hasDestinationAdmissionForPending_(send_cycle, 1)
                                       : hasDestinationAdmissionSlot_(send_cycle));
        if (!has_admission) {
            if (transaction_group_) transaction_group_->release(this);
            return false;
        }

        ++pushes_this_cycle_;
        transaction_destination_epoch_ = to_->portTransactionEpoch_();
        return true;
    }

    /** Validate a previously claimed slot without consuming another credit. */
    bool transactionReservationValid_(uint64_t send_cycle) const {
        if (from_->getCurrentCycle() != send_cycle || last_pushes_cycle_ != send_cycle ||
            pushes_this_cycle_ == 0 || shared_broadcast_ != nullptr ||
            transaction_destination_epoch_ != to_->portTransactionEpoch_() ||
            pushes_this_cycle_ > edgeCycleRateLimit_()) {
            return false;
        }

        if (dependency_only_transport_) return true;

        if (boundedTransactionDestination_() &&
            (!transaction_group_ || !transaction_group_->valid(this, send_cycle))) {
            return false;
        }

        const size_t pending_pushes = boundedTransactionDestination_() ? 1 : pushes_this_cycle_;
        if (thread_queue_id_ != SIZE_MAX) {
            return hasMPSCLaneAdmissionForPending_(send_cycle, pending_pushes) &&
                   to_->canPushToThreadQueue(thread_queue_id_);
        }
        return hasDestinationAdmissionForPending_(send_cycle, pending_pushes);
    }

    /** Release one uncommitted producer-cycle claim. */
    void releaseReservedTransfer_(uint64_t send_cycle) noexcept {
        if (last_pushes_cycle_ == send_cycle && pushes_this_cycle_ != 0) {
            --pushes_this_cycle_;
        }
        if (transaction_group_) transaction_group_->release(this);
    }

    /**
     * Publish through a slot that was validated as part of the complete
     * multi-port transaction.
     *
     * There is deliberately no logical admission recheck here: the claim is
     * already included in pushes_this_cycle_.  Once every participating port
     * validates, producer ownership proves that the physical push cannot become
     * full before this call (consumers only free space).  A false return is thus
     * a framework invariant violation, not model backpressure after partial
     * publication.
     */
    void commitReservedTransfer_(T&& data, uint64_t send_cycle) {
        if (dependency_only_transport_) return;
        const uint64_t arrive_cycle = send_cycle + delay_;
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        const uint64_t cancel_epoch = cancel_epoch_.load(std::memory_order_acquire);
#endif
        bool ok = false;
        if (thread_queue_id_ != SIZE_MAX) {
            ok = to_->pushToThreadQueueCancelable(thread_queue_id_, std::move(data), arrive_cycle,
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
                                                  &cancel_epoch_, cancel_epoch,
#else
                                                  nullptr, 0,
#endif
                                                  send_cycle, conn_id_);
        } else {
            ok = to_->enqueueCancelable(std::move(data), arrive_cycle,
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
                                        &cancel_epoch_, cancel_epoch,
#else
                                        nullptr, 0,
#endif
                                        send_cycle, conn_id_);
        }

        if (!ok) {
            std::terminate();
        }
        if (transaction_group_) transaction_group_->release(this);
        wakeUnitAt(destination(), arrive_cycle);
    }

    std::optional<size_t> effectiveHeadroomRate_() const noexcept {
        if (registered_rate_.has_value()) {
            return *registered_rate_;
        }
        const size_t rate = from_->perCycleCapacity();
        if (rate == OutPort<T>::UNLIMITED_CAPACITY) {
            return std::nullopt;
        }
        return rate;
    }

    size_t edgeAdmissionCapacity_() const noexcept {
        return registered_capacity_.value_or(to_->configuredCapacity());
    }

    bool boundedTransactionDestination_() const noexcept {
        if (dependency_only_transport_ || shared_broadcast_ != nullptr) return false;
        const bool bounded_model_admission =
            edgeAdmissionCapacity_() != InPort<T>::UNLIMITED_CAPACITY;
        const bool bounded_shared_transport =
            thread_queue_id_ == SIZE_MAX && to_->storageCapacity() != InPort<T>::UNLIMITED_CAPACITY;
        return bounded_model_admission || bounded_shared_transport;
    }

    static constexpr size_t minCapacity_(size_t lhs, size_t rhs) noexcept {
        if (lhs == InPort<T>::UNLIMITED_CAPACITY) return rhs;
        if (rhs == InPort<T>::UNLIMITED_CAPACITY) return lhs;
        return std::min(lhs, rhs);
    }

    size_t edgeCycleRateLimit_() const noexcept {
        size_t limit = from_->perCycleCapacity();
        if (registered_rate_.has_value()) {
            limit = *registered_rate_;
        }
        return minCapacity_(limit, edgeAdmissionCapacity_());
    }

    bool hasDestinationAdmissionForPending_(uint64_t send_cycle, size_t pending_pushes) const {
        if (isDFFStyleEdge_()) {
            const auto min_arrival = to_->admissionMinArrivalCycle(send_cycle);
            return !min_arrival.has_value() || *min_arrival == send_cycle;
        }
        size_t admission_cap = edgeAdmissionCapacity_();
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) {
            admission_cap = to_->capacity();
        }
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) {
            return true;
        }
        if (admission_cap == 0) {
            return false;
        }
        const size_t occupancy = to_->admissionOccupancy(send_cycle);
        return occupancy <= admission_cap && pending_pushes <= admission_cap - occupancy;
    }

    bool hasDestinationAdmissionSlot_(uint64_t send_cycle) const noexcept {
        if (isDFFStyleEdge_()) {
            const auto min_arrival = to_->admissionMinArrivalCycle(send_cycle);
            return !min_arrival.has_value() || *min_arrival == send_cycle;
        }
        size_t admission_cap = edgeAdmissionCapacity_();
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) {
            admission_cap = to_->capacity();
        }
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) {
            return true;
        }
        if (admission_cap == 0) {
            return false;
        }
        const size_t occupancy = destinationAdmissionOccupancy_(send_cycle);
        return occupancy < admission_cap && pushes_this_cycle_ < admission_cap - occupancy;
    }

    bool hasMPSCLaneAdmissionForPending_(uint64_t send_cycle,
                                         size_t pending_pushes) const noexcept {
        const size_t admission_cap = edgeAdmissionCapacity_();
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) return true;
        if (isDFFStyleEdge_()) {
            const auto min_arrival =
                to_->threadQueueAdmissionMinArrivalCycle(thread_queue_id_, send_cycle);
            return !min_arrival.has_value() || *min_arrival == send_cycle;
        }
        if (admission_cap == 0) return false;

        const size_t occupancy = to_->threadQueueAdmissionOccupancy(thread_queue_id_, send_cycle);
        return occupancy <= admission_cap && pending_pushes <= admission_cap - occupancy;
    }

    bool hasMPSCLaneAdmissionSlot_(uint64_t send_cycle) const noexcept {
        const size_t admission_cap = edgeAdmissionCapacity_();
        if (admission_cap == InPort<T>::UNLIMITED_CAPACITY) return true;
        if (isDFFStyleEdge_()) {
            const auto min_arrival =
                to_->threadQueueAdmissionMinArrivalCycle(thread_queue_id_, send_cycle);
            return !min_arrival.has_value() || *min_arrival == send_cycle;
        }
        if (admission_cap == 0) return false;

        const size_t occupancy = mpscAdmissionOccupancy_(send_cycle);
        return occupancy < admission_cap && pushes_this_cycle_ < admission_cap - occupancy;
    }

    size_t destinationAdmissionOccupancy_(uint64_t send_cycle) const noexcept {
        if (!admission_snapshot_valid_ || send_cycle != last_admission_cycle_) {
            admission_occupancy_at_cycle_start_ = to_->admissionOccupancy(send_cycle);
            last_admission_cycle_ = send_cycle;
            admission_snapshot_valid_ = true;
        }
        return admission_occupancy_at_cycle_start_;
    }

    size_t mpscAdmissionOccupancy_(uint64_t send_cycle) const noexcept {
        if (!admission_snapshot_valid_ || send_cycle != last_admission_cycle_) {
            admission_occupancy_at_cycle_start_ =
                to_->threadQueueAdmissionOccupancy(thread_queue_id_, send_cycle);
            last_admission_cycle_ = send_cycle;
            admission_snapshot_valid_ = true;
        }
        return admission_occupancy_at_cycle_start_;
    }

    bool isDFFStyleEdge_() const noexcept {
        const auto rate = effectiveHeadroomRate_();
        return delay_ == 1 && edgeAdmissionCapacity_() == 1 && rate.has_value() && *rate == 1;
    }

    size_t mpscLogicalHeadroomCapacity_() const noexcept {
        const size_t user_cap = edgeAdmissionCapacity_();
        if (user_cap == InPort<T>::UNLIMITED_CAPACITY) {
            return to_->storageCapacity();
        }
        return std::min(to_->storageCapacity(), user_cap);
    }

    size_t spscLogicalHeadroomCapacity_() const noexcept {
        const size_t user_cap = edgeAdmissionCapacity_();
        if (user_cap == InPort<T>::UNLIMITED_CAPACITY) {
            return to_->storageCapacity();
        }
        return std::min(to_->storageCapacity(), user_cap);
    }

    std::optional<size_t> requiredUsableForHeadroom_(uint32_t max_lookahead_cycles) const {
        const uint64_t desired = std::max<uint64_t>(max_lookahead_cycles, 2);
        const uint64_t cycles = static_cast<uint64_t>(delay_) + desired - 1;
        const auto rate = effectiveHeadroomRate_();
        if (!rate.has_value()) return std::nullopt;
        if (*rate != 0 && cycles > std::numeric_limits<size_t>::max() / *rate) {
            return std::nullopt;
        }
        return static_cast<size_t>(cycles) * *rate;
    }

    OutPort<T>* from_;
    InPort<T>* to_;
    uint32_t delay_;
    SharedBroadcast* shared_broadcast_ = nullptr;
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    // Fits in the pre-existing padding before the cache-aligned broadcast
    // cursor. Producer and receiver normally only read this word; cancellation
    // is the rare writer, so no steady-state ownership ping-pong is introduced.
    std::atomic<uint64_t> cancel_epoch_{0};
#endif
    mutable SharedBroadcastCursor shared_cursor_{};
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    std::atomic<uint64_t> shared_cancel_before_{0};
#endif
    bool dependency_only_transport_ = false;
    std::optional<size_t> registered_capacity_;
    std::optional<size_t> registered_rate_;
    size_t thread_queue_id_ = SIZE_MAX;  ///< SIZE_MAX means not in MPSC mode
    uint32_t conn_id_ = 0;               ///< Stable topology-based tiebreaker

    /// Producer-side cycle-local push counter (Track H v2 / RTL-strict
    /// backpressure). Touched only by the producer thread for this
    /// connection. No atomicity needed.
    ///
    /// Reset to 0 whenever transfer() observes a new producer cycle. Read by
    /// canTransfer() to compute "snapshot + my pending pushes this cycle"
    /// — the RTL-correct view of "would my next push exceed the destination
    /// FIFO bound?" without ever consulting the destination's mid-cycle pop
    /// activity (which a parallel-thread implementation can otherwise expose
    /// and break cycle-count reproducibility across num_workers).
    mutable size_t pushes_this_cycle_ = 0;
    mutable uint64_t last_pushes_cycle_ = 0;
    mutable bool admission_snapshot_valid_ = false;
    mutable uint64_t last_admission_cycle_ = 0;
    mutable size_t admission_occupancy_at_cycle_start_ = 0;

    void maybeResetPushesCycle_(uint64_t send_cycle) const noexcept {
        if (send_cycle != last_pushes_cycle_) {
            pushes_this_cycle_ = 0;
            last_pushes_cycle_ = send_cycle;
            admission_snapshot_valid_ = false;
        }
    }
    /// Finite producer run-ahead supported by a model-owned external
    /// transport. SIZE_MAX retains the legacy unbounded dependency-only edge.
    size_t dependency_only_headroom_ = std::numeric_limits<size_t>::max();
    detail::ProducerDestinationTransactionState* transaction_group_ = nullptr;
    // Snapshot of the destination's cold control-plane epoch. This occupies
    // the Connection's pre-existing tail padding, so transactions do not grow
    // the cache-aligned object or perturb ordinary send/canSend fields.
    uint32_t transaction_destination_epoch_ = 0;
};

}  // namespace chronon::sender
