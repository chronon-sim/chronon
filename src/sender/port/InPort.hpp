// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <concepts>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Connection.hpp"
#include "MessageQueue.hpp"
#include "Port.hpp"
#include "PortDirectory.hpp"
#include "detail/SharedBroadcastQueueAdapter.hpp"

namespace chronon::sender {

struct InPortSelectiveFlushTestAccess;

namespace detail {

template <auto KeyFn, typename T>
concept SelectiveCancelKeyFn = requires(const T& data) {
    { std::invoke(KeyFn, data) } -> std::convertible_to<uint64_t>;
};

}  // namespace detail

/**
 * InPort - Receives data from connected OutPorts.
 *
 * Features:
 * - Timestamped message queue for deterministic delivery
 * - Synchronous tryReceive for tick-based units
 *
 * Usage:
 *   InPort<int> in{this, "in"};
 *
 *   // In tick() method
 *   if (auto value = in.tryReceive(localCycle())) {
 *       process(*value);
 *   }
 */
template <typename T>
class InPort : public PortBase, public IMultiProducerPort {
    friend struct InPortSelectiveFlushTestAccess;
    friend class Connection<T>;

public:
    using StoredMessage = detail::PortEnvelope<T>;
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    /**
     * Create an input port.
     *
     * Automatically registers with PortDirectory when owner's TreeNode is set.
     *
     * @param owner The unit that owns this port
     * @param name The port name (for debugging)
     * @param capacity Maximum queue capacity (default unlimited)
     */
    InPort(Unit* owner, std::string name, size_t capacity = UNLIMITED_CAPACITY,
           PortPolicy policy = PortPolicy::LegacyFastPath)
        : PortBase(owner, std::move(name)),
          capacity_(capacity),
          policy_(policy),
          queue_(std::make_unique<SingleThreadQueueAdapter<StoredMessage>>(capacity)) {
        reserveScratch_();
        installAutoRegistration_();
    }

    /// Convenience constructor: specify policy without setting capacity.
    InPort(Unit* owner, std::string name, PortPolicy policy)
        : PortBase(owner, std::move(name)),
          capacity_(UNLIMITED_CAPACITY),
          policy_(policy),
          queue_(std::make_unique<SingleThreadQueueAdapter<StoredMessage>>(UNLIMITED_CAPACITY)) {
        reserveScratch_();
        installAutoRegistration_();
    }

    /// @returns the cancellation policy for this InPort.
    PortPolicy policy() const noexcept { return policy_; }

private:
    /// Pre-size the receiveAllBuffered() scratch buffers so the first drain on a
    /// bounded-capacity port doesn't allocate (unlimited ports just grow once).
    void reserveScratch_() {
        if (capacity_ != UNLIMITED_CAPACITY && capacity_ > 0 && capacity_ <= (1u << 16)) {
            drain_scratch_.reserve(capacity_);
            recv_scratch_.reserve(capacity_);
        }
    }

    void installAutoRegistration_() {
        if (owner_) {
            addPortRegistrationToUnit(owner_, [this](const std::string& prefix) {
                std::string full_path = prefix + "." + name_;
                PortDirectory::instance().registerPort(
                    full_path, std::make_unique<InPortHandle<T>>(this, owner_, name_, full_path));
            });
        }
    }

public:
    /**
     * Switch to single-thread queue (no mutex overhead).
     *
     * Call this during initialization when both producer and consumer
     * are determined to be on the same thread.
     *
     * WARNING: Must be called before simulation starts (queue must be empty).
     */
    void useSingleThreadQueue(bool cycle_strict_admission = false) {
        if (shared_broadcast_queue_raw_) {
            throw std::logic_error("cannot replace an active shared broadcast transport");
        }
        multi_producer_queue_raw_ = nullptr;
        direct_spsc_queue_raw_ = nullptr;
        lock_free_queue_ = false;
        queue_ = std::make_unique<SingleThreadQueueAdapter<StoredMessage>>(capacity_,
                                                                           cycle_strict_admission);
        invalidatePortTransactions_();
    }

    /// True when this port drains a bounded lock-free SPSC ring (cross-thread,
    /// single producer). The ring is finite even for an unlimited-capacity port,
    /// so the epoch-free gate must bound producer run-ahead against capacity().
    bool usesLockFreeQueue() const noexcept { return lock_free_queue_; }
    bool usesDirectSPSC() const noexcept { return direct_spsc_queue_raw_ != nullptr; }

    /**
     * Switch to lock-free SPSC queue.
     *
     * Call this during initialization for cross-thread connections where
     * there is only ONE source thread writing to this port.
     * Uses atomic operations instead of mutex.
     */
    void useLockFreeQueue(size_t min_usable_capacity = 0) {
        if (shared_broadcast_queue_raw_) {
            throw std::logic_error("cannot replace an active shared broadcast transport");
        }
        multi_producer_queue_raw_ = nullptr;
        lock_free_queue_ = true;
        if (queue_detail::directSPSCEnabled()) {
            auto direct = std::make_unique<DirectSPSCQueueAdapter<StoredMessage>>(
                capacity_, min_usable_capacity);
            direct_spsc_queue_raw_ = direct.get();
            queue_ = std::move(direct);
            invalidatePortTransactions_();
            return;
        }
        direct_spsc_queue_raw_ = nullptr;
        queue_ =
            std::make_unique<LockFreeQueueAdapter<StoredMessage>>(capacity_, min_usable_capacity);
        invalidatePortTransactions_();
    }

    /**
     * Switch to multi-producer queue mode.
     *
     * Call this during initialization for cross-thread connections where
     * multiple source threads write to this port.
     * Creates independent SPSC ingress lanes polled by the consumer.
     * TickSimulation uses stable connection ids as the producer keys. Bounded
     * ports also create one receiver-owned aggregate FIFO; unbounded ports keep
     * the direct in-place lane-consume fast path.
     */
    void useMultiProducerQueue(size_t min_per_thread_usable_capacity = 0) {
        if (multi_producer_queue_raw_) {
            multi_producer_queue_raw_->ensurePerThreadUsableCapacity(
                min_per_thread_usable_capacity);
            invalidatePortTransactions_();
            registerCyclePreparationIfBounded_();
            return;
        }
        if (shared_broadcast_queue_raw_) {
            throw std::logic_error("cannot replace an active shared broadcast transport");
        }
        direct_spsc_queue_raw_ = nullptr;
        lock_free_queue_ = false;
        auto mpq = std::make_unique<MultiProducerQueueAdapter<StoredMessage>>(
            capacity_, min_per_thread_usable_capacity);
        multi_producer_queue_raw_ = mpq.get();
        queue_ = std::move(mpq);
        invalidatePortTransactions_();
        registerCyclePreparationIfBounded_();
    }

    /**
     * Select a queue implementation from the number of source threads.
     *
     * This is a semantic wrapper for manual setup. TickSimulation performs
     * this optimization automatically during initialization.
     */
    void configureForSourceThreadCount(size_t source_thread_count) {
        if (source_thread_count <= 1) {
            useLockFreeQueue();
            return;
        }
        useMultiProducerQueue();
    }

    /**
     * Register a stable producer key and get its queue ID.
     *
     * Only valid in multi-producer mode.
     * @param thread_id Stable producer key; TickSimulation passes conn_id + 1.
     * @return Queue ID for this producer key (used in pushToThreadQueue)
     */
    size_t registerProducerThread(size_t thread_id) {
        return registerProducerThread(thread_id, capacity_ != UNLIMITED_CAPACITY);
    }

    size_t registerProducerThread(size_t thread_id, bool track_admission) {
        if (!multi_producer_queue_raw_) {
            return SIZE_MAX;
        }
        const size_t queue_id =
            multi_producer_queue_raw_->addProducerThread(thread_id, track_admission);
        invalidatePortTransactions_();
        return queue_id;
    }

    /// Get the queue ID for a given producer key (multi-producer mode only).
    size_t getQueueIdForThread(size_t thread_id) const {
        if (!multi_producer_queue_raw_) {
            return SIZE_MAX;
        }
        return multi_producer_queue_raw_->getQueueIdForThread(thread_id);
    }

    /// True if the port is in multi-producer mode.
    bool isMultiProducerMode() const { return multi_producer_queue_raw_ != nullptr; }

    /**
     * Push to a specific thread's queue in multi-producer mode.
     *
     * @param queue_id The queue ID (from registerProducerThread)
     * @param data The message data
     * @param arrive_cycle When the message should be delivered
     * @return true if enqueued, false if queue full
     */
    bool pushToThreadQueue(size_t queue_id, T data, uint64_t arrive_cycle,
                           uint64_t enqueue_cycle = 0, uint32_t sender_id = 0) {
        if (!multi_producer_queue_raw_) {
            return false;
        }
        StoredMessage msg{.data = std::move(data)};
        msg.enqueue_cycle = enqueue_cycle;
        return multi_producer_queue_raw_->pushFromThread(queue_id, std::move(msg), arrive_cycle,
                                                         sender_id);
    }

    /// Enqueue a cancelable message to a specific thread queue (MPSC mode).
    bool pushToThreadQueueCancelable(size_t queue_id, T data, uint64_t arrive_cycle,
                                     const std::atomic<uint64_t>* cancel_epoch,
                                     uint64_t epoch_snapshot, uint64_t enqueue_cycle = 0,
                                     uint32_t sender_id = 0) {
        if (!multi_producer_queue_raw_) {
            return false;
        }
        StoredMessage msg{.data = std::move(data)};
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        msg.cancel_epoch = cancel_epoch;
        msg.epoch_snapshot = epoch_snapshot;
#else
        (void)cancel_epoch;
        (void)epoch_snapshot;
#endif
        msg.enqueue_cycle = enqueue_cycle;
        if (detail::isCanceled(msg)) {
            return true;
        }
        return multi_producer_queue_raw_->pushFromThread(queue_id, std::move(msg), arrive_cycle,
                                                         sender_id);
    }

    /**
     * Enqueue a cancelable message for delivery.
     *
     * Used by Connection to support OutPort::cancelInFlight().
     */
    bool enqueueCancelable(T data, uint64_t arrive_cycle, const std::atomic<uint64_t>* cancel_epoch,
                           uint64_t epoch_snapshot, uint32_t sender_id = 0) {
        StoredMessage msg{.data = std::move(data)};
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        msg.cancel_epoch = cancel_epoch;
        msg.epoch_snapshot = epoch_snapshot;
#else
        (void)cancel_epoch;
        (void)epoch_snapshot;
#endif
        msg.enqueue_cycle = 0;
        msg.sender_id = sender_id;
        return enqueueStored_(std::move(msg), arrive_cycle);
    }

    /**
     * Cancelable enqueue with explicit enqueue_cycle stamp.
     *
     * Connection::transfer() uses this overload (computed as send_cycle, which
     * is the producer's localCycle) so receiver-owned predicates can decide
     * whether the message predates a flush.
     */
    bool enqueueCancelable(T data, uint64_t arrive_cycle, const std::atomic<uint64_t>* cancel_epoch,
                           uint64_t epoch_snapshot, uint64_t enqueue_cycle,
                           uint32_t sender_id = 0) {
        StoredMessage msg{.data = std::move(data)};
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        msg.cancel_epoch = cancel_epoch;
        msg.epoch_snapshot = epoch_snapshot;
#else
        (void)cancel_epoch;
        (void)epoch_snapshot;
#endif
        msg.enqueue_cycle = enqueue_cycle;
        msg.sender_id = sender_id;
        return enqueueStored_(std::move(msg), arrive_cycle);
    }

    /// True if this producer can push again in the current simulated cycle.
    bool canAccept(size_t pending = 0) const { return pending < admissionCapacity(); }

    /**
     * Check if a specific producer queue can accept data (MPSC mode).
     *
     * Falls back to port-level canAccept in non-MPSC mode.
     */
    bool canAcceptOnThreadQueue(size_t queue_id, size_t pending = 0) const {
        // Port-level cap dominates: every producer (regardless of which
        // thread queue it lands in) is throttled by the user-set capacity.
        if (!canAccept(pending)) {
            return false;
        }
        if (!multi_producer_queue_raw_) {
            return true;
        }
        // Per-producer ring physical limit; ensures one hot producer doesn't
        // exhaust its own ring.
        return !multi_producer_queue_raw_->fullForThread(queue_id);
    }

    bool canPushToThreadQueue(size_t queue_id) const {
        return multi_producer_queue_raw_ &&
               !multi_producer_queue_raw_->storageFullForThread(queue_id);
    }

    size_t threadQueueAdmissionOccupancy(size_t queue_id, uint64_t send_cycle) const noexcept {
        return multi_producer_queue_raw_
                   ? multi_producer_queue_raw_->admissionOccupancyForThread(queue_id, send_cycle)
                   : 0;
    }

    std::optional<uint64_t> threadQueueAdmissionMinArrivalCycle(
        size_t queue_id, uint64_t send_cycle) const noexcept {
        return multi_producer_queue_raw_
                   ? multi_producer_queue_raw_->admissionMinArrivalCycleForThread(queue_id,
                                                                                  send_cycle)
                   : std::nullopt;
    }

    size_t capacity() const { return queue_->capacity(); }
    size_t storageCapacity() const noexcept { return queue_->storageCapacity(); }
    size_t configuredCapacity() const noexcept { return capacity_; }
    size_t available() const { return queue_->available(); }
    size_t admissionOccupancy(uint64_t send_cycle) const {
        return queue_->admissionOccupancy(send_cycle);
    }
    std::optional<uint64_t> admissionMinArrivalCycle(uint64_t send_cycle) const {
        return queue_->admissionMinArrivalCycle(send_cycle);
    }

    void setCapacity(size_t capacity) {
        // Configuration-time operation. Multi-producer mode rejects a change
        // while either ingress or the shared FIFO contains data so no payload
        // can be silently displaced.
        queue_->setCapacity(capacity);
        capacity_ = capacity;
        invalidatePortTransactions_();
        registerCyclePreparationIfBounded_();
    }

    /**
     * Effective per-cycle admission bound visible to producer-side logic.
     *
     * Connection::transfer consults this to enforce the same rate limit
     * as canAccept() on the push path.
     */
    size_t admissionCapacity() const noexcept { return effectiveCapacity_(); }

    /** Initialization-only connection metadata for selective-flush retirement. */
    void registerIncomingDelay(uint32_t delay) noexcept {
        max_incoming_delay_ = std::max(max_incoming_delay_, delay);
    }

    /**
     * Register one connection in its producer-owned destination transaction
     * group. Topology construction is single-threaded; after initialization a
     * group is touched only by its source Unit's worker.
     */
    detail::ProducerDestinationTransactionState* registerTransactionProducer(
        Unit* producer, ConnectionBase* connection) {
        if (!producer || !connection) return nullptr;
        if (!producer_transaction_states_) {
            producer_transaction_states_ = std::make_unique<ProducerTransactionStates>();
        }
        for (auto& state : *producer_transaction_states_) {
            if (state->producer != producer) continue;
            state->connections.push_back({.connection = connection});
            return state.get();
        }

        auto state = std::make_unique<detail::ProducerDestinationTransactionState>(producer);
        state->connections.push_back({.connection = connection});
        auto* result = state.get();
        producer_transaction_states_->push_back(std::move(state));
        return result;
    }

    /**
     * Register an MPSC-mode Connection with this InPort. Connections are kept
     * sorted by conn_id for stable topology/progress bookkeeping; payload
     * ordering is handled by the direct-lane frontier.
     */
    void registerMPSCConnection(ConnectionBase* conn) {
        if (!conn) return;
        const uint32_t cid = conn->connId();
        auto it = mpsc_connections_.begin();
        for (; it != mpsc_connections_.end(); ++it) {
            if ((*it) == conn) return;
            if ((*it)->connId() > cid) break;
        }
        mpsc_connections_.insert(it, conn);
    }

    /** Register one source lane selected by transparent broadcast discovery. */
    void registerSharedBroadcastConnection(Connection<T>* conn) {
        if (!conn) return;
        if constexpr (!std::is_copy_constructible_v<T>) {
            throw std::logic_error("shared broadcast requires copy-constructible payloads");
        } else {
            if (!shared_broadcast_queue_raw_) {
                if (queue_->size() != 0 || !mpsc_connections_.empty()) {
                    throw std::logic_error("cannot enable shared broadcast on a non-empty InPort");
                }
                auto adapter = std::make_unique<detail::SharedBroadcastQueueAdapter<T>>();
                shared_broadcast_queue_raw_ = adapter.get();
                direct_spsc_queue_raw_ = nullptr;
                multi_producer_queue_raw_ = nullptr;
                lock_free_queue_ = false;
                queue_ = std::move(adapter);
            }
            shared_broadcast_queue_raw_->registerConnection(conn);
            invalidatePortTransactions_();
        }
    }

    /** Finalize the initialization-time direct replay plan for a complete bus. */
    bool finalizeTransparentBroadcastReplay(size_t producer_count) {
        return shared_broadcast_queue_raw_ &&
               shared_broadcast_queue_raw_->finalizeCompleteGroup(producer_count);
    }

    [[nodiscard]] bool usesTransparentBroadcast() const noexcept {
        return shared_broadcast_queue_raw_ != nullptr;
    }

    [[nodiscard]] bool transparentBroadcastEligible() const noexcept {
        if constexpr (!std::is_copy_constructible_v<T>) return false;
        return capacity_ == UNLIMITED_CAPACITY && queue_->size() == 0 && mpsc_connections_.empty();
    }

    /// Resolve one producer completed_cycle atomic per MPSC connection (by the
    /// connection's source unit), aligned with mpsc_connections_ (conn_id order).
    /// Used by the epoch-free safety gate and by selective-flush retirement:
    /// every direct lane must have a scheduler progress publication before a
    /// visible future head can prove that all pre-flush publishes are complete.
    void setProducerProgress(
        const std::unordered_map<Unit*, const std::atomic<uint64_t>*>& src_progress) override {
        mpsc_conn_progress_.clear();
        mpsc_conn_progress_.reserve(mpsc_connections_.size());
        for (ConnectionBase* conn : mpsc_connections_) {
            auto it = src_progress.find(conn->source());
            mpsc_conn_progress_.push_back(it == src_progress.end() ? nullptr : it->second);
        }
    }

    bool producerProgressFullyResolved() const noexcept override {
        // No fan-in connections => nothing is drained late; safe by vacuity.
        if (mpsc_connections_.empty()) return true;
        // Every connection must have a non-null per-connection progress atomic.
        if (mpsc_conn_progress_.size() != mpsc_connections_.size()) return false;
        for (const auto* p : mpsc_conn_progress_) {
            if (!p) return false;
        }
        return true;
    }

    uint64_t transportOverflowEvents() const noexcept override {
        return multi_producer_queue_raw_ ? multi_producer_queue_raw_->transportOverflowEvents() : 0;
    }

    /**
     * Try to receive a message synchronously.
     *
     * @param current_cycle The current simulation cycle
     * @return The message if available, std::nullopt otherwise
     */
    std::optional<T> tryReceive(uint64_t current_cycle) {
        if constexpr (std::is_copy_constructible_v<T>) {
            if (shared_broadcast_queue_raw_) {
                if (selective_flush_state_.empty()) {
                    return shared_broadcast_queue_raw_->tryPopData(current_cycle);
                }
                while (auto message = shared_broadcast_queue_raw_->tryPop(current_cycle)) {
                    const bool rejected = isReceiverCanceled_(*message);
                    retireSelectiveFlushesAtFront_(*shared_broadcast_queue_raw_, current_cycle);
                    if (!rejected) return std::move(message->data);
                }
                return std::nullopt;
            }
        }
        if (selective_flush_state_.empty()) [[likely]] {
            if (direct_spsc_queue_raw_) {
                while (true) {
                    StoredMessage* message = direct_spsc_queue_raw_->peekReady(current_cycle);
                    if (!message) return std::nullopt;
                    if (detail::isCanceled(*message)) {
                        direct_spsc_queue_raw_->consumePeeked(current_cycle);
                        continue;
                    }
                    std::optional<T> result{std::in_place, std::move(message->data)};
                    direct_spsc_queue_raw_->consumePeeked(current_cycle);
                    return result;
                }
            }
            if (multi_producer_queue_raw_) {
                return tryReceiveUnfilteredFromConsumableQueue_(*multi_producer_queue_raw_,
                                                                current_cycle);
            }
        }
        return tryReceiveFiltered(current_cycle, [](const T&) noexcept { return true; });
    }

    /**
     * Receive the first ready message accepted by @p filter.
     *
     * Ready messages rejected by the receiver are consumed and permanently
     * canceled. The predicate must be noexcept and is invoked directly (no
     * std::function, allocation, or producer-side shared-state read), so
     * capturing lambdas are supported without indirection.
     */
    template <typename Filter>
        requires std::predicate<Filter&, const T&> &&
                 std::is_nothrow_invocable_r_v<bool, Filter&, const T&>
    std::optional<T> tryReceiveFiltered(uint64_t current_cycle, Filter&& filter) {
        if constexpr (std::is_copy_constructible_v<T>) {
            if (shared_broadcast_queue_raw_) {
                return tryReceiveFromSharedQueue_(current_cycle, filter);
            }
        }
        if (direct_spsc_queue_raw_) {
            return tryReceiveFromConsumableQueue_(*direct_spsc_queue_raw_, current_cycle, filter);
        }
        if (multi_producer_queue_raw_) {
            return tryReceiveFromConsumableQueue_(*multi_producer_queue_raw_, current_cycle,
                                                  filter);
        }

        return tryReceiveFromQueue_(*queue_, current_cycle, filter);
    }

    template <typename Filter>
        requires std::predicate<Filter&, const T&> &&
                 std::is_nothrow_invocable_r_v<bool, Filter&, const T&>
    std::optional<T> tryReceiveFiltered(Filter&& filter) {
        return tryReceiveFiltered(getCurrentCycle(), std::forward<Filter>(filter));
    }

    /**
     * Receive all available messages.
     *
     * @param current_cycle The current simulation cycle
     * @return Vector of all ready messages
     */
    std::vector<T> receiveAll(uint64_t current_cycle) {
        popAllReadyInto_(drain_scratch_, current_cycle);
        std::vector<T> result;
        result.reserve(drain_scratch_.size());
        for (auto& msg : drain_scratch_) {
            if (!detail::isCanceled(msg) && !isReceiverCanceled_(msg)) {
                result.push_back(std::move(msg.data));
            }
        }
        if (!drain_scratch_.empty()) retireSelectiveFlushesAfterDrain_(current_cycle);
        return result;
    }

    /// Receive all messages ready at the owning Unit's current local cycle.
    std::vector<T> receiveAll() { return receiveAll(getCurrentCycle()); }

    /**
     * Allocation-free variant of receiveAll() with identical ordering and
     * cancellation semantics. Returns a reference into a reused buffer, valid
     * until the next receive on this port. Single-consumer (only the owning unit
     * drains its port), so the buffers need no synchronization.
     */
    const std::vector<T>& receiveAllBuffered(uint64_t current_cycle) {
        popAllReadyInto_(drain_scratch_, current_cycle);
        recv_scratch_.clear();
        for (auto& msg : drain_scratch_) {
            if (!detail::isCanceled(msg) && !isReceiverCanceled_(msg)) {
                recv_scratch_.push_back(std::move(msg.data));
            }
        }
        if (!drain_scratch_.empty()) retireSelectiveFlushesAfterDrain_(current_cycle);
        return recv_scratch_;
    }
    const std::vector<T>& receiveAllBuffered() { return receiveAllBuffered(getCurrentCycle()); }

    bool hasData(uint64_t current_cycle) const {
        return shared_broadcast_queue_raw_ ? shared_broadcast_queue_raw_->hasReady(current_cycle)
                                           : queue_->hasReady(current_cycle);
    }

    /// True if messages are ready at the owning Unit's current local cycle.
    bool hasMessages() const { return hasData(getCurrentCycle()); }

    /// Earliest arrival cycle of pending or staged messages, used for lookahead.
    std::optional<uint64_t> minArrivalCycle() const override {
        return shared_broadcast_queue_raw_ ? shared_broadcast_queue_raw_->minArrivalCycle()
                                           : queue_->minArrivalCycle();
    }

    size_t queuedMessageCount() const {
        return shared_broadcast_queue_raw_ ? shared_broadcast_queue_raw_->size() : queue_->size();
    }

    /**
     * Messages waiting in per-Connection ingress lanes, outside the bounded
     * destination FIFO. Diagnostic only; normal model backpressure uses
     * OutPort/Connection and does not scan every producer lane.
     */
    size_t transportPendingMessageCount() const noexcept {
        return multi_producer_queue_raw_ ? multi_producer_queue_raw_->stagedSize() : 0;
    }

    /** Largest aggregate destination FIFO occupancy observed since creation. */
    size_t sharedFifoHighWatermark() const noexcept {
        return multi_producer_queue_raw_ ? multi_producer_queue_raw_->sharedFifoHighWatermark() : 0;
    }

    /** Receiver-cycle hook registered only for bounded MPSC InPorts. */
    void prepareConsumerCycle(uint64_t current_cycle) override {
        if (multi_producer_queue_raw_) {
            multi_producer_queue_raw_->prepareSharedFifo(current_cycle);
        }
    }

    /// Drop all queued messages (including future arrivals).
    void flush() {
        if (shared_broadcast_queue_raw_) {
            shared_broadcast_queue_raw_->clear();
        } else {
            queue_->clear();
        }
    }

    /**
     * Apply one receiver-owned range predicate to messages enqueued before
     * the current receiver cycle. Messages enqueued in this cycle or later are
     * post-flush and bypass it. The operation is independent of PortPolicy,
     * queue backend, producer count, and edge delay.
     */
    template <auto KeyFn>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void flush(FlushRange keep_range) {
        constexpr auto extractor = &extractSelectiveFlushKey_<KeyFn>;
        const uint64_t cycle = getCurrentCycle();
        selective_flush_state_.install(cycle, keep_range, extractor);
        traceSelectiveFlushInstall_(cycle, extractor);
    }

    /// Compatibility spelling: cancel keys strictly below @p watermark.
    template <auto KeyFn, typename K>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelOlderThan(K watermark) {
        flush<KeyFn>(FlushRange::olderThan(watermark));
    }

    /// Compatibility spelling: cancel keys strictly above @p watermark.
    template <auto KeyFn, typename K>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelYoungerThan(K watermark) {
        flush<KeyFn>(FlushRange::youngerThan(watermark));
    }

    /// Compatibility spelling: keep only [min_keep, max_keep].
    template <auto KeyFn, typename MinK, typename MaxK>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelOutsideInclusive(MinK min_keep, MaxK max_keep) {
        flush<KeyFn>(FlushRange::outsideInclusive(min_keep, max_keep));
    }

    /**
     * Compatibility no-op. Selective predicates are immutable, independently
     * scoped, and retire automatically; clearing live state could resurrect a
     * message canceled by an overlapping flush.
     */
    void resetSelectiveCancellation() noexcept {}

private:
    uint64_t getCurrentCycle() const;

    [[nodiscard]] uint32_t portTransactionEpoch_() const noexcept {
        return port_transaction_epoch_.load(std::memory_order_acquire);
    }

    void invalidatePortTransactions_() noexcept {
        port_transaction_epoch_.fetch_add(1, std::memory_order_release);
    }

    void registerCyclePreparationIfBounded_() {
        if (cycle_preparation_registered_ || !multi_producer_queue_raw_ ||
            capacity_ == UNLIMITED_CAPACITY) {
            return;
        }
        recordCyclePreparedPortOnOwnerUnit(owner_, this);
        cycle_preparation_registered_ = true;
    }

    template <auto KeyFn>
    static uint64_t extractSelectiveFlushKey_(const T& data) {
        return static_cast<uint64_t>(std::invoke(KeyFn, data));
    }

    template <typename Filter>
    [[gnu::always_inline]] inline std::optional<T> tryReceiveFromSharedQueue_(
        uint64_t current_cycle, Filter& filter) {
        return tryReceiveFromConsumableQueue_(*shared_broadcast_queue_raw_, current_cycle, filter);
    }

    template <typename Queue, typename Filter>
    std::optional<T> tryReceiveFromConsumableQueue_(Queue& queue, uint64_t current_cycle,
                                                    Filter& filter) {
        while (true) {
            std::optional<T> result;
            auto visit = [&](StoredMessage& msg) {
                if (isSenderCanceledByQueue_<Queue>(msg) || isReceiverCanceled_(msg) ||
                    !std::invoke(filter, std::as_const(msg.data))) {
                    return;
                }
                result.emplace(std::move(msg.data));
            };
            const bool consumed = queue.consumeReady(current_cycle, visit);
            if (!consumed) return std::nullopt;
            retireSelectiveFlushesAtFront_(queue, current_cycle);
            if (result) return result;
        }
    }

    template <typename Queue>
    [[gnu::always_inline]] inline std::optional<T> tryReceiveUnfilteredFromConsumableQueue_(
        Queue& queue, uint64_t current_cycle) {
        while (true) {
            std::optional<T> result;
            auto visit = [&](StoredMessage& msg) {
                if (!isSenderCanceledByQueue_<Queue>(msg)) {
                    result.emplace(std::move(msg.data));
                }
            };
            const bool consumed = queue.consumeReady(current_cycle, visit);
            if (!consumed) return std::nullopt;
            if (result) return result;
        }
    }

    template <typename Queue>
    [[nodiscard]] static bool isSenderCanceledByQueue_(const StoredMessage& msg) noexcept {
        if constexpr (requires { Queue::resolves_sender_cancellation; }) {
            static_assert(Queue::resolves_sender_cancellation);
            (void)msg;
            return false;
        } else {
            return detail::isCanceled(msg);
        }
    }

    template <typename Queue, typename Filter>
    std::optional<T> tryReceiveFromQueue_(Queue& queue, uint64_t current_cycle, Filter& filter) {
        while (true) {
            auto msg = queue.tryPop(current_cycle);
            if (!msg.has_value()) return std::nullopt;
            const bool rejected = detail::isCanceled(*msg) || isReceiverCanceled_(*msg) ||
                                  !std::invoke(filter, std::as_const(msg->data));
            retireSelectiveFlushesAtFront_(queue, current_cycle);
            if (!rejected) {
                return std::move(msg->data);
            }
        }
    }

    void popAllReadyInto_(std::vector<StoredMessage>& out, uint64_t current_cycle) {
        if constexpr (std::is_copy_constructible_v<T>) {
            if (shared_broadcast_queue_raw_) {
                shared_broadcast_queue_raw_->popAllInto(out, current_cycle);
                return;
            }
        }
        queue_->popAllInto(out, current_cycle);
    }

    void retireSelectiveFlushesAfterDrain_(uint64_t current_cycle) noexcept {
        if (selective_flush_state_.empty()) return;
        if (shared_broadcast_queue_raw_) {
            retireSelectiveFlushesAtFront_(*shared_broadcast_queue_raw_, current_cycle);
        } else {
            retireSelectiveFlushesAtFront_(*queue_, current_cycle);
        }
    }

    template <typename Queue>
    void retireSelectiveFlushesAtFront_(const Queue& queue,
                                        uint64_t drained_through_cycle) noexcept {
        if (selective_flush_state_.empty()) return;
        auto front_arrival = queue.minArrivalCycle();
        const uint64_t empty_frontier =
            drained_through_cycle == std::numeric_limits<uint64_t>::max()
                ? drained_through_cycle
                : drained_through_cycle + 1;
        uint64_t retirement_frontier = front_arrival.value_or(empty_frontier);
        if (!selective_flush_state_.hasRetirementCandidate(retirement_frontier,
                                                           max_incoming_delay_))
            return;

        const auto producer_progress_floor = mpscProducerProgressFloor_();
        if (producer_progress_floor) {
            // A producer publishes its queue entry before release-publishing
            // completed_cycle. Re-read the frontier after acquiring progress
            // so an old arrival published between the first head read and the
            // progress read cannot hide behind a stale future head.
            front_arrival = queue.minArrivalCycle();
            retirement_frontier = front_arrival.value_or(empty_frontier);
        }
        selective_flush_state_.retireBeforeArrival(retirement_frontier, max_incoming_delay_,
                                                   producer_progress_floor);
    }

    [[nodiscard]] std::optional<uint64_t> mpscProducerProgressFloor_() const noexcept {
        if (!multi_producer_queue_raw_ || mpsc_conn_progress_.empty() ||
            mpsc_conn_progress_.size() != mpsc_connections_.size()) {
            return std::nullopt;
        }

        uint64_t floor = std::numeric_limits<uint64_t>::max();
        for (const auto* progress : mpsc_conn_progress_) {
            // An unresolved set vetoes epoch-free execution, so its queue is
            // drained by a globally synchronized mode and needs no progress gate.
            if (!progress) return std::nullopt;
            floor = std::min(floor, progress->load(std::memory_order_acquire));
        }
        return floor;
    }

    void traceSelectiveFlushInstall_(
        uint64_t cycle,
        typename detail::SelectiveFlushPredicate<T>::KeyExtractor extractor) const noexcept {
        if (!stageTraceEnabled_() || !stageTracePortMatches_(name_)) {
            return;
        }
        FlushRange range =
            FlushRange::outsideInclusive(uint64_t{0}, std::numeric_limits<uint64_t>::max());
        for (const auto& predicate : selective_flush_state_.slots) {
            if (predicate.flush_cycle == cycle && predicate.key_extractor == extractor) {
                range = predicate.keep_range;
                break;
            }
        }
        std::fprintf(stderr,
                     "[STAGE] install port=%s cycle=%lu keep=%c%lu,%lu%c live=%zu hwm=%zu\n",
                     name_.c_str(), static_cast<unsigned long>(cycle),
                     range.minInclusive() ? '[' : '(', static_cast<unsigned long>(range.minKeep()),
                     static_cast<unsigned long>(range.maxKeep()), range.maxInclusive() ? ']' : ')',
                     selective_flush_state_.active_slot_count(), selective_flush_state_.high_water);
    }

    /// Effective backpressure bound = user-set capacity, but never larger
    /// than the underlying ring's physical capacity (overflow protection).
    /// UNLIMITED_CAPACITY means the user opted out of model-side backpressure.
    size_t effectiveCapacity_() const noexcept {
        if (capacity_ == UNLIMITED_CAPACITY) {
            return queue_->capacity();
        }
        return std::min(capacity_, queue_->capacity());
    }

    bool enqueueStored_(StoredMessage msg, uint64_t arrive_cycle) {
        // Sender-owned epoch cancellation may be resolved before publication.
        // Receiver selective-flush state is intentionally never consulted here.
        if (detail::isCanceled(msg)) {
            return true;
        }

        if (direct_spsc_queue_raw_) {
            return direct_spsc_queue_raw_->pushDirect(std::move(msg), arrive_cycle);
        }
        return queue_->push(std::move(msg), arrive_cycle);
    }

    [[nodiscard]] bool isReceiverCanceled_(const StoredMessage& msg) noexcept {
        if (selective_flush_state_.empty()) return false;
        const bool cancel = selective_flush_state_.shouldCancel(msg);
        if (stageTraceEnabled_() && stageTracePortMatches_(name_)) {
            std::fprintf(stderr, "[STAGE] check port=%s enq_cyc=%lu result=%s live=%zu\n",
                         name_.c_str(), static_cast<unsigned long>(msg.enqueue_cycle),
                         cancel ? "CANCEL" : "pass ", selective_flush_state_.active_slot_count());
        }
        return cancel;
    }

    size_t capacity_;
    PortPolicy policy_ = PortPolicy::LegacyFastPath;
    std::unique_ptr<IMessageQueue<StoredMessage>> queue_;
    detail::SharedBroadcastQueueAdapter<T>* shared_broadcast_queue_raw_ = nullptr;
    DirectSPSCQueueAdapter<StoredMessage>* direct_spsc_queue_raw_ = nullptr;
    MultiProducerQueueAdapter<StoredMessage>* multi_producer_queue_raw_ =
        nullptr;                    ///< Non-owning ptr for MPSC access
    bool lock_free_queue_ = false;  ///< True iff queue_ is the lock-free SPSC ring
    bool cycle_preparation_registered_ = false;
    // Lives in the six-byte alignment hole before drain_scratch_. Even a
    // continuously invalidated cycle-local claim cannot observe 2^32 control
    // mutations before commit, so wrapping cannot resurrect a live claim.
    std::atomic<uint32_t> port_transaction_epoch_{0};
    // Reused by receiveAllBuffered() to avoid per-cycle allocation; single-consumer,
    // so no synchronization is needed.
    std::vector<StoredMessage> drain_scratch_;
    std::vector<T> recv_scratch_;
    /// Initialization-only maximum; read by the destination Unit thereafter.
    uint32_t max_incoming_delay_ = 0;
    /// Installed, evaluated, and retired only by the destination Unit.
    detail::InPortSelectiveFlushState<T> selective_flush_state_{};

    /// Direct MPSC lane registry, sorted by topology-stable conn_id.
    std::vector<ConnectionBase*> mpsc_connections_;

    /// Per-connection producer completed_cycle atomics, aligned with
    /// mpsc_connections_. Besides proving scheduler coverage, a live selective
    /// flush uses them to stabilize the heterogeneous-delay arrival frontier.
    std::vector<const std::atomic<uint64_t>*> mpsc_conn_progress_;

    /// Lazily allocated topology-only ownership for cache-line-isolated,
    /// producer-owned transaction control blocks.
    using ProducerTransactionStates =
        std::vector<std::unique_ptr<detail::ProducerDestinationTransactionState>>;
    std::unique_ptr<ProducerTransactionStates> producer_transaction_states_;
};

template <typename T>
IMultiProducerPort* Connection<T>::registerOnDestMPSC() {
    if (thread_queue_id_ == SIZE_MAX || !to_) {
        return nullptr;
    }
    to_->registerMPSCConnection(this);
    return static_cast<IMultiProducerPort*>(to_);
}

template <typename T>
bool Connection<T>::finalizeTransparentBroadcastForDestination(size_t producer_count) {
    return to_ && to_->finalizeTransparentBroadcastReplay(producer_count);
}

template <typename T>
PortBase* InPortHandle<T>::portBase() const {
    return port_;
}

}  // namespace chronon::sender
