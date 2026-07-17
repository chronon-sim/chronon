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
#include <mutex>
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

namespace chronon::sender {

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
    void useSingleThreadQueue() {
        multi_producer_queue_raw_ = nullptr;
        direct_spsc_queue_raw_ = nullptr;
        lock_free_queue_ = false;
        queue_ = std::make_unique<SingleThreadQueueAdapter<StoredMessage>>(capacity_);
    }

    /// True when this port drains a bounded lock-free SPSC ring (cross-thread,
    /// single producer). The ring is finite even for an unlimited-capacity port,
    /// so the epoch-free gate must bound producer run-ahead against capacity().
    bool usesLockFreeQueue() const noexcept { return lock_free_queue_; }
    bool usesDirectSPSC() const noexcept { return direct_spsc_queue_raw_ != nullptr; }
    [[deprecated("use usesDirectSPSC()")]]
    bool usesExperimentalDirectSPSC() const noexcept {
        return usesDirectSPSC();
    }

    /**
     * Switch to lock-free SPSC queue.
     *
     * Call this during initialization for cross-thread connections where
     * there is only ONE source thread writing to this port.
     * Uses atomic operations instead of mutex.
     */
    void useLockFreeQueue(size_t min_usable_capacity = 0) {
        multi_producer_queue_raw_ = nullptr;
        lock_free_queue_ = true;
        if (queue_detail::directSPSCEnabled()) {
            auto direct = std::make_unique<DirectSPSCQueueAdapter<StoredMessage>>(
                capacity_, min_usable_capacity);
            direct_spsc_queue_raw_ = direct.get();
            queue_ = std::move(direct);
            return;
        }
        direct_spsc_queue_raw_ = nullptr;
        queue_ =
            std::make_unique<LockFreeQueueAdapter<StoredMessage>>(capacity_, min_usable_capacity);
    }

    /**
     * Switch to multi-producer queue mode.
     *
     * Call this during initialization for cross-thread connections where
     * multiple source threads write to this port.
     * Creates independent SPSC queues polled by the consumer. TickSimulation
     * uses stable connection ids as the producer keys.
     */
    void useMultiProducerQueue(size_t min_per_thread_usable_capacity = 0) {
        if (multi_producer_queue_raw_) {
            multi_producer_queue_raw_->ensurePerThreadUsableCapacity(
                min_per_thread_usable_capacity);
            return;
        }
        direct_spsc_queue_raw_ = nullptr;
        lock_free_queue_ = false;
        auto mpq = std::make_unique<MultiProducerQueueAdapter<StoredMessage>>(
            capacity_, min_per_thread_usable_capacity);
        multi_producer_queue_raw_ = mpq.get();
        queue_ = std::move(mpq);
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
        return multi_producer_queue_raw_->addProducerThread(thread_id, track_admission);
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
        // Only LegacyFastPath uses receiver_generation_snapshot. Skipping the
        // stamp on StageSelective avoids the cross-thread acquire-load of
        // receiver_enqueue_generation_ entirely (the read identified in #8).
        if (policy_ == PortPolicy::LegacyFastPath) {
            stampReceiverGeneration_(msg);
        }
        // StageSelective: NEVER consult receiver state from the sender
        // thread; that read is what races in #8. The receiver applies the
        // predicate when it pops.
        if (policy_ == PortPolicy::LegacyFastPath && isReceiverCanceled_(msg)) {
            return true;
        }
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
        if (policy_ == PortPolicy::LegacyFastPath) {
            stampReceiverGeneration_(msg);
        }
        if (detail::isCanceled(msg)) {
            return true;
        }
        if (policy_ == PortPolicy::LegacyFastPath && isReceiverCanceled_(msg)) {
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
     * is the producer's localCycle) so that StageSelective predicates can
     * decide whether the message predates the most recent flush.
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
        queue_->setCapacity(capacity);
        capacity_ = capacity;
    }

    /**
     * Effective per-cycle admission bound visible to producer-side logic.
     *
     * Connection::transfer consults this to enforce the same rate limit
     * as canAccept() on the push path.
     */
    size_t admissionCapacity() const noexcept { return effectiveCapacity_(); }

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
        if (queue_->size() != 0 || !mpsc_connections_.empty()) {
            throw std::logic_error("cannot enable shared broadcast on a non-empty InPort");
        }
        auto it = shared_broadcast_connections_.begin();
        for (; it != shared_broadcast_connections_.end(); ++it) {
            if (*it == conn) return;
            if ((*it)->connId() > conn->connId()) break;
        }
        shared_broadcast_connections_.insert(it, conn);

        // Unit::initialize() runs before transparent-broadcast discovery. If
        // legacy receiver cancellation established its in-flight scope there,
        // no shared lane existed for ensureReceiverScopeToInFlightLocked_() to
        // stamp. Catch this newly attached lane up now: the current shared tail
        // is the cutoff, so every later publish belongs to the next generation
        // and is not filtered by the initialize-time cancellation.
        if (policy_ == PortPolicy::LegacyFastPath) {
            const uint64_t generation = receiver_filter_generation_.load(std::memory_order_acquire);
            if (generation != std::numeric_limits<uint64_t>::max()) {
                if constexpr (std::is_copy_constructible_v<T>) {
                    conn->captureSharedReceiverCancellationScope(generation);
                }
            }
        }
    }

    [[nodiscard]] bool usesTransparentBroadcast() const noexcept {
        return !shared_broadcast_connections_.empty();
    }

    [[nodiscard]] bool transparentBroadcastEligible() const noexcept {
        if constexpr (!std::is_copy_constructible_v<T>) return false;
        return capacity_ == UNLIMITED_CAPACITY && queue_->size() == 0 && mpsc_connections_.empty();
    }

    /// Resolve one producer completed_cycle atomic per MPSC connection (by the
    /// connection's source unit), aligned with mpsc_connections_ (conn_id order).
    /// Retained for the epoch-free safety gate: every direct lane must have a
    /// scheduler progress dependency before producer/consumer run-ahead is used.
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
            if (!shared_broadcast_connections_.empty()) {
                return tryReceiveSharedBroadcastFiltered_(current_cycle, filter);
            }
        }
        if (direct_spsc_queue_raw_) {
            while (true) {
                StoredMessage* msg = direct_spsc_queue_raw_->peekReady(current_cycle);
                if (!msg) return std::nullopt;
                if (detail::isCanceled(*msg) || isReceiverCanceled_(*msg) ||
                    !std::invoke(filter, std::as_const(msg->data))) {
                    direct_spsc_queue_raw_->consumePeeked(current_cycle);
                    continue;
                }
                std::optional<T> result{std::in_place, std::move(msg->data)};
                direct_spsc_queue_raw_->consumePeeked(current_cycle);
                return result;
            }
        }
        if (multi_producer_queue_raw_) {
            while (true) {
                std::optional<T> result;
                const bool consumed =
                    multi_producer_queue_raw_->consumeReady(current_cycle, [&](StoredMessage& msg) {
                        if (detail::isCanceled(msg) || isReceiverCanceled_(msg) ||
                            !std::invoke(filter, std::as_const(msg.data))) {
                            return;
                        }
                        result.emplace(std::move(msg.data));
                    });
                if (!consumed) return std::nullopt;
                if (result) return result;
            }
        }

        while (true) {
            auto msg = queue_->tryPop(current_cycle);
            if (!msg.has_value()) {
                return std::nullopt;
            }
            if (detail::isCanceled(*msg) || isReceiverCanceled_(*msg) ||
                !std::invoke(filter, std::as_const(msg->data))) {
                continue;
            }
            return std::move(msg->data);
        }
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
        if constexpr (std::is_copy_constructible_v<T>) {
            if (!shared_broadcast_connections_.empty()) {
                std::vector<T> result;
                while (auto value = tryReceiveSharedBroadcast_(current_cycle)) {
                    result.push_back(std::move(*value));
                }
                return result;
            }
        }
        auto all = queue_->popAll(current_cycle);
        std::vector<T> result;
        result.reserve(all.size());
        for (auto& msg : all) {
            if (!detail::isCanceled(msg) && !isReceiverCanceled_(msg)) {
                result.push_back(std::move(msg.data));
            }
        }
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
        if constexpr (std::is_copy_constructible_v<T>) {
            if (!shared_broadcast_connections_.empty()) {
                recv_scratch_.clear();
                while (auto value = tryReceiveSharedBroadcast_(current_cycle)) {
                    recv_scratch_.push_back(std::move(*value));
                }
                return recv_scratch_;
            }
        }
        queue_->popAllInto(drain_scratch_, current_cycle);
        recv_scratch_.clear();
        for (auto& msg : drain_scratch_) {
            if (!detail::isCanceled(msg) && !isReceiverCanceled_(msg)) {
                recv_scratch_.push_back(std::move(msg.data));
            }
        }
        return recv_scratch_;
    }
    const std::vector<T>& receiveAllBuffered() { return receiveAllBuffered(getCurrentCycle()); }

    bool hasData(uint64_t current_cycle) const {
        if constexpr (std::is_copy_constructible_v<T>) {
            if (!shared_broadcast_connections_.empty()) {
                return peekReadySharedBroadcast_(current_cycle).has_value();
            }
        }
        return queue_->hasReady(current_cycle);
    }

    /// True if messages are ready at the owning Unit's current local cycle.
    bool hasMessages() const { return hasData(getCurrentCycle()); }

    /// Earliest arrival cycle of pending or staged messages, used for lookahead.
    std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> earliest = queue_->minArrivalCycle();
        if constexpr (std::is_copy_constructible_v<T>) {
            for (const auto* conn : shared_broadcast_connections_) {
                if (auto view = conn->peekSharedBroadcast()) {
                    if (!earliest || view->arrive_cycle < *earliest) {
                        earliest = view->arrive_cycle;
                    }
                }
            }
        }
        return earliest;
    }

    size_t queuedMessageCount() const {
        size_t count = queue_->size();
        if constexpr (std::is_copy_constructible_v<T>) {
            for (const auto* conn : shared_broadcast_connections_) {
                count += conn->sharedBroadcastQueuedCount();
            }
        }
        return count;
    }

    /// Drop all queued messages (including future arrivals).
    void flush() {
        queue_->clear();
        if constexpr (std::is_copy_constructible_v<T>) {
            for (auto* conn : shared_broadcast_connections_) {
                conn->flushSharedBroadcast();
            }
        }
    }

    /**
     * Selectively cancel in-flight messages where KeyFn(data) < watermark.
     *
     * Receiver-side selective cancellation defaults to in-flight scope: the
     * first cancellation call captures the current enqueue generation so
     * future messages are unaffected.
     */
    template <auto KeyFn, typename K>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelOlderThan(K watermark) {
        if (policy_ != PortPolicy::LegacyFastPath) {
            // StageSelective: install the lower half of a timestamp-scoped
            // keep range. A same-cycle cancelYoungerThan call intersects with
            // this predicate in-place, so cancelOutsideInclusive remains one
            // receiver-side predicate and never adds a sender-side atomic read.
            if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) {
                return;
            }
            const uint64_t cycle = getCurrentCycle();
            const uint64_t threshold = static_cast<uint64_t>(watermark);
            stage_state_.install(cycle, threshold, std::numeric_limits<uint64_t>::max());
            traceStageInstall_(cycle);
            return;
        }
        if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) {
            return;
        }
        const uint64_t threshold = static_cast<uint64_t>(watermark);
        auto cur = receiver_min_keep_key_.load(std::memory_order_relaxed);
        while (cur < threshold &&
               !receiver_min_keep_key_.compare_exchange_weak(
                   cur, threshold, std::memory_order_release, std::memory_order_relaxed));
    }

    /// Selectively cancel in-flight messages where KeyFn(data) > watermark.
    template <auto KeyFn, typename K>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelYoungerThan(K watermark) {
        if (policy_ != PortPolicy::LegacyFastPath) {
            // StageSelective: install a per-flush predicate. Each predicate
            // is independent and retired pop-driven (see StagePredicate).
            // No generation bumps, no mutex, no bound mutation that #7
            // races against.
            //
            // We still need a key extractor; ensure it's set. We pass through
            // configureReceiverSelectiveExtractorAndScope_ for that side
            // effect, but its generation/scope mutations are harmless under
            // StageSelective (they are simply ignored by the StageSelective
            // path of isReceiverCanceled_).
            if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) {
                return;
            }
            const uint64_t cycle = getCurrentCycle();
            const uint64_t thr = static_cast<uint64_t>(watermark);
            stage_state_.install(cycle, 0, thr);
            traceStageInstall_(cycle);
            return;
        }
        if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) {
            return;
        }
        const uint64_t threshold = static_cast<uint64_t>(watermark);
        auto cur = receiver_max_keep_key_.load(std::memory_order_relaxed);
        while (cur > threshold &&
               !receiver_max_keep_key_.compare_exchange_weak(
                   cur, threshold, std::memory_order_release, std::memory_order_relaxed));
    }

    /// Keep only keys in [min_keep, max_keep] for current in-flight generation.
    template <auto KeyFn, typename MinK, typename MaxK>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    void cancelOutsideInclusive(MinK min_keep, MaxK max_keep) {
        cancelOlderThan<KeyFn>(min_keep);
        cancelYoungerThan<KeyFn>(max_keep);
    }

    /**
     * Clear receiver-side selective cancellation bounds and extractor.
     *
     * StageSelective: this is a NO-OP. Predicates are scoped per-flush and
     * retired automatically pop-driven by shouldCancel(). Clearing
     * live predicates here would re-open the #7 overlapping-flush zombie
     * bug: a second flush's reset would erase the first flush's strict
     * max_keep, allowing zombies enqueued between the two flushes to bypass
     * the stricter predicate. The caller is expected to continue invoking
     * resetSelectiveCancellation() + cancelYoungerThan() in that order
     * (for backward compat with the LegacyFastPath path) — we simply ignore the
     * reset and let install() accumulate predicates into the slots array.
     */
    void resetSelectiveCancellation() {
        if (policy_ != PortPolicy::LegacyFastPath) {
            return;
        }
        std::lock_guard<std::mutex> lock(receiver_cancel_mutex_);
        receiver_min_keep_key_.store(0, std::memory_order_release);
        receiver_max_keep_key_.store(std::numeric_limits<uint64_t>::max(),
                                     std::memory_order_release);
        receiver_key_extractor_.store(nullptr, std::memory_order_release);
        receiver_filter_generation_.store(std::numeric_limits<uint64_t>::max(),
                                          std::memory_order_release);
    }

private:
    uint64_t getCurrentCycle() const;

    void traceStageInstall_(uint64_t cycle) const noexcept {
        if (!stageTraceEnabled_() || !stageTracePortMatches_(name_)) {
            return;
        }
        uint64_t min_keep = 0;
        uint64_t max_keep = std::numeric_limits<uint64_t>::max();
        for (const auto& predicate : stage_state_.slots) {
            if (predicate.flush_cycle == cycle) {
                min_keep = predicate.min_keep;
                max_keep = predicate.max_keep;
                break;
            }
        }
        std::fprintf(stderr, "[STAGE] install port=%s cycle=%lu keep=[%lu,%lu] live=%zu hwm=%zu\n",
                     name_.c_str(), static_cast<unsigned long>(cycle),
                     static_cast<unsigned long>(min_keep), static_cast<unsigned long>(max_keep),
                     stage_state_.active_slot_count(), stage_state_.high_water);
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
        // See pushToThreadQueue for rationale: only LegacyFastPath consumes
        // receiver_generation_snapshot. Skipping the cross-thread acquire-
        // load on StageSelective makes the "no sender-side
        // receiver-atomic read" claim true at the C++ statement level (#8).
        if (policy_ == PortPolicy::LegacyFastPath) {
            stampReceiverGeneration_(msg);
        }

        // Drop immediately if already canceled.
        // - cancel_epoch check is cheap and policy-independent.
        // - Receiver-side selective filter is consulted only in the LegacyFastPath
        //   path. The StageSelective paths apply the predicate
        //   exclusively at pop time so the sender thread never reads receiver
        //   state (eliminates the #8 race).
        if (detail::isCanceled(msg)) {
            return true;
        }
        if (policy_ == PortPolicy::LegacyFastPath && isReceiverCanceled_(msg)) {
            return true;
        }

        if (direct_spsc_queue_raw_) {
            return direct_spsc_queue_raw_->pushDirect(std::move(msg), arrive_cycle);
        }
        return queue_->push(std::move(msg), arrive_cycle);
    }

    using ReceiverSelectiveKeyExtractor = uint64_t (*)(const T&);

    template <auto KeyFn>
        requires detail::SelectiveCancelKeyFn<KeyFn, T>
    bool configureReceiverSelectiveExtractorAndScope_() {
        constexpr ReceiverSelectiveKeyExtractor new_extractor = +[](const T& data) -> uint64_t {
            return static_cast<uint64_t>(std::invoke(KeyFn, data));
        };

        std::lock_guard<std::mutex> lock(receiver_cancel_mutex_);

        auto existing = receiver_key_extractor_.load(std::memory_order_acquire);
        if (!existing) {
            receiver_key_extractor_.store(new_extractor, std::memory_order_release);
            existing = new_extractor;
        }

        // Keep the first extractor for consistency.
        if (existing != new_extractor) {
            return false;
        }

        ensureReceiverScopeToInFlightLocked_();
        return true;
    }

    void ensureReceiverScopeToInFlightLocked_() {
        const uint64_t all_generations = std::numeric_limits<uint64_t>::max();
        if (receiver_filter_generation_.load(std::memory_order_acquire) != all_generations) {
            return;
        }

        // Always bump the enqueue generation so future messages are stamped
        // with a different generation and won't be subject to the current
        // cancellation policy. Critical for flush recovery: after a flush,
        // new instructions must not be canceled by the old policy.
        const uint64_t generation =
            receiver_enqueue_generation_.fetch_add(1, std::memory_order_acq_rel);
        receiver_filter_generation_.store(generation, std::memory_order_release);
        if constexpr (std::is_copy_constructible_v<T>) {
            for (auto* conn : shared_broadcast_connections_) {
                conn->captureSharedReceiverCancellationScope(generation);
            }
        }
    }

    /// Non-const: the StageSelective path mutates stage_state_ (lazy
    /// retirement of obsolete predicates). Safe because stage_state_ is
    /// touched only from the receiver thread.
    [[nodiscard]] bool isReceiverCanceled_(const StoredMessage& msg) noexcept {
        if (policy_ != PortPolicy::LegacyFastPath) {
            // StageSelective: predicates are checked here.
            // No mutex, no atomic; stage_state_ is touched only by the
            // receiver thread (install and shouldCancel; retirement is
            // pop-driven inside shouldCancel).
            auto key_fn = receiver_key_extractor_.load(std::memory_order_acquire);
            if (!key_fn) {
                return false;
            }
            if (stage_state_.slots.empty()) {
                return false;
            }
            const uint64_t key = key_fn(msg.data);
            const bool cancel = stage_state_.shouldCancel(msg.enqueue_cycle, key);
            if (stageTraceEnabled_() && stageTracePortMatches_(name_)) {
                char slots_buf[512];
                int off = 0;
                for (size_t i = 0; i < stage_state_.slots.size() && off < 400; ++i) {
                    off += std::snprintf(
                        slots_buf + off, sizeof(slots_buf) - off, " s%zu{fc=%lu,keep=[%lu,%lu]}", i,
                        static_cast<unsigned long>(stage_state_.slots[i].flush_cycle),
                        static_cast<unsigned long>(stage_state_.slots[i].min_keep),
                        static_cast<unsigned long>(stage_state_.slots[i].max_keep));
                }
                if (off == 0) {
                    slots_buf[0] = '\0';
                }
                std::fprintf(stderr,
                             "[STAGE] check   port=%s enq_cyc=%lu key=%lu result=%s live=%zu%s\n",
                             name_.c_str(), static_cast<unsigned long>(msg.enqueue_cycle),
                             static_cast<unsigned long>(key), cancel ? "CANCEL" : "pass ",
                             stage_state_.active_slot_count(), slots_buf);
            }
            return cancel;
        }

        auto key_fn = receiver_key_extractor_.load(std::memory_order_acquire);
        if (!key_fn) {
            return false;
        }

        const uint64_t filter_generation =
            receiver_filter_generation_.load(std::memory_order_acquire);
        // Messages from generations NEWER than the filter should bypass
        // cancellation (they were enqueued after the flush). Messages from
        // the filter generation or any OLDER generation must be checked.
        if (filter_generation != std::numeric_limits<uint64_t>::max() &&
            msg.receiver_generation_snapshot > filter_generation) {
            return false;
        }

        const uint64_t key = key_fn(msg.data);
        const uint64_t min_keep = receiver_min_keep_key_.load(std::memory_order_acquire);
        const uint64_t max_keep = receiver_max_keep_key_.load(std::memory_order_acquire);
        return key < min_keep || key > max_keep;
    }

    void stampReceiverGeneration_(StoredMessage& msg) {
        msg.receiver_generation_snapshot =
            receiver_enqueue_generation_.load(std::memory_order_acquire);
    }

    size_t capacity_;
    PortPolicy policy_ = PortPolicy::LegacyFastPath;
    std::unique_ptr<IMessageQueue<StoredMessage>> queue_;
    DirectSPSCQueueAdapter<StoredMessage>* direct_spsc_queue_raw_ = nullptr;
    MultiProducerQueueAdapter<StoredMessage>* multi_producer_queue_raw_ =
        nullptr;                    ///< Non-owning ptr for MPSC access
    bool lock_free_queue_ = false;  ///< True iff queue_ is the lock-free SPSC ring
    // Reused by receiveAllBuffered() to avoid per-cycle allocation; single-consumer,
    // so no synchronization is needed.
    std::vector<StoredMessage> drain_scratch_;
    std::vector<T> recv_scratch_;
    mutable std::mutex receiver_cancel_mutex_;
    std::atomic<uint64_t> receiver_min_keep_key_{0};
    std::atomic<uint64_t> receiver_max_keep_key_{std::numeric_limits<uint64_t>::max()};
    std::atomic<ReceiverSelectiveKeyExtractor> receiver_key_extractor_{nullptr};
    std::atomic<uint64_t> receiver_filter_generation_{std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> receiver_enqueue_generation_{0};
    /// StageSelective: per-port live flush predicates. Touched only from the
    /// receiver thread (install/retire/shouldCancel). No locking needed.
    detail::InPortStageCancelState stage_state_{};

    struct SharedBroadcastCandidate {
        Connection<T>* connection = nullptr;
        const T* data = nullptr;
        uint64_t sequence = 0;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
    };

    [[nodiscard]] std::optional<SharedBroadcastCandidate> peekReadySharedBroadcast_(
        uint64_t current_cycle) const noexcept;

    template <typename Filter>
    std::optional<T> tryReceiveSharedBroadcastFiltered_(uint64_t current_cycle, Filter& filter);

    std::optional<T> tryReceiveSharedBroadcast_(uint64_t current_cycle);

    std::vector<Connection<T>*> shared_broadcast_connections_;

    /// Direct MPSC lane registry, sorted by topology-stable conn_id.
    std::vector<ConnectionBase*> mpsc_connections_;

    /// Per-connection producer completed_cycle atomics, aligned with
    /// mpsc_connections_. Used only to prove the epoch-free scheduler has a
    /// progress dependency for every lane.
    std::vector<const std::atomic<uint64_t>*> mpsc_conn_progress_;
};

#include "detail/InPortSharedBroadcastImpl.hpp"

template <typename T>
IMultiProducerPort* Connection<T>::registerOnDestMPSC() {
    if (thread_queue_id_ == SIZE_MAX || !to_) {
        return nullptr;
    }
    to_->registerMPSCConnection(this);
    return static_cast<IMultiProducerPort*>(to_);
}

template <typename T>
PortBase* InPortHandle<T>::portBase() const {
    return port_;
}

}  // namespace chronon::sender
