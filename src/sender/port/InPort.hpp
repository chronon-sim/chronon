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
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
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
class InPort : public PortBase, public IArbitratablePort {
public:
    using StoredMessage = detail::PortEnvelope<T>;
    static constexpr size_t UNLIMITED_CAPACITY = MessageQueue<StoredMessage>::UNLIMITED_CAPACITY;

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
          queue_(std::make_unique<MessageQueueAdapter<StoredMessage>>(capacity)) {
        reserveScratch_();
        installAutoRegistration_();
    }

    /// Convenience constructor: specify policy without setting capacity.
    InPort(Unit* owner, std::string name, PortPolicy policy)
        : PortBase(owner, std::move(name)),
          capacity_(UNLIMITED_CAPACITY),
          policy_(policy),
          queue_(std::make_unique<MessageQueueAdapter<StoredMessage>>(UNLIMITED_CAPACITY)) {
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
        lock_free_queue_ = false;
        queue_ = std::make_unique<SingleThreadQueueAdapter<StoredMessage>>(capacity_);
    }

    /// True when this port drains a bounded lock-free SPSC ring (cross-thread,
    /// single producer). The ring is finite even for an unlimited-capacity port,
    /// so the epoch-free gate must bound producer run-ahead against capacity().
    bool usesLockFreeQueue() const noexcept { return lock_free_queue_; }

    /**
     * Switch to lock-free SPSC queue.
     *
     * Call this during initialization for cross-thread connections where
     * there is only ONE source thread writing to this port.
     * Uses atomic operations instead of mutex.
     */
    void useLockFreeQueue() {
        multi_producer_queue_raw_ = nullptr;
        lock_free_queue_ = true;
        queue_ = std::make_unique<LockFreeQueueAdapter<StoredMessage>>(capacity_);
    }

    /**
     * Switch to multi-producer queue mode.
     *
     * Call this during initialization for cross-thread connections where
     * multiple source threads write to this port.
     * Creates per-thread SPSC queues polled by consumer.
     */
    void useMultiProducerQueue() {
        if (multi_producer_queue_raw_) {
            return;
        }
        lock_free_queue_ = false;
        auto mpq = std::make_unique<MultiProducerQueueAdapter<StoredMessage>>(capacity_);
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
     * Register a source thread and get its queue ID.
     *
     * Only valid in multi-producer mode.
     * @param thread_id The source thread ID
     * @return Queue ID for this thread (used in pushToThreadQueue)
     */
    size_t registerProducerThread(size_t thread_id) {
        if (!multi_producer_queue_raw_) {
            return SIZE_MAX;
        }
        return multi_producer_queue_raw_->addProducerThread(thread_id);
    }

    /// Get the queue ID for a given source thread (multi-producer mode only).
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
        StoredMessage msg{.data = std::move(data),
                          .cancel_epoch = cancel_epoch,
                          .epoch_snapshot = epoch_snapshot};
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
                           uint64_t epoch_snapshot) {
        StoredMessage msg{.data = std::move(data),
                          .cancel_epoch = cancel_epoch,
                          .epoch_snapshot = epoch_snapshot};
        msg.enqueue_cycle = 0;
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
                           uint64_t epoch_snapshot, uint64_t enqueue_cycle) {
        StoredMessage msg{.data = std::move(data),
                          .cancel_epoch = cancel_epoch,
                          .epoch_snapshot = epoch_snapshot};
        msg.enqueue_cycle = enqueue_cycle;
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

    size_t capacity() const { return queue_->capacity(); }
    size_t available() const { return queue_->available(); }

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
     * Register an MPSC-mode Connection with this InPort so that the
     * cycle-boundary arbiter can drain its staging deque. Connections are
     * kept sorted by conn_id to give the arbiter a topology-stable fixed-
     * priority order independent of num_workers or partition layout.
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

    /**
     * Per-cycle MPSC admission arbitration.
     *
     * Called by the scheduler at each cycle boundary on InPorts that have
     * at least one MPSC connection registered. Iterates the connections in
     * conn_id-ascending order (stable across num_workers since conn_id is
     * assigned at topology construction) and lets each drain its full
     * staging deque into the shared queue.
     *
     * The user_cap on the InPort governs per-producer staging depth (a
     * Connection's staging deque is bounded at user_cap, giving the
     * producer back-pressure when full); the arbiter itself does NOT cap
     * aggregate per-cycle admission. A strict aggregate-cap budget here
     * would starve lower-conn_id connections under heavy multi-producer
     * fan-in: with N producers each pushing at the cap and a budget of
     * cap/cycle, only the highest-priority producer makes progress.
     * Bounding admission via per-producer staging plus consumer drain
     * preserves both determinism (conn_id ordering of admission) and
     * fairness (every producer that has unblocked staging makes progress
     * every cycle).
     */
    void arbitrateMPSC() override { arbitrateMPSCUpTo_(std::numeric_limits<uint64_t>::max()); }

    /**
     * Consumer-tick-driven arbitration (Option 1, see
     * docs/mpsc-atomic-publish.md). Called by TickableUnit::executeTick
     * on the consumer thread, immediately before the user's tick()
     * body. Computes the safe drain bound S = min over predecessor
     * threads of completed_cycle (acquire) and drains staging entries
     * with enqueue_cycle <= S in conn_id order.
     *
     * A no-op if the port has no MPSC connections, if the producer-
     * thread set was never resolved (progress-based sync off), or if
     * S has not advanced since the last arbitration.
     */
    void arbitrateMPSCConsumerDriven() noexcept override {
        if (mpsc_connections_.empty()) return;

        // Preferred path: per-connection progress. Drain EACH connection up to
        // a PER-CONNECTION send-cycle bound. This is required for correctness
        // under heterogeneous edge delays: with a single min-over-producers
        // bound (the legacy path below), a low-delay connection's entry needed
        // at the consumer's current cycle can be held back because a lagging
        // high-delay producer on the same InPort drags the min down — the
        // message then arrives a cycle late, diverging from barrier/sequential.
        //
        // The bound is min(producer_completed - 1, K - delay):
        //   - producer_completed - 1: only admit entries the producer has finished.
        //   - K - delay (K = this consumer's current cycle): only admit entries
        //     whose arrive_cycle (= send_cycle + delay) is <= K, i.e. visible to
        //     the receiver THIS cycle. A producer running ahead under lookahead
        //     must not have its future-cycle entries admitted early — with
        //     LegacyFastPath selective cancellation, an entry admitted before the
        //     receiver reaches its cycle can be stamped and then wrongly canceled
        //     by a later cancelYoungerThan flush, which sequential/barrier (that
        //     never admit early) would not do.
        // Each connection's staging is monotonic in send_cycle and has its own
        // conn_id-keyed ring (one fixed delay), so the shared k-way merge still
        // pops in (arrive_cycle, conn_id) order. The lookahead gate guarantees
        // producer_completed >= K+1-delay, so all arrive_cycle <= K entries are
        // admitted before the consumer reads them.
        if (!mpsc_conn_progress_.empty()) {
            constexpr size_t kUnlimitedBudget = std::numeric_limits<size_t>::max();
            const uint64_t k = getCurrentCycle();
            for (size_t i = 0; i < mpsc_connections_.size(); ++i) {
                const std::atomic<uint64_t>* p = mpsc_conn_progress_[i];
                if (!p) continue;  // unresolved producer (covered by epoch-end flush)
                const uint64_t pc = p->load(std::memory_order_acquire);
                if (pc == 0) continue;  // producer hasn't finished cycle 0 yet
                const uint64_t delay = mpsc_connections_[i]->delay();
                if (k < delay) continue;  // nothing has arrived for this edge yet
                const uint64_t bound = std::min<uint64_t>(pc - 1, k - delay);
                (void)mpsc_connections_[i]->arbitrateAdmitBoundedErased(kUnlimitedBudget, bound);
            }
            return;
        }

        if (producer_progress_ptrs_.empty()) {
            // No resolved progress atomics => fall back to unbounded drain.
            // This path keeps the consumer-driven hook correct when running
            // in Sequential or Barrier mode (where producers have already
            // finished their cycle before the consumer ticks).
            arbitrateMPSCUpTo_(std::numeric_limits<uint64_t>::max());
            return;
        }
        uint64_t s = std::numeric_limits<uint64_t>::max();
        for (const auto* p : producer_progress_ptrs_) {
            const uint64_t c = p->load(std::memory_order_acquire);
            if (c < s) s = c;
        }
        const uint64_t last = last_arbitrated_cycle_.load(std::memory_order_relaxed);
        // completed_cycle is 1-indexed (stores post-increment); s == 0 means
        // no producer has finished even cycle 0, so nothing is safe to drain.
        if (s == 0 || s == last) return;
        // s is "lowest cycle every producer has FINISHED". Drain entries
        // with enqueue_cycle strictly less than s — i.e. entries pushed
        // during a cycle every producer has fully completed.
        arbitrateMPSCUpTo_(s == 0 ? 0 : s - 1);
        last_arbitrated_cycle_.store(s, std::memory_order_relaxed);
    }

    bool hasMPSCConnections() const noexcept override { return !mpsc_connections_.empty(); }

    /**
     * Install the set of `completed_cycle` atomics for the predecessor
     * threads feeding this InPort's MPSC connections. Called at
     * TickSimulation::initialize() once the thread_progress_array_
     * has been allocated. An empty set (e.g. under Sequential or Barrier
     * execution) makes arbitrateMPSCConsumerDriven() degrade to the
     * legacy unbounded arbiter — safe because those modes only call it
     * when producers are known to have finished their cycle.
     */
    void setArbitrationProgressPointers(std::vector<const std::atomic<uint64_t>*> ptrs) override {
        producer_progress_ptrs_ = std::move(ptrs);
    }

    /// Resolve one producer completed_cycle atomic per MPSC connection (by the
    /// connection's source unit), aligned with mpsc_connections_ (conn_id order).
    /// Enables the per-connection drain in arbitrateMPSCConsumerDriven().
    void setArbitrationConnProgress(
        const std::unordered_map<Unit*, const std::atomic<uint64_t>*>& src_progress) override {
        mpsc_conn_progress_.clear();
        mpsc_conn_progress_.reserve(mpsc_connections_.size());
        for (ConnectionBase* conn : mpsc_connections_) {
            auto it = src_progress.find(conn->source());
            mpsc_conn_progress_.push_back(it == src_progress.end() ? nullptr : it->second);
        }
    }

    bool mpscConnProgressFullyResolved() const noexcept override {
        // No fan-in connections => nothing is drained late; safe by vacuity.
        if (mpsc_connections_.empty()) return true;
        // Every connection must have a non-null per-connection progress atomic.
        // A null entry is an unresolved producer that arbitrateMPSCConsumerDriven
        // skips (relying on the epoch-end central flush), which epoch-free
        // execution does not provide mid-run.
        if (mpsc_conn_progress_.size() != mpsc_connections_.size()) return false;
        for (const auto* p : mpsc_conn_progress_) {
            if (!p) return false;
        }
        return true;
    }

    uint64_t stagingOverflowEvents() const noexcept override {
        return multi_producer_queue_raw_ ? multi_producer_queue_raw_->stagingOverflowEvents() : 0;
    }

    void* arbitratablePortKey() noexcept override { return static_cast<void*>(this); }

    /**
     * Try to receive a message synchronously.
     *
     * @param current_cycle The current simulation cycle
     * @return The message if available, std::nullopt otherwise
     */
    std::optional<T> tryReceive(uint64_t current_cycle) {
        while (true) {
            auto msg = queue_->tryPop(current_cycle);
            if (!msg.has_value()) {
                return std::nullopt;
            }
            if (detail::isCanceled(*msg) || isReceiverCanceled_(*msg)) {
                continue;
            }
            return std::move(msg->data);
        }
    }

    /**
     * Receive all available messages.
     *
     * @param current_cycle The current simulation cycle
     * @return Vector of all ready messages
     */
    std::vector<T> receiveAll(uint64_t current_cycle) {
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

    bool hasData(uint64_t current_cycle) const { return queue_->hasReady(current_cycle); }

    /// True if messages are ready at the owning Unit's current local cycle.
    bool hasMessages() const { return hasData(getCurrentCycle()); }

    /// Earliest arrival cycle of pending or staged messages, used for lookahead.
    std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> earliest = queue_->minArrivalCycle();
        for (const auto* conn : mpsc_connections_) {
            if (auto staged = conn->minStagedArrivalCycle()) {
                if (!earliest || *staged < *earliest) {
                    earliest = staged;
                }
            }
        }
        return earliest;
    }

    size_t queuedMessageCount() const { return queue_->size(); }

    /// Drop all queued messages (including future arrivals).
    void flush() { queue_->clear(); }

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
            // StageSelective semantics target "younger than" only (the
            // dominant flush direction in the OOO pipeline). cancelOlderThan
            // on a StageSelective port is currently unsupported and would
            // require a second predicate slot direction. Fall through to
            // legacy path (it's still functionally correct, just not the
            // fast path) so callers that occasionally use it keep working.
            (void)configureReceiverSelectiveExtractorAndScope_<KeyFn>();
            const uint64_t threshold = static_cast<uint64_t>(watermark);
            auto cur = receiver_min_keep_key_.load(std::memory_order_relaxed);
            while (cur < threshold &&
                   !receiver_min_keep_key_.compare_exchange_weak(
                       cur, threshold, std::memory_order_release, std::memory_order_relaxed));
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
            (void)configureReceiverSelectiveExtractorAndScope_<KeyFn>();
            const uint64_t cycle = getCurrentCycle();
            const uint64_t thr = static_cast<uint64_t>(watermark);
            stage_state_.install(cycle, thr);
            if (stageTraceEnabled_() && stageTracePortMatches_(name_)) {
                std::fprintf(stderr,
                             "[STAGE] install port=%s cycle=%lu max_keep=%lu live=%zu hwm=%zu\n",
                             name_.c_str(), static_cast<unsigned long>(cycle),
                             static_cast<unsigned long>(thr), stage_state_.active_slot_count(),
                             stage_state_.high_water);
            }
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
                        slots_buf + off, sizeof(slots_buf) - off, " s%zu{fc=%lu,mk=%lu}", i,
                        static_cast<unsigned long>(stage_state_.slots[i].flush_cycle),
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

    /// MPSC arbitration state. Populated by registerMPSCConnection() during
    /// TickSimulation::initialize().
    std::vector<ConnectionBase*> mpsc_connections_;

    /// Consumer-tick-driven arbitration state (see docs/mpsc-atomic-publish.md).
    /// Resolved at initialize() to the set of
    /// thread_progress_array_[p].completed_cycle atomics whose threads feed
    /// this InPort's MPSC connections. Empty when progress-based sync is
    /// not in use — arbitrateMPSCConsumerDriven() falls back to unbounded
    /// drain in that case.
    std::vector<const std::atomic<uint64_t>*> producer_progress_ptrs_;
    /// Per-connection producer completed_cycle atomics, aligned with
    /// mpsc_connections_ (one entry per connection, conn_id order). Populated
    /// by setArbitrationConnProgress(); enables heterogeneous-delay-correct
    /// per-connection draining. nullptr entries (unresolved) are skipped.
    std::vector<const std::atomic<uint64_t>*> mpsc_conn_progress_;
    /// Last S we drained up to, used to short-circuit re-arbitration when S
    /// hasn't advanced. Written only by the consumer thread, so relaxed
    /// ordering suffices.
    std::atomic<uint64_t> last_arbitrated_cycle_{0};

    /// Shared arbitration body used by both arbitrateMPSC() (unbounded,
    /// legacy / main-thread sync points) and arbitrateMPSCConsumerDriven()
    /// (bounded by min predecessor completed_cycle).
    void arbitrateMPSCUpTo_(uint64_t max_send_cycle) noexcept {
        if (mpsc_connections_.empty()) return;
        constexpr size_t kUnlimitedBudget = std::numeric_limits<size_t>::max();
        for (ConnectionBase* conn : mpsc_connections_) {
            (void)conn->arbitrateAdmitBoundedErased(kUnlimitedBudget, max_send_cycle);
        }
    }
};

template <typename T>
IArbitratablePort* Connection<T>::registerOnDestMPSC() {
    if (thread_queue_id_ == SIZE_MAX || !to_) {
        return nullptr;
    }
    to_->registerMPSCConnection(this);
    return static_cast<IArbitratablePort*>(to_);
}

template <typename T>
PortBase* InPortHandle<T>::portBase() const {
    return port_;
}

}  // namespace chronon::sender
