// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
#include <vector>

namespace chronon::sender {

/**
 * MessageQueue - Thread-safe priority queue ordered by arrival cycle.
 *
 * This queue ensures deterministic message delivery:
 * - Messages are delivered in cycle order
 * - Messages within the same cycle are delivered in FIFO order
 * - Thread-safe for concurrent push/pop operations
 *
 * Usage:
 *   MessageQueue<int> queue;
 *   queue.push(42, current_cycle + delay);  // Will arrive after delay
 *   if (auto msg = queue.tryPop(current_cycle)) {
 *       process(*msg);
 *   }
 */
template <typename T>
class MessageQueue {
public:
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    /**
     * Create a message queue.
     *
     * @param capacity Maximum number of messages (default unlimited)
     */
    explicit MessageQueue(size_t capacity = UNLIMITED_CAPACITY) : capacity_(capacity) {}

    // Non-copyable, movable
    MessageQueue(const MessageQueue&) = delete;
    MessageQueue& operator=(const MessageQueue&) = delete;
    MessageQueue(MessageQueue&&) = default;
    MessageQueue& operator=(MessageQueue&&) = default;

    /**
     * Push a message to arrive at a specific cycle.
     *
     * @param data The message data
     * @param arrive_cycle The cycle at which the message should be delivered
     * @return true if push succeeded, false if queue is full (back pressure)
     */
    bool push(T data, uint64_t arrive_cycle) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Model-side admission (user-visible capacity) is enforced at the
        // InPort/Connection layer via a per-cycle push counter. The queue
        // itself does not reject on user_capacity_ here — doing so would
        // race with the consumer thread's mid-cycle pops under num_workers>=2
        // and spuriously back-pressure the producer under num_workers=1
        // where the consumer always runs after the producer within the
        // same simulated cycle.
        messages_.push({std::move(data), arrive_cycle, sequence_++});
        size_.store(messages_.size(), std::memory_order_release);
        return true;
    }

    /**
     * Try to pop a message if one is ready.
     *
     * @param current_cycle The current simulation cycle
     * @return The message data if available, std::nullopt otherwise
     */
    std::optional<T> tryPop(uint64_t current_cycle) {
        // Fast-path: check if queue is empty using atomic size
        if (size_.load(std::memory_order_relaxed) == 0) {
            return std::nullopt;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        if (messages_.empty()) {
            return std::nullopt;
        }

        const auto& top = messages_.top();
        if (top.arrive_cycle > current_cycle) {
            return std::nullopt;
        }

        T data = std::move(const_cast<InternalMessage&>(top).data);
        messages_.pop();
        size_.store(messages_.size(), std::memory_order_release);
        return data;
    }

    /**
     * Pop all messages ready at the current cycle.
     *
     * @param current_cycle The current simulation cycle
     * @return Vector of all ready messages in order
     */
    std::vector<T> popAll(uint64_t current_cycle) {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    /// Drain ready messages into `out` (cleared first, capacity retained) so a
    /// caller can reuse one buffer across cycles instead of allocating a fresh
    /// vector per popAll(). See InPort::receiveAllBuffered.
    void popAllInto(std::vector<T>& out, uint64_t current_cycle) {
        std::lock_guard<std::mutex> lock(mutex_);
        out.clear();
        while (!messages_.empty() && messages_.top().arrive_cycle <= current_cycle) {
            out.push_back(std::move(const_cast<InternalMessage&>(messages_.top()).data));
            messages_.pop();
        }
        size_.store(messages_.size(), std::memory_order_release);
    }

    /**
     * Check if any messages are ready.
     *
     * @param current_cycle The current simulation cycle
     * @return true if at least one message is ready
     */
    bool hasReady(uint64_t current_cycle) const {
        // Fast-path: check if queue is empty using atomic size
        if (size_.load(std::memory_order_relaxed) == 0) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        return !messages_.empty() && messages_.top().arrive_cycle <= current_cycle;
    }

    /**
     * Get the minimum arrival cycle of all pending messages.
     *
     * Used for lookahead computation.
     *
     * @return Minimum arrival cycle, or std::nullopt if queue is empty
     */
    std::optional<uint64_t> minArrivalCycle() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (messages_.empty()) {
            return std::nullopt;
        }
        return messages_.top().arrive_cycle;
    }

    bool empty() const { return size_.load(std::memory_order_relaxed) == 0; }

    bool full() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_.size() >= capacity_;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_.size();
    }

    size_t capacity() const noexcept { return capacity_; }

    size_t available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (capacity_ == UNLIMITED_CAPACITY) {
            return UNLIMITED_CAPACITY;
        }
        return capacity_ - messages_.size();
    }

    /**
     * Set the queue capacity.
     *
     * @param capacity New capacity (does not drop existing messages)
     */
    void setCapacity(size_t capacity) {
        std::lock_guard<std::mutex> lock(mutex_);
        capacity_ = capacity;
    }

    /**
     * Clear all pending messages.
     */
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_ = InternalQueue{};
        sequence_ = 0;
        size_.store(0, std::memory_order_release);
    }

private:
    // Internal message with sequence number for FIFO within same cycle
    struct InternalMessage {
        T data;
        uint64_t arrive_cycle;
        uint64_t sequence;

        bool operator>(const InternalMessage& other) const {
            if (arrive_cycle != other.arrive_cycle) {
                return arrive_cycle > other.arrive_cycle;
            }
            return sequence > other.sequence;  // FIFO: earlier sequence = higher priority
        }
    };

    using InternalQueue = std::priority_queue<InternalMessage, std::vector<InternalMessage>,
                                              std::greater<InternalMessage>>;

    mutable std::mutex mutex_;
    InternalQueue messages_;
    uint64_t sequence_ = 0;
    size_t capacity_ = UNLIMITED_CAPACITY;
    mutable std::atomic<size_t> size_{0};  // Cached size for fast-path checks
};

/**
 * LockFreeMessageQueue - Single-producer single-consumer message queue.
 *
 * Optimized for the common case of one sender and one receiver.
 * Uses atomic operations for lock-free operation.
 */
template <typename T>
class LockFreeMessageQueue {
public:
    static constexpr size_t CAPACITY = 4096;
    static constexpr size_t USABLE_CAPACITY = CAPACITY - 1;

    LockFreeMessageQueue()
        : buffer_(CAPACITY), head_(0), cached_tail_(0), tail_(0), cached_head_(0) {}

    /**
     * Push a message (producer only).
     *
     * @param sender_id Stable producer identifier (Connection::conn_id on the
     *                  MPSC path). Used by MultiProducerQueueAdapter as a
     *                  cross-run-stable tiebreak key in k-way merge, replacing
     *                  the partition-dependent queue_id. Zero (default) for
     *                  single-producer paths where tiebreak is irrelevant.
     * @return true if successful, false if queue is full
     */
    bool tryPush(T data, uint64_t arrive_cycle, uint32_t sender_id = 0) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t next = (tail + 1) % CAPACITY;

        // Re-read the consumer's head_ (a cross-core load) only when the cached
        // copy says full; a stale cached_head_ is conservative (looks more full).
        if (next == cached_head_) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (next == cached_head_) {
                return false;
            }
        }

        buffer_[tail] = {std::move(data), arrive_cycle, sender_id};
        tail_.store(next, std::memory_order_release);
        return true;
    }

    /**
     * Try to pop a ready message (consumer only).
     *
     * @param current_cycle The current simulation cycle
     * @return The message data if available, std::nullopt otherwise
     */
    std::optional<T> tryPop(uint64_t current_cycle) {
        size_t head = head_.load(std::memory_order_relaxed);

        // Symmetric to tryPush: re-read the producer's tail_ only when the
        // cached copy says empty.
        if (head == cached_tail_) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (head == cached_tail_) {
                return std::nullopt;
            }
        }

        // Check if head message is ready
        if (buffer_[head].arrive_cycle > current_cycle) {
            return std::nullopt;
        }

        T data = std::move(buffer_[head].data);
        head_.store((head + 1) % CAPACITY, std::memory_order_release);
        return data;
    }

    /**
     * Peek at the minimum arrival cycle.
     */
    std::optional<uint64_t> minArrivalCycle() const {
        size_t head = head_.load(std::memory_order_acquire);
        if (head == tail_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return buffer_[head].arrive_cycle;
    }

    /**
     * Peek head's (arrive_cycle, sender_id). Used by k-way merge to pick
     * the globally-earliest message with a topology-stable tiebreak.
     */
    std::optional<std::pair<uint64_t, uint32_t>> peekHead() const {
        size_t head = head_.load(std::memory_order_acquire);
        if (head == tail_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return std::make_pair(buffer_[head].arrive_cycle, buffer_[head].sender_id);
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    size_t size() const noexcept {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) {
            return tail - head;
        }
        return CAPACITY - (head - tail);
    }

    bool full() const noexcept { return size() >= USABLE_CAPACITY; }

    size_t available() const noexcept { return USABLE_CAPACITY - size(); }

    /**
     * Clear all pending messages (consumer-side).
     *
     * Drops all in-flight items currently visible in the queue by
     * advancing head to the current tail.
     */
    void clear() noexcept {
        auto tail = tail_.load(std::memory_order_acquire);
        head_.store(tail, std::memory_order_release);
        // Refresh the consumer cache, else the next tryPop sees
        // head != cached_tail_ and reads the unwritten tail slot.
        cached_tail_ = tail;
    }

private:
    struct Entry {
        T data;
        uint64_t arrive_cycle;
        uint32_t sender_id;
    };

    std::vector<Entry> buffer_;
    // Each side's atomic and its private cache of the opposite index sit on one
    // cache line, kept apart from the other side's to avoid false sharing.
    alignas(64) std::atomic<size_t> head_;  // written by consumer
    size_t cached_tail_;                    // consumer-private
    alignas(64) std::atomic<size_t> tail_;  // written by producer
    size_t cached_head_;                    // producer-private
};

/**
 * SingleThreadMessageQueue - Non-thread-safe priority queue for single-thread access.
 *
 * This queue provides the same interface as MessageQueue but with zero
 * synchronization overhead. Use when both producer and consumer are guaranteed
 * to be on the same thread (e.g., units in the same tight-coupling cluster).
 *
 * Features:
 * - Same API as MessageQueue for easy switching
 * - No mutex, no atomic operations
 * - Priority ordering by arrival cycle with FIFO within same cycle
 *
 * Usage:
 *   // Determined at initialization based on thread assignment
 *   SingleThreadMessageQueue<int> queue;
 *   queue.push(42, current_cycle + delay);
 *   if (auto msg = queue.tryPop(current_cycle)) {
 *       process(*msg);
 *   }
 */
template <typename T>
class SingleThreadMessageQueue {
public:
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    /**
     * Create a single-thread message queue.
     *
     * @param capacity Maximum number of messages (default unlimited)
     */
    explicit SingleThreadMessageQueue(size_t capacity = UNLIMITED_CAPACITY) : capacity_(capacity) {}

    // Non-copyable, movable
    SingleThreadMessageQueue(const SingleThreadMessageQueue&) = delete;
    SingleThreadMessageQueue& operator=(const SingleThreadMessageQueue&) = delete;
    SingleThreadMessageQueue(SingleThreadMessageQueue&&) = default;
    SingleThreadMessageQueue& operator=(SingleThreadMessageQueue&&) = default;

    /**
     * Push a message to arrive at a specific cycle.
     *
     * @param data The message data
     * @param arrive_cycle The cycle at which the message should be delivered
     * @return true if push succeeded, false if queue is full (back pressure)
     */
    bool push(T data, uint64_t arrive_cycle) {
        // Capacity admission is enforced upstream (see MessageQueue::push).
        messages_.push({std::move(data), arrive_cycle, sequence_++});
        return true;
    }

    /**
     * Try to pop a message if one is ready.
     *
     * @param current_cycle The current simulation cycle
     * @return The message data if available, std::nullopt otherwise
     */
    std::optional<T> tryPop(uint64_t current_cycle) {
        if (messages_.empty()) {
            return std::nullopt;
        }

        const auto& top = messages_.top();
        if (top.arrive_cycle > current_cycle) {
            return std::nullopt;
        }

        T data = std::move(const_cast<InternalMessage&>(top).data);
        messages_.pop();
        return data;
    }

    std::vector<T> popAll(uint64_t current_cycle) {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    /// Drain ready messages into `out` (cleared first, capacity retained) so the
    /// caller can reuse one buffer across cycles. See InPort::receiveAllBuffered.
    void popAllInto(std::vector<T>& out, uint64_t current_cycle) {
        out.clear();
        while (!messages_.empty() && messages_.top().arrive_cycle <= current_cycle) {
            out.push_back(std::move(const_cast<InternalMessage&>(messages_.top()).data));
            messages_.pop();
        }
    }

    bool hasReady(uint64_t current_cycle) const {
        return !messages_.empty() && messages_.top().arrive_cycle <= current_cycle;
    }

    /// Earliest arrival cycle of pending messages.
    std::optional<uint64_t> minArrivalCycle() const {
        if (messages_.empty()) {
            return std::nullopt;
        }
        return messages_.top().arrive_cycle;
    }

    bool empty() const { return messages_.empty(); }

    bool full() const { return messages_.size() >= capacity_; }

    size_t size() const { return messages_.size(); }

    size_t capacity() const noexcept { return capacity_; }

    size_t available() const {
        if (capacity_ == UNLIMITED_CAPACITY) {
            return UNLIMITED_CAPACITY;
        }
        return capacity_ - messages_.size();
    }

    void setCapacity(size_t capacity) { capacity_ = capacity; }

    void clear() {
        messages_ = InternalQueue{};
        sequence_ = 0;
    }

private:
    struct InternalMessage {
        T data;
        uint64_t arrive_cycle;
        uint64_t sequence;

        bool operator>(const InternalMessage& other) const {
            if (arrive_cycle != other.arrive_cycle) {
                return arrive_cycle > other.arrive_cycle;
            }
            return sequence > other.sequence;
        }
    };

    using InternalQueue = std::priority_queue<InternalMessage, std::vector<InternalMessage>,
                                              std::greater<InternalMessage>>;

    InternalQueue messages_;
    uint64_t sequence_ = 0;
    size_t capacity_ = UNLIMITED_CAPACITY;
};

/**
 * IMessageQueue - Type-erased interface for message queues.
 *
 * Allows switching between MessageQueue and SingleThreadMessageQueue at runtime.
 */
template <typename T>
class IMessageQueue {
public:
    virtual ~IMessageQueue() = default;

    virtual bool push(T data, uint64_t arrive_cycle) = 0;
    virtual std::optional<T> tryPop(uint64_t current_cycle) = 0;
    virtual std::vector<T> popAll(uint64_t current_cycle) = 0;
    /// Drain ready messages into a caller-owned buffer (cleared first, capacity
    /// retained) to avoid the per-call allocation of popAll().
    virtual void popAllInto(std::vector<T>& out, uint64_t current_cycle) = 0;
    virtual bool hasReady(uint64_t current_cycle) const = 0;
    virtual std::optional<uint64_t> minArrivalCycle() const = 0;
    virtual bool empty() const = 0;
    virtual bool full() const = 0;
    virtual size_t size() const = 0;
    virtual size_t capacity() const noexcept = 0;
    virtual size_t available() const = 0;
    virtual void setCapacity(size_t capacity) = 0;
    virtual void clear() = 0;
};

/**
 * QueueAdapterImpl - Generic adapter from a concrete queue to IMessageQueue.
 *
 * Works for any queue type that exposes: push(), tryPop(), popAll(),
 * hasReady(), minArrivalCycle(), empty(), full(), size(), capacity(),
 * available(), setCapacity(), clear().  Both MessageQueue and
 * SingleThreadMessageQueue satisfy this contract.
 */
template <typename T, typename QueueImpl>
class QueueAdapterImpl : public IMessageQueue<T> {
public:
    template <typename... Args>
    explicit QueueAdapterImpl(Args&&... args) : queue_(std::forward<Args>(args)...) {}

    bool push(T data, uint64_t arrive_cycle) override {
        return queue_.push(std::move(data), arrive_cycle);
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        return queue_.tryPop(current_cycle);
    }

    std::vector<T> popAll(uint64_t current_cycle) override { return queue_.popAll(current_cycle); }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        queue_.popAllInto(out, current_cycle);
    }

    bool hasReady(uint64_t current_cycle) const override { return queue_.hasReady(current_cycle); }

    std::optional<uint64_t> minArrivalCycle() const override { return queue_.minArrivalCycle(); }

    bool empty() const override { return queue_.empty(); }
    bool full() const override { return queue_.full(); }
    size_t size() const override { return queue_.size(); }
    size_t capacity() const noexcept override { return queue_.capacity(); }
    size_t available() const override { return queue_.available(); }
    void setCapacity(size_t capacity) override { queue_.setCapacity(capacity); }
    void clear() override { queue_.clear(); }

private:
    QueueImpl queue_;
};

/** Adapts MessageQueue (mutex-protected) to IMessageQueue. */
template <typename T>
using MessageQueueAdapter = QueueAdapterImpl<T, MessageQueue<T>>;

/** Adapts SingleThreadMessageQueue (no synchronization) to IMessageQueue. */
template <typename T>
using SingleThreadQueueAdapter = QueueAdapterImpl<T, SingleThreadMessageQueue<T>>;

/**
 * LockFreeQueueAdapter - Adapts LockFreeMessageQueue to IMessageQueue interface.
 *
 * Used for cross-thread SPSC (Single-Producer Single-Consumer) connections.
 * No mutex needed - uses atomic operations for thread-safe communication
 * between one producer thread and one consumer thread.
 */
template <typename T>
class LockFreeQueueAdapter : public IMessageQueue<T> {
public:
    explicit LockFreeQueueAdapter(size_t capacity = LockFreeMessageQueue<T>::USABLE_CAPACITY)
        : queue_(), user_capacity_(std::min(capacity, LockFreeMessageQueue<T>::USABLE_CAPACITY)) {}

    bool push(T data, uint64_t arrive_cycle) override {
        // Model-side admission is enforced upstream (Connection::transfer
        // uses a per-cycle push counter, InPort::canAccept(pending)
        // checks the same bound). The push path here is authoritative only
        // about the physical ring capacity: reading the live queue size to
        // gate on user_capacity_ would race with the consumer thread's
        // mid-cycle pops (num_workers>=2) and spuriously back-pressure the
        // producer in sequential num_workers=1 where the consumer always
        // runs after the producer within the same simulated cycle.
        return queue_.tryPush(std::move(data), arrive_cycle);
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        return queue_.tryPop(current_cycle);
    }

    std::vector<T> popAll(uint64_t current_cycle) override {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        out.clear();
        while (auto v = queue_.tryPop(current_cycle)) {
            out.push_back(std::move(*v));
        }
    }

    bool hasReady(uint64_t current_cycle) const override {
        auto min = queue_.minArrivalCycle();
        return min.has_value() && *min <= current_cycle;
    }

    std::optional<uint64_t> minArrivalCycle() const override { return queue_.minArrivalCycle(); }

    bool empty() const override { return queue_.empty(); }
    bool full() const override { return queue_.size() >= user_capacity_; }
    size_t size() const override { return queue_.size(); }
    size_t capacity() const noexcept override { return user_capacity_; }
    size_t available() const override {
        const size_t sz = queue_.size();
        return sz < user_capacity_ ? user_capacity_ - sz : 0;
    }
    void setCapacity(size_t capacity) override {
        user_capacity_ = std::min(capacity, LockFreeMessageQueue<T>::USABLE_CAPACITY);
    }
    void clear() override { queue_.clear(); }

private:
    mutable LockFreeMessageQueue<T> queue_;
    size_t user_capacity_;
};

/**
 * MultiProducerQueueAdapter - Lock-free MPSC via per-thread SPSC queues.
 *
 * Each source thread gets its own LockFreeMessageQueue to the consumer.
 * This is thread-safe because:
 * - Each thread has its own dedicated queue (no concurrent push to same queue)
 * - Units on the same thread share a queue but execute sequentially
 * - Consumer pops via a k-way merge keyed on (arrive_cycle, queue_id) for
 *   deterministic, simulated-time-ordered delivery.
 *
 * Use when multiple threads write to the same InPort.
 *
 * Ordering guarantees:
 * - Primary key: arrive_cycle (lowest first).
 * - Tiebreak key: queue_id (lowest first). queue_id is assigned by
 *   MultiProducerQueueAdapter::addProducerThread in the order producer
 *   threads are discovered during TickSimulation::optimizeConnectionQueuesForThreads,
 *   which iterates a std::set<size_t>. This makes ordering reproducible run-to-run
 *   for a fixed num_workers, but NOT num_workers-invariant: same-cycle cross-thread
 *   ties may resolve differently between single-threaded (priority_queue sequence)
 *   and multi-threaded (queue_id) modes. For full num_workers-invariant replay, use
 *   PortPolicy::LegacyFastPath with MessageQueueAdapter (priority_queue), at the cost of
 *   a mutex on push.
 *
 * Correctness prerequisite:
 * - Each per-thread LockFreeMessageQueue must be pushed with non-decreasing
 *   arrive_cycle (i.e., producer pushes arrive_cycle = X + const_delay where X
 *   is monotonically advancing). This holds for any TickableUnit whose push
 *   site is `OutPort::send(data)` with fixed delay — the standard pattern.
 *   Mixed-delay producers must not route through MultiProducerQueueAdapter.
 */
template <typename T>
class MultiProducerQueueAdapter : public IMessageQueue<T> {
public:
    /**
     * Construct an MPSC adapter with an initial user-visible capacity.
     *
     * The per-thread physical LockFreeMessageQueue ring capacity
     * (USABLE_CAPACITY, typically 4095 slots) is fixed and remains
     * unchanged. user_capacity_ is a *soft* aggregate gate consulted by
     * capacity()/full(); it is only honored by
     * the arbiter/canAccept layer before admitting a push. The push path
     * itself (pushFromThread) does NOT enforce user_capacity_ — enforcing
     * it there would reintroduce a wall-clock race where two producer
     * threads racing against each other could both see size < cap and
     * both succeed even though their joint push exceeds the soft cap.
     *
     * If the user does not override capacity explicitly, we still want
     * full() to never fire for a well-sized system, so the default
     * behaves like the legacy physical aggregate (effectively unlimited
     * for typical configurations).
     */
    explicit MultiProducerQueueAdapter(size_t capacity = std::numeric_limits<size_t>::max())
        : user_capacity_(capacity) {}

    /**
     * Register a new source thread and create its queue.
     *
     * @param thread_id The thread ID (from cluster assignment)
     * @return Queue ID for this thread (used in pushFromThread)
     */
    size_t addProducerThread(size_t thread_id) {
        auto existing = thread_to_queue_id_.find(thread_id);
        if (existing != thread_to_queue_id_.end()) {
            return existing->second;
        }

        size_t id = thread_queues_.size();
        thread_queues_.push_back(std::make_unique<LockFreeMessageQueue<T>>());
        thread_to_queue_id_[thread_id] = id;
        return id;
    }

    /**
     * Get the queue ID for a given thread.
     */
    size_t getQueueIdForThread(size_t thread_id) const {
        auto it = thread_to_queue_id_.find(thread_id);
        if (it != thread_to_queue_id_.end()) {
            return it->second;
        }
        return SIZE_MAX;  // Not found
    }

    bool fullForThread(size_t queue_id) const {
        if (queue_id >= thread_queues_.size()) {
            return true;
        }
        return thread_queues_[queue_id]->full();
    }

    /**
     * Push using thread's queue ID.
     *
     * Thread-safe: each thread has its own queue.
     * Multiple units on same thread share same queue - OK because they run sequentially.
     */
    bool pushFromThread(size_t queue_id, T data, uint64_t arrive_cycle, uint32_t sender_id = 0) {
        if (queue_id >= thread_queues_.size()) {
            return false;
        }
        return thread_queues_[queue_id]->tryPush(std::move(data), arrive_cycle, sender_id);
    }

    // IMessageQueue interface - consumer polls all thread queues

    bool push(T data, uint64_t arrive_cycle) override {
        // Default push to first queue (should use pushFromThread for MPSC)
        if (thread_queues_.empty()) {
            return false;
        }
        return thread_queues_[0]->tryPush(std::move(data), arrive_cycle);
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        if (thread_queues_.empty()) {
            return std::nullopt;
        }
        // K-way merge keyed on (arrive_cycle, sender_id). sender_id is the
        // producer Connection's stable conn_id (assigned at setup in
        // connection-registration order), so tiebreak is independent of
        // partition / thread-to-queue mapping — same order for any
        // num_workers. Fallback: when sender_id is 0 everywhere (e.g. tests
        // that call pushFromThread without sender_id), this reduces to the
        // earlier queue_id-based behavior via strict '<' below.
        size_t best = SIZE_MAX;
        uint64_t best_ac = UINT64_MAX;
        uint32_t best_sid = UINT32_MAX;
        for (size_t i = 0; i < thread_queues_.size(); ++i) {
            auto head = thread_queues_[i]->peekHead();
            if (!head.has_value() || head->first > current_cycle) continue;
            const uint64_t ac = head->first;
            const uint32_t sid = head->second;
            if (ac < best_ac || (ac == best_ac && sid < best_sid)) {
                best_ac = ac;
                best_sid = sid;
                best = i;
            }
        }
        if (best == SIZE_MAX) return std::nullopt;
        return thread_queues_[best]->tryPop(current_cycle);
    }

    std::vector<T> popAll(uint64_t current_cycle) override {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        out.clear();
        while (auto v = tryPop(current_cycle)) {
            out.push_back(std::move(*v));
        }
    }

    bool hasReady(uint64_t current_cycle) const override {
        for (auto& q : thread_queues_) {
            auto min = q->minArrivalCycle();
            if (min.has_value() && *min <= current_cycle) {
                return true;
            }
        }
        return false;
    }

    std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> min;
        for (auto& q : thread_queues_) {
            auto cycle = q->minArrivalCycle();
            if (cycle.has_value()) {
                if (!min.has_value() || *cycle < *min) {
                    min = cycle;
                }
            }
        }
        return min;
    }

    bool empty() const override {
        for (auto& q : thread_queues_) {
            if (!q->empty()) return false;
        }
        return true;
    }

    bool full() const override {
        if (thread_queues_.empty()) {
            return true;
        }
        // Soft user cap takes precedence over physical ring aggregate.
        // full() is consulted by the arbiter / canAccept path to decide
        // whether to admit a new push.
        return size() >= user_capacity_;
    }

    size_t size() const override {
        size_t total = 0;
        for (const auto& q : thread_queues_) {
            total += q->size();
        }
        return total;
    }

    size_t capacity() const noexcept override { return user_capacity_; }

    size_t available() const override {
        const size_t sz = size();
        return sz < user_capacity_ ? user_capacity_ - sz : 0;
    }

    /**
     * Set the soft user-visible capacity.
     *
     * Per-thread physical ring capacity (USABLE_CAPACITY) is unchanged.
     * Only the soft aggregate gate is updated. pushFromThread() still
     * does NOT enforce this cap — per-thread ring fullness
     * (fullForThread()) continues to govern push failure on the push
     * path to avoid wall-clock races between producer threads. The
     * MPSC arbiter consults full() before admitting a message.
     */
    void setCapacity(size_t cap) override { user_capacity_ = cap; }

    void clear() override {
        for (auto& q : thread_queues_) {
            q->clear();
        }
    }

private:
    std::vector<std::unique_ptr<LockFreeMessageQueue<T>>> thread_queues_;
    std::unordered_map<size_t, size_t> thread_to_queue_id_;  // thread_id -> queue_id
    size_t user_capacity_;  // Soft aggregate cap consulted by arbiter/full().
};

}  // namespace chronon::sender
