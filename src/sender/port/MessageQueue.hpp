// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace chronon::sender {

namespace queue_detail {

inline bool experimentalDirectSPSCEnabled() noexcept {
    const char* value = std::getenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC");
    return value == nullptr || value[0] != '0' || value[1] != '\0';
}

inline bool experimentalMPSCActiveLanesEnabled() noexcept {
    const char* value = std::getenv("CHRONON_EXPERIMENTAL_MPSC_ACTIVE_LANES");
    return value == nullptr || value[0] != '0' || value[1] != '\0';
}
}  // namespace queue_detail

struct LockFreeQueueAdapterTestAccess;

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

    explicit LockFreeMessageQueue(size_t physical_capacity = CAPACITY)
        : capacity_(roundUpPhysicalCapacity_(physical_capacity)),
          usable_capacity_(capacity_ - 1),
          buffer_(capacity_),
          head_(0),
          cached_tail_(0),
          tail_(0),
          cached_head_(0) {}

    static size_t physicalCapacityForUserCapacity(size_t user_capacity) {
        if (user_capacity == std::numeric_limits<size_t>::max()) {
            return CAPACITY;
        }
        if (user_capacity == std::numeric_limits<size_t>::max() - 1) {
            throw std::length_error("LockFreeMessageQueue requested capacity is too large");
        }
        return roundUpPhysicalCapacity_(std::max(CAPACITY, user_capacity + 1));
    }

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
        size_t next = (tail + 1) % capacity_;

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
    template <typename BeforeRelease, typename AfterRelease>
    std::optional<T> tryPopWithReleaseCallbacks(uint64_t current_cycle,
                                                BeforeRelease&& before_release,
                                                AfterRelease&& after_release) {
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

        const uint64_t arrive_cycle = buffer_[head].arrive_cycle;
        T data = std::move(buffer_[head].data);
        before_release(arrive_cycle);
        head_.store((head + 1) % capacity_, std::memory_order_release);
        after_release(arrive_cycle);
        return data;
    }

    template <typename AfterRelease>
    std::optional<T> tryPopAfterRelease(uint64_t current_cycle, AfterRelease&& after_release) {
        return tryPopWithReleaseCallbacks(
            current_cycle, [](uint64_t) {}, [&after_release](uint64_t) { after_release(); });
    }

    std::optional<T> tryPop(uint64_t current_cycle) {
        return tryPopAfterRelease(current_cycle, [] {});
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
        return capacity_ - (head - tail);
    }

    bool full() const noexcept { return size() >= usable_capacity_; }

    size_t available() const noexcept {
        const size_t sz = size();
        return sz < usable_capacity_ ? usable_capacity_ - sz : 0;
    }

    size_t capacity() const noexcept { return capacity_; }

    size_t usableCapacity() const noexcept { return usable_capacity_; }

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
    static size_t roundUpPhysicalCapacity_(size_t capacity) {
        size_t phys = 2;
        const size_t target = std::max(capacity, size_t{2});
        while (phys < target) {
            if (phys > (std::numeric_limits<size_t>::max() / 2)) {
                throw std::length_error("LockFreeMessageQueue physical capacity is too large");
            }
            phys <<= 1;
        }
        return phys;
    }

    struct Entry {
        T data;
        uint64_t arrive_cycle;
        uint32_t sender_id;
    };

    const size_t capacity_;
    const size_t usable_capacity_;
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
 * This queue provides the registered-edge interface for single-thread
 * producer/consumer pairs.
 *
 * Features:
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
        // Model-visible capacity admission is enforced by Connection. Keeping
        // this queue permissive preserves same-cycle dequeue/enqueue semantics
        // for delay>0 register-style edges in single-thread execution.
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
        if (messages_.size() >= capacity_) {
            return 0;
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
 * Allows InPort to hold the selected registered-edge storage at runtime.
 */
template <typename T>
class IMessageQueue {
public:
    virtual ~IMessageQueue() = default;

    virtual bool push(T data, uint64_t arrive_cycle) = 0;
    virtual std::optional<T> tryPop(uint64_t current_cycle) = 0;
    virtual std::vector<T> popAll(uint64_t current_cycle) = 0;
    /// Reusable-buffer drain: avoids the fresh-vector allocation popAll() makes per call.
    virtual void popAllInto(std::vector<T>& out, uint64_t current_cycle) = 0;
    virtual bool hasReady(uint64_t current_cycle) const = 0;
    virtual std::optional<uint64_t> minArrivalCycle() const = 0;
    virtual bool empty() const = 0;
    virtual bool full() const = 0;
    virtual size_t size() const = 0;
    virtual size_t capacity() const noexcept = 0;
    virtual size_t storageCapacity() const noexcept { return capacity(); }
    virtual size_t available() const = 0;
    virtual size_t admissionOccupancy(uint64_t send_cycle) const {
        (void)send_cycle;
        return size();
    }
    virtual std::optional<uint64_t> admissionMinArrivalCycle(uint64_t send_cycle) const {
        (void)send_cycle;
        return minArrivalCycle();
    }
    virtual void setCapacity(size_t capacity) = 0;
    virtual void clear() = 0;
};

/**
 * QueueAdapterImpl - Generic adapter from a concrete queue to IMessageQueue.
 *
 * Works for any queue type that exposes: push(), tryPop(), popAll(),
 * hasReady(), minArrivalCycle(), empty(), full(), size(), capacity(),
 * storageCapacity(), available(), setCapacity(), clear().
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
    size_t storageCapacity() const noexcept override { return queue_.capacity(); }
    size_t available() const override { return queue_.available(); }
    void setCapacity(size_t capacity) override { queue_.setCapacity(capacity); }
    void clear() override { queue_.clear(); }

private:
    QueueImpl queue_;
};

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
    explicit LockFreeQueueAdapter(size_t capacity = LockFreeMessageQueue<T>::USABLE_CAPACITY,
                                  size_t min_usable_capacity = 0)
        : queue_(LockFreeMessageQueue<T>::physicalCapacityForUserCapacity(
              capacity == std::numeric_limits<size_t>::max()
                  ? min_usable_capacity
                  : std::max(capacity, min_usable_capacity))),
          user_capacity_(capacity == std::numeric_limits<size_t>::max()
                             ? queue_.usableCapacity()
                             : std::min(capacity, queue_.usableCapacity())) {}

    bool push(T data, uint64_t arrive_cycle) override {
        // Model-visible capacity admission is enforced by Connection. The ring
        // remains the physical overflow guard, while Connection supplies the
        // architectural back-pressure bound.
        if (!queue_.tryPush(std::move(data), arrive_cycle)) {
            return false;
        }
        admitted_pushes_.fetch_add(1, std::memory_order_release);
        return true;
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        return queue_.tryPopWithReleaseCallbacks(
            current_cycle,
            [this, current_cycle](uint64_t arrive_cycle) {
                recordPopArrivalForAdmission_(current_cycle, arrive_cycle);
            },
            [this, current_cycle](uint64_t) { recordPopCreditForAdmission_(current_cycle); });
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
        auto min = queue_.minArrivalCycle();
        return min.has_value() && *min <= current_cycle;
    }

    std::optional<uint64_t> minArrivalCycle() const override { return queue_.minArrivalCycle(); }

    bool empty() const override { return queue_.empty(); }
    bool full() const override { return queue_.size() >= user_capacity_; }
    size_t size() const override { return queue_.size(); }
    size_t capacity() const noexcept override { return user_capacity_; }
    size_t storageCapacity() const noexcept override { return queue_.usableCapacity(); }
    size_t available() const override {
        const size_t sz = queue_.size();
        return sz < user_capacity_ ? user_capacity_ - sz : 0;
    }
    size_t admissionOccupancy(uint64_t send_cycle) const override {
        const size_t pushed = admitted_pushes_.load(std::memory_order_acquire);
        std::lock_guard<std::mutex> lock(admission_mutex_);
        retireAdmissionHistoryBeforeLocked_(send_cycle);
        return pushed > popped_before_front_ ? pushed - popped_before_front_ : 0;
    }
    std::optional<uint64_t> admissionMinArrivalCycle(uint64_t send_cycle) const override {
        std::optional<uint64_t> min = queue_.minArrivalCycle();
        std::lock_guard<std::mutex> lock(admission_mutex_);
        retireAdmissionHistoryBeforeLocked_(send_cycle);
        if (!pop_arrivals_.empty() && pop_arrivals_.front().cycle == send_cycle) {
            if (!min.has_value() || pop_arrivals_.front().min_arrival < *min) {
                min = pop_arrivals_.front().min_arrival;
            }
        }
        return min;
    }
    void setCapacity(size_t capacity) override {
        user_capacity_ = std::min(capacity, queue_.usableCapacity());
    }
    void clear() override {
        queue_.clear();
        admitted_pushes_.store(0, std::memory_order_release);
        std::lock_guard<std::mutex> lock(admission_mutex_);
        pop_credits_.clear();
        pop_arrivals_.clear();
        popped_before_front_ = 0;
    }

private:
    friend struct LockFreeQueueAdapterTestAccess;

    struct PopCredit {
        uint64_t cycle;
        size_t count;
    };

    struct PopArrival {
        uint64_t cycle;
        uint64_t min_arrival;
    };

    void retirePopCreditsBeforeLocked_(uint64_t cycle) const {
        while (!pop_credits_.empty() && pop_credits_.front().cycle < cycle) {
            popped_before_front_ += pop_credits_.front().count;
            pop_credits_.pop_front();
        }
    }

    void retirePopArrivalsBeforeLocked_(uint64_t cycle) const {
        while (!pop_arrivals_.empty() && pop_arrivals_.front().cycle < cycle) {
            pop_arrivals_.pop_front();
        }
    }

    void retireAdmissionHistoryBeforeLocked_(uint64_t cycle) const {
        retirePopCreditsBeforeLocked_(cycle);
        retirePopArrivalsBeforeLocked_(cycle);
    }

    void recordPopCreditForAdmission_(uint64_t cycle) {
        std::lock_guard<std::mutex> lock(admission_mutex_);
        if (!pop_credits_.empty() && pop_credits_.back().cycle == cycle) {
            ++pop_credits_.back().count;
            return;
        }
        pop_credits_.push_back(PopCredit{cycle, 1});
    }

    void recordPopArrivalForAdmission_(uint64_t cycle, uint64_t arrive_cycle) {
        std::lock_guard<std::mutex> lock(admission_mutex_);
        if (!pop_arrivals_.empty() && pop_arrivals_.back().cycle == cycle) {
            pop_arrivals_.back().min_arrival =
                std::min(pop_arrivals_.back().min_arrival, arrive_cycle);
            return;
        }
        pop_arrivals_.push_back(PopArrival{cycle, arrive_cycle});
    }

    mutable LockFreeMessageQueue<T> queue_;
    size_t user_capacity_;
    std::atomic<size_t> admitted_pushes_{0};
    mutable std::mutex admission_mutex_;
    mutable std::deque<PopCredit> pop_credits_;
    mutable std::deque<PopArrival> pop_arrivals_;
    mutable size_t popped_before_front_ = 0;
};

#include "DirectSPSCQueueAdapter.hpp"
/**
 * MultiProducerQueueAdapter - Lock-free MPSC via independent SPSC queues.
 *
 * Chronon's scheduler registers one producer key per Connection, so each
 * registered edge has a dedicated staging queue into the consumer. This is
 * thread-safe because no two producers push the same physical queue, and the
 * consumer pops via a k-way merge keyed on (arrive_cycle, sender_id) for
 * deterministic, simulated-time-ordered delivery.
 *
 * Use when multiple threads write to the same InPort.
 *
 * Ordering guarantees:
 * - Primary key: arrive_cycle (lowest first).
 * - Tiebreak key: Connection::conn_id, supplied as sender_id, so same-cycle
 *   ordering remains stable across thread placements and worker counts.
 *
 * Each per-connection lane must be pushed with non-decreasing arrive_cycle;
 * fixed-delay OutPort connections satisfy this requirement.
 */
template <typename T>
class MultiProducerQueueAdapter : public IMessageQueue<T> {
public:
    /**
     * Construct an MPSC adapter with an initial user-visible capacity.
     *
     * The per-connection physical LockFreeMessageQueue ring capacity is chosen
     * at construction from the bounded user capacity. user_capacity_ is a
     * *soft* aggregate gate consulted by capacity()/full(); it is only
     * honored by the arbiter/canAccept layer before admitting a push. The
     * push path itself (pushFromThread) does NOT enforce user_capacity_ —
     * enforcing it there would reintroduce a wall-clock race where two
     * producer queues racing against each other could both see size < cap
     * and both succeed even though their joint push exceeds the soft cap.
     *
     * If the user does not override capacity explicitly, we still want
     * full() to never fire for a well-sized system, so the default
     * behaves like the legacy physical aggregate (effectively unlimited
     * for typical configurations).
     */
    explicit MultiProducerQueueAdapter(size_t capacity = std::numeric_limits<size_t>::max(),
                                       size_t min_per_thread_usable_capacity = 0)
        : per_thread_queue_capacity_(LockFreeMessageQueue<T>::physicalCapacityForUserCapacity(
              capacity == std::numeric_limits<size_t>::max()
                  ? min_per_thread_usable_capacity
                  : std::max(capacity, min_per_thread_usable_capacity))),
          user_capacity_(capacity),
          active_lane_tracking_requested_(queue_detail::experimentalMPSCActiveLanesEnabled()) {}

    void ensurePerThreadUsableCapacity(size_t min_usable_capacity) {
        if (min_usable_capacity == std::numeric_limits<size_t>::max()) {
            throw std::length_error("MultiProducerQueueAdapter per-thread capacity is too large");
        }
        const size_t requested =
            LockFreeMessageQueue<T>::physicalCapacityForUserCapacity(min_usable_capacity);
        if (requested <= per_thread_queue_capacity_) {
            return;
        }
        if (!thread_queues_.empty()) {
            for (const auto& queue : thread_queues_) {
                if (!queue->empty()) {
                    throw std::length_error(
                        "Cannot grow MultiProducerQueueAdapter while producer queues contain data");
                }
            }
            per_thread_queue_capacity_ = requested;
            for (auto& queue : thread_queues_) {
                queue = std::make_unique<LockFreeMessageQueue<T>>(per_thread_queue_capacity_);
            }
            for (auto& word : active_lane_words_) word.store(0, std::memory_order_relaxed);
            return;
        }
        per_thread_queue_capacity_ = requested;
    }

    /**
     * Register a stable producer key and create its queue.
     *
     * @param thread_id Stable producer key. Chronon passes conn_id + 1 here.
     * @return Queue ID for this producer key (used in pushFromThread)
     */
    size_t addProducerThread(size_t thread_id) {
        auto existing = thread_to_queue_id_.find(thread_id);
        if (existing != thread_to_queue_id_.end()) {
            return existing->second;
        }

        size_t id = thread_queues_.size();
        thread_queues_.push_back(
            std::make_unique<LockFreeMessageQueue<T>>(per_thread_queue_capacity_));
        if (active_lane_tracking_requested_ && id % 64 == 0) active_lane_words_.emplace_back(0);
        if (active_lane_tracking_requested_ &&
            thread_queues_.size() == kActiveLaneTrackingThreshold) {
            active_lane_tracking_enabled_ = true;
            for (size_t lane = 0; lane < id; ++lane) {
                if (!thread_queues_[lane]->empty()) markLaneActive_(lane);
            }
        }
        thread_to_queue_id_[thread_id] = id;
        return id;
    }

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
        const bool ok = thread_queues_[queue_id]->tryPush(std::move(data), arrive_cycle, sender_id);
        if (ok) markLaneActive_(queue_id);
        if (!ok) {
            // Physical SPSC ring full: the entry is dropped, which corrupts the
            // run. Under epoch-free lookahead a producer can run up to
            // max_lookahead_cycles ahead of a lagging consumer, so this counter
            // is the staging-overflow watchdog (each connection writes its own
            // queue_id, hence atomic). Relaxed: rare path, read after join.
            staging_overflow_events_.fetch_add(1, std::memory_order_relaxed);
        }
        return ok;
    }

    /// Count of dropped pushes due to a full physical staging ring. Nonzero
    /// means the lookahead window outran the configured ring capacity — a
    /// correctness failure, surfaced for the epoch-free A/B watchdog.
    uint64_t stagingOverflowEvents() const noexcept {
        return staging_overflow_events_.load(std::memory_order_relaxed);
    }

    bool push(T data, uint64_t arrive_cycle) override {
        if (thread_queues_.empty()) {
            return false;
        }
        const bool ok = thread_queues_[0]->tryPush(std::move(data), arrive_cycle);
        if (ok) markLaneActive_(0);
        return ok;
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
        forEachCandidateLane_([&](size_t i) {
            auto head = thread_queues_[i]->peekHead();
            if (!head.has_value()) {
                clearLaneIfEmpty_(i);
                return true;
            }
            if (head->first > current_cycle) return true;
            const uint64_t ac = head->first;
            const uint32_t sid = head->second;
            if (ac < best_ac || (ac == best_ac && sid < best_sid)) {
                best_ac = ac;
                best_sid = sid;
                best = i;
            }
            return true;
        });
        if (best == SIZE_MAX) return std::nullopt;
        auto value = thread_queues_[best]->tryPop(current_cycle);
        clearLaneIfEmpty_(best);
        return value;
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
        bool ready = false;
        forEachCandidateLane_([&](size_t i) {
            auto min = thread_queues_[i]->minArrivalCycle();
            if (min.has_value() && *min <= current_cycle) {
                ready = true;
                return false;
            }
            return true;
        });
        return ready;
    }

    std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> min;
        forEachCandidateLane_([&](size_t i) {
            auto cycle = thread_queues_[i]->minArrivalCycle();
            if (cycle.has_value()) {
                if (!min.has_value() || *cycle < *min) {
                    min = cycle;
                }
            }
            return true;
        });
        return min;
    }

    bool empty() const override {
        bool all_empty = true;
        forEachCandidateLane_([&](size_t i) {
            all_empty = thread_queues_[i]->empty();
            return all_empty;
        });
        return all_empty;
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
        forEachCandidateLane_([&](size_t i) {
            total += thread_queues_[i]->size();
            return true;
        });
        return total;
    }

    size_t capacity() const noexcept override { return user_capacity_; }
    size_t storageCapacity() const noexcept override { return perThreadUsableCapacity_(); }

    size_t available() const override {
        const size_t sz = size();
        return sz < user_capacity_ ? user_capacity_ - sz : 0;
    }

    /**
     * Set the soft user-visible capacity.
     *
     * Per-producer physical ring capacity is unchanged. Only the soft
     * aggregate gate is updated. pushFromThread() still does NOT enforce
     * this cap — per-producer ring fullness (fullForThread()) continues to
     * govern push failure on the push path to avoid wall-clock races
     * between producer threads. The MPSC arbiter consults full() before
     * admitting a message.
     */
    void setCapacity(size_t cap) override {
        if (cap != std::numeric_limits<size_t>::max() && cap > perThreadUsableCapacity_()) {
            ensurePerThreadUsableCapacity(cap);
        }
        user_capacity_ = cap;
    }

    void clear() override {
        for (auto& q : thread_queues_) {
            q->clear();
        }
        for (auto& word : active_lane_words_) word.store(0, std::memory_order_release);
        for (size_t i = 0; i < thread_queues_.size(); ++i) {
            if (!thread_queues_[i]->empty()) markLaneActive_(i);
        }
    }

private:
    static constexpr size_t kActiveLaneTrackingThreshold = 32;
    size_t perThreadUsableCapacity_() const noexcept { return per_thread_queue_capacity_ - 1; }

    void markLaneActive_(size_t lane) const noexcept {
        if (!active_lane_tracking_enabled_) return;
        active_lane_words_[lane / 64].fetch_or(uint64_t{1} << (lane % 64),
                                               std::memory_order_release);
    }

    void clearLaneIfEmpty_(size_t lane) noexcept {
        if (!active_lane_tracking_enabled_ || !thread_queues_[lane]->empty()) return;
        const uint64_t mask = uint64_t{1} << (lane % 64);
        active_lane_words_[lane / 64].fetch_and(~mask, std::memory_order_acq_rel);
        if (!thread_queues_[lane]->empty()) markLaneActive_(lane);
    }

    template <typename Visitor>
    bool forEachCandidateLane_(Visitor&& visitor) const {
        if (!active_lane_tracking_enabled_) {
            for (size_t i = 0; i < thread_queues_.size(); ++i) {
                if (!visitor(i)) return false;
            }
            return true;
        }
        for (size_t word_index = 0; word_index < active_lane_words_.size(); ++word_index) {
            uint64_t bits = active_lane_words_[word_index].load(std::memory_order_acquire);
            while (bits != 0) {
                const size_t lane = word_index * 64 + std::countr_zero(bits);
                if (lane < thread_queues_.size() && !visitor(lane)) return false;
                bits &= bits - 1;
            }
        }
        return true;
    }

    std::vector<std::unique_ptr<LockFreeMessageQueue<T>>> thread_queues_;
    mutable std::deque<std::atomic<uint64_t>> active_lane_words_;
    std::unordered_map<size_t, size_t> thread_to_queue_id_;  // thread_id -> queue_id
    size_t per_thread_queue_capacity_;
    size_t user_capacity_;  // Soft aggregate cap consulted by arbiter/full().
    const bool active_lane_tracking_requested_;
    bool active_lane_tracking_enabled_ = false;
    std::atomic<uint64_t> staging_overflow_events_{0};  // full-ring drops (watchdog)
};

}  // namespace chronon::sender
