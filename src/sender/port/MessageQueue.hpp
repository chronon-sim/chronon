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

inline bool directSPSCEnabled() noexcept {
    // Compatibility kill switch retained for A/B diagnosis. The direct ring
    // is the production default; only the exact value "0" selects the legacy
    // adapter.
    const char* value = std::getenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC");
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
    static constexpr size_t USABLE_CAPACITY = CAPACITY;

    explicit LockFreeMessageQueue(size_t physical_capacity = CAPACITY)
        : capacity_(roundUpPhysicalCapacity_(physical_capacity)),
          usable_capacity_(capacity_),
          buffer_(capacity_),
          head_(0),
          cached_tail_(0),
          tail_(0),
          cached_head_(0) {}

    static size_t physicalCapacityForUserCapacity(size_t user_capacity) {
        if (user_capacity == std::numeric_limits<size_t>::max()) {
            return CAPACITY;
        }
        return roundUpPhysicalCapacity_(std::max(CAPACITY, user_capacity));
    }

    /** Smallest power-of-two ring that can hold @p usable_capacity entries. */
    static size_t physicalCapacityForMinimumUsable(size_t usable_capacity) {
        if (usable_capacity == std::numeric_limits<size_t>::max()) {
            throw std::length_error("LockFreeMessageQueue requested capacity is too large");
        }
        return roundUpPhysicalCapacity_(usable_capacity);
    }

    /** Select compact storage for bounded edges and the default for unbounded ones. */
    static size_t physicalCapacityForConfiguration(size_t user_capacity,
                                                   size_t min_usable_capacity) {
        size_t requested = min_usable_capacity;
        if (user_capacity != std::numeric_limits<size_t>::max()) {
            requested = std::max(requested, user_capacity);
        }
        return requested == 0 ? physicalCapacityForUserCapacity(0)
                              : physicalCapacityForMinimumUsable(std::max(requested, size_t{2}));
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

        // Re-read the consumer's head_ (a cross-core load) only when the cached
        // copy says full; a stale cached_head_ is conservative (looks more full).
        // Absolute tickets avoid modulo ABA, make occupancy one subtraction,
        // and allow every physical slot to be used (no sentinel slot).
        if (tail - cached_head_ >= usable_capacity_) {
            cached_head_ = head_.load(std::memory_order_acquire);
            if (tail - cached_head_ >= usable_capacity_) {
                return false;
            }
        }

        buffer_[tail & (capacity_ - 1)] = {std::move(data), arrive_cycle, sender_id};
        tail_.store(tail + 1, std::memory_order_release);
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
        Entry& entry = buffer_[head & (capacity_ - 1)];
        if (entry.arrive_cycle > current_cycle) {
            return std::nullopt;
        }

        const uint64_t arrive_cycle = entry.arrive_cycle;
        T data = std::move(entry.data);
        before_release(arrive_cycle);
        head_.store(head + 1, std::memory_order_release);
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
        return buffer_[head & (capacity_ - 1)].arrive_cycle;
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
        const Entry& entry = buffer_[head & (capacity_ - 1)];
        return std::make_pair(entry.arrive_cycle, entry.sender_id);
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    size_t size() const noexcept {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        return tail - head;
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
    template <typename OnPop>
    std::optional<T> tryPopWithArrival(uint64_t current_cycle, OnPop&& on_pop) {
        if (messages_.empty()) {
            return std::nullopt;
        }

        const auto& top = messages_.top();
        if (top.arrive_cycle > current_cycle) {
            return std::nullopt;
        }

        const uint64_t arrive_cycle = top.arrive_cycle;
        T data = std::move(const_cast<InternalMessage&>(top).data);
        messages_.pop();
        std::forward<OnPop>(on_pop)(arrive_cycle);
        return data;
    }

    std::optional<T> tryPop(uint64_t current_cycle) {
        return tryPopWithArrival(current_cycle, [](uint64_t) noexcept {});
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

/**
 * SingleThreadQueueAdapter - cycle-strict registered-edge storage without
 * synchronization.
 *
 * A consumer pop at cycle C must not become producer admission credit until
 * C + 1, regardless of whether topology selection chooses same-thread, SPSC,
 * or MPSC transport. Consulting the live priority-queue size would otherwise
 * make model backpressure depend on the scheduler and host execution order.
 * Epoch-free workers can expose this directly when a floor-blocked local
 * cluster changes the normal sweep order; sequential SCC order can expose the
 * same boundary under sustained registered feedback.
 *
 * Bounded ports keep a ring of per-cycle pop summaries. Admission is the live
 * queue size plus pops whose simulated cycle has not become credit yet. There
 * are no atomics, locks, cache-line handoffs, push-side bookkeeping, or
 * per-message allocations; the uncommon history expansion only occurs when a
 * producer trails more distinct pop cycles than the small initialization
 * reserve can retain. Unbounded ports bypass the ledger entirely.
 */
template <typename T>
class SingleThreadQueueAdapter final : public IMessageQueue<T> {
private:
    struct PopCycle {
        uint64_t cycle = 0;
        uint64_t min_arrival = 0;
        size_t count = 0;
    };

    static constexpr size_t kExpandedHistorySlots = 4;

public:
    explicit SingleThreadQueueAdapter(
        size_t capacity = SingleThreadMessageQueue<T>::UNLIMITED_CAPACITY,
        bool cycle_strict_admission = false)
        : queue_(capacity),
          cycle_strict_admission_(cycle_strict_admission),
          track_admission_(cycle_strict_admission && capacity != unlimitedCapacity_()) {}

    bool push(T data, uint64_t arrive_cycle) override {
        return queue_.push(std::move(data), arrive_cycle);
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        if (!track_admission_) return queue_.tryPop(current_cycle);

        return queue_.tryPopWithArrival(current_cycle, [this, current_cycle](uint64_t arrival) {
            recordPop_(current_cycle, arrival);
        });
    }

    std::vector<T> popAll(uint64_t current_cycle) override {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        if (!track_admission_) {
            queue_.popAllInto(out, current_cycle);
            return;
        }

        out.clear();
        while (auto value = tryPop(current_cycle)) {
            out.push_back(std::move(*value));
        }
    }

    bool hasReady(uint64_t current_cycle) const override { return queue_.hasReady(current_cycle); }

    std::optional<uint64_t> minArrivalCycle() const override { return queue_.minArrivalCycle(); }

    bool empty() const override { return queue_.empty(); }
    bool full() const override { return queue_.full(); }
    size_t size() const override { return queue_.size(); }
    size_t capacity() const noexcept override { return queue_.capacity(); }
    size_t storageCapacity() const noexcept override { return queue_.capacity(); }
    size_t available() const override { return queue_.available(); }

    size_t admissionOccupancy(uint64_t send_cycle) const override {
        if (!track_admission_) return queue_.size();
        retirePopCyclesBefore_(send_cycle);
        const size_t live = queue_.size();
        const size_t pending = pendingPopCount_();
        return pending > std::numeric_limits<size_t>::max() - live
                   ? std::numeric_limits<size_t>::max()
                   : live + pending;
    }

    std::optional<uint64_t> admissionMinArrivalCycle(uint64_t send_cycle) const override {
        auto min = queue_.minArrivalCycle();
        if (!track_admission_) return min;

        retirePopCyclesBefore_(send_cycle);
        if (const PopCycle* event = oldestPendingPop_()) {
            if (!min || event->min_arrival < *min) {
                min = event->min_arrival;
            }
        }
        return min;
    }

    void setCapacity(size_t capacity) override {
        queue_.setCapacity(capacity);
        const bool should_track = cycle_strict_admission_ && capacity != unlimitedCapacity_();
        if (should_track == track_admission_) return;

        track_admission_ = should_track;
        resetAdmission_();
    }

    void clear() override {
        queue_.clear();
        resetAdmission_();
    }

private:
    static constexpr size_t unlimitedCapacity_() noexcept {
        return SingleThreadMessageQueue<T>::UNLIMITED_CAPACITY;
    }

    PopCycle& historyAt_(uint64_t ticket) noexcept {
        return expanded_history_[static_cast<size_t>(ticket) & pop_history_mask_];
    }

    const PopCycle& historyAt_(uint64_t ticket) const noexcept {
        return expanded_history_[static_cast<size_t>(ticket) & pop_history_mask_];
    }

    size_t pendingPopCount_() const noexcept {
        return expanded_history_active_ ? outstanding_pop_count_ : inline_pop_.count;
    }

    const PopCycle* oldestPendingPop_() const noexcept {
        if (!expanded_history_active_) {
            return inline_pop_.count == 0 ? nullptr : &inline_pop_;
        }
        return pop_history_head_ == pop_history_tail_ ? nullptr : &historyAt_(pop_history_head_);
    }

    void activateExpandedHistory_(uint64_t cycle, uint64_t arrival) {
        if (!expanded_history_) {
            expanded_history_ = std::make_unique<PopCycle[]>(kExpandedHistorySlots);
            pop_history_mask_ = kExpandedHistorySlots - 1;
        }
        expanded_history_[0] = inline_pop_;
        expanded_history_[1] = PopCycle{cycle, arrival, 1};
        pop_history_head_ = 0;
        pop_history_tail_ = 2;
        outstanding_pop_count_ = inline_pop_.count + 1;
        inline_pop_ = {};
        expanded_history_active_ = true;
    }

    void growHistory_() {
        const size_t old_size = pop_history_mask_ + 1;
        if (old_size > std::numeric_limits<size_t>::max() / 2) {
            throw std::length_error("same-thread admission history is too large");
        }
        const size_t new_size = old_size * 2;
        auto grown = std::make_unique<PopCycle[]>(new_size);
        const uint64_t live = pop_history_tail_ - pop_history_head_;
        for (uint64_t i = 0; i < live; ++i) {
            grown[static_cast<size_t>(i)] = historyAt_(pop_history_head_ + i);
        }
        expanded_history_ = std::move(grown);
        pop_history_mask_ = new_size - 1;
        pop_history_head_ = 0;
        pop_history_tail_ = live;
    }

    void recordPop_(uint64_t cycle, uint64_t arrival) {
        if (!expanded_history_active_) {
            if (inline_pop_.count == 0) {
                inline_pop_ = PopCycle{cycle, arrival, 1};
                return;
            }
            if (inline_pop_.cycle == cycle) {
                ++inline_pop_.count;
                inline_pop_.min_arrival = std::min(inline_pop_.min_arrival, arrival);
                return;
            }
            activateExpandedHistory_(cycle, arrival);
            return;
        }

        if (pop_history_head_ != pop_history_tail_) {
            PopCycle& last = historyAt_(pop_history_tail_ - 1);
            if (last.cycle == cycle) {
                ++last.count;
                last.min_arrival = std::min(last.min_arrival, arrival);
                ++outstanding_pop_count_;
                return;
            }
        }

        if (pop_history_tail_ - pop_history_head_ == pop_history_mask_ + 1) {
            growHistory_();
        }
        historyAt_(pop_history_tail_) = PopCycle{cycle, arrival, 1};
        ++pop_history_tail_;
        ++outstanding_pop_count_;
    }

    void retirePopCyclesBefore_(uint64_t cycle) const noexcept {
        if (!expanded_history_active_) {
            if (inline_pop_.count != 0 && inline_pop_.cycle < cycle) {
                inline_pop_ = {};
            }
            return;
        }

        while (pop_history_head_ != pop_history_tail_) {
            const PopCycle& event = historyAt_(pop_history_head_);
            if (event.cycle >= cycle) break;
            outstanding_pop_count_ -= event.count;
            ++pop_history_head_;
        }

        const uint64_t live = pop_history_tail_ - pop_history_head_;
        if (live <= 1) {
            inline_pop_ = live == 0 ? PopCycle{} : historyAt_(pop_history_head_);
            pop_history_head_ = 0;
            pop_history_tail_ = 0;
            outstanding_pop_count_ = 0;
            expanded_history_active_ = false;
        }
    }

    void resetAdmission_() noexcept {
        inline_pop_ = {};
        expanded_history_active_ = false;
        pop_history_head_ = 0;
        pop_history_tail_ = 0;
        outstanding_pop_count_ = 0;
    }

    SingleThreadMessageQueue<T> queue_;
    bool cycle_strict_admission_ = false;
    bool track_admission_ = false;
    mutable PopCycle inline_pop_{};
    std::unique_ptr<PopCycle[]> expanded_history_;
    mutable bool expanded_history_active_ = false;
    size_t pop_history_mask_ = kExpandedHistorySlots - 1;
    mutable uint64_t pop_history_head_ = 0;
    mutable uint64_t pop_history_tail_ = 0;
    mutable size_t outstanding_pop_count_ = 0;
};

/**
 * LockFreeQueueAdapter - Adapts LockFreeMessageQueue to IMessageQueue interface.
 *
 * Used for cross-thread SPSC (Single-Producer Single-Consumer) connections.
 * No mutex needed - uses atomic operations for thread-safe communication
 * between one producer thread and one consumer thread.
 */
template <typename T>
class LockFreeQueueAdapter final : public IMessageQueue<T> {
public:
    explicit LockFreeQueueAdapter(size_t capacity = LockFreeMessageQueue<T>::USABLE_CAPACITY,
                                  size_t min_usable_capacity = 0)
        : queue_(LockFreeMessageQueue<T>::physicalCapacityForConfiguration(capacity,
                                                                           min_usable_capacity)),
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
#include "MultiProducerQueueAdapter.hpp"

}  // namespace chronon::sender
