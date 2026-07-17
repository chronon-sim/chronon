// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

// Included by MessageQueue.hpp after IMessageQueue, LockFreeMessageQueue,
// and LockFreeQueueAdapter are defined, inside chronon::sender.

/**
 * DirectSPSCQueueAdapter - registered-edge SPSC backend.
 *
 * Unlike LockFreeQueueAdapter, the physical queue and the admission ledger
 * share one monotonic producer ticket. A successful release-store of
 * tail_ticket_ therefore both publishes the payload and accounts for the
 * architectural push, avoiding the second locked RMW in the legacy adapter.
 * InPort may also push and consume the stored envelope directly, avoiding the
 * extra by-value virtual-interface hops on the common one-message path.
 *
 * The consumer still publishes each pop in three ordered stages required by
 * same-cycle registered-edge admission:
 *   1. arrival metadata, 2. reusable queue head, 3. admission credit.
 * A pop at cycle C is consequently not admission credit until cycle C + 1.
 */
template <typename T>
class DirectSPSCQueueAdapter final : public IMessageQueue<T> {
private:
    struct Entry {
        T data;
        uint64_t arrive_cycle;
        uint32_t sender_id;
    };

    struct PopEvent {
        uint64_t cycle;
        uint64_t arrive_cycle;
    };

public:
    explicit DirectSPSCQueueAdapter(size_t capacity = LockFreeMessageQueue<T>::USABLE_CAPACITY,
                                    size_t min_usable_capacity = 0, bool track_admission = true)
        : ring_capacity_(LockFreeMessageQueue<T>::physicalCapacityForConfiguration(
              capacity, min_usable_capacity)),
          ring_mask_(ring_capacity_ - 1),
          usable_capacity_(ring_capacity_),
          buffer_(ring_capacity_),
          user_capacity_(capacity == std::numeric_limits<size_t>::max()
                             ? usable_capacity_
                             : std::min(capacity, usable_capacity_)),
          track_admission_(track_admission),
          // At most one pop event exists for each payload publication that has
          // not yet been observed by the producer. A history ring equal to the
          // payload ring is therefore sufficient and keeps bounded lanes compact.
          pop_history_(ring_capacity_),
          pop_history_mask_(pop_history_.size() - 1) {}

    /** Initialization-only: enable simulated-cycle admission credit tracking. */
    void enableAdmissionTracking() noexcept { track_admission_ = true; }
    bool tracksAdmission() const noexcept { return track_admission_; }

    bool push(T data, uint64_t arrive_cycle) override {
        return pushDirect(std::move(data), arrive_cycle);
    }

    /** Producer-only direct path used by InPort after backend selection. */
    bool pushDirect(T&& data, uint64_t arrive_cycle, uint32_t sender_id = 0) {
        uint64_t tail = tail_ticket_.load(std::memory_order_relaxed);

        // cached_head_ticket_ is conservative: only refresh the cross-core
        // consumer head when the cached value makes the physical ring full.
        if (tail - cached_head_ticket_ >= usable_capacity_) {
            cached_head_ticket_ = head_ticket_.load(std::memory_order_acquire);
            if (tail - cached_head_ticket_ >= usable_capacity_) {
                return false;
            }
        }

        Entry& entry = buffer_[tail & ring_mask_];
        entry.data = std::move(data);
        entry.arrive_cycle = arrive_cycle;
        entry.sender_id = sender_id;

        // This single publication is both queue visibility and the admission
        // push ticket observed by admissionOccupancy().
        tail_ticket_.store(tail + 1, std::memory_order_release);
        return true;
    }

    /**
     * Consumer-only ready-entry view. The returned pointer remains owned by
     * the consumer until consumePeeked() publishes the new head. This split
     * lets typed InPort paths decide cancellation before constructing a large
     * optional return object.
     */
    T* peekReady(uint64_t current_cycle) {
        uint64_t head = head_ticket_.load(std::memory_order_relaxed);

        if (head == cached_tail_ticket_) {
            cached_tail_ticket_ = tail_ticket_.load(std::memory_order_acquire);
            if (head == cached_tail_ticket_) {
                return nullptr;
            }
        }

        Entry& entry = buffer_[head & ring_mask_];
        if (entry.arrive_cycle > current_cycle) {
            return nullptr;
        }

        return &entry.data;
    }

    /** Publish consumption of the ready entry returned by peekReady(). */
    void consumePeeked(uint64_t current_cycle) {
        const uint64_t head = head_ticket_.load(std::memory_order_relaxed);
        Entry& entry = buffer_[head & ring_mask_];

        const uint64_t arrive_cycle = entry.arrive_cycle;
        const bool track_admission = track_admission_;
        if (track_admission) {
            recordPopArrivalForAdmission_(current_cycle, arrive_cycle);
        }
        head_ticket_.store(head + 1, std::memory_order_release);
        if (track_admission) {
            recordPopCreditForAdmission_();
        }
    }

    /**
     * Consumer-only direct path. The visitor runs while the slot is owned by
     * the consumer and may move from or discard the payload. Returning true
     * means one ready entry was consumed, even if the visitor discarded it.
     */
    template <typename Visitor>
    bool consumeReady(uint64_t current_cycle, Visitor&& visitor) {
        T* data = peekReady(current_cycle);
        if (!data) {
            return false;
        }
        std::forward<Visitor>(visitor)(*data);
        consumePeeked(current_cycle);
        return true;
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        std::optional<T> result;
        consumeReady(current_cycle, [&result](T& data) { result.emplace(std::move(data)); });
        return result;
    }

    std::vector<T> popAll(uint64_t current_cycle) override {
        std::vector<T> result;
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        out.clear();
        while (auto value = tryPop(current_cycle)) {
            out.push_back(std::move(*value));
        }
    }

    bool hasReady(uint64_t current_cycle) const override {
        auto min = minArrivalCycle();
        return min.has_value() && *min <= current_cycle;
    }

    std::optional<uint64_t> minArrivalCycle() const override {
        const uint64_t head = head_ticket_.load(std::memory_order_acquire);
        if (head == tail_ticket_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return buffer_[head & ring_mask_].arrive_cycle;
    }

    std::optional<std::pair<uint64_t, uint32_t>> peekHead() const {
        const uint64_t head = head_ticket_.load(std::memory_order_acquire);
        if (head == tail_ticket_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        const Entry& entry = buffer_[head & ring_mask_];
        return std::make_pair(entry.arrive_cycle, entry.sender_id);
    }

    bool empty() const override {
        return head_ticket_.load(std::memory_order_acquire) ==
               tail_ticket_.load(std::memory_order_acquire);
    }

    bool full() const override { return size() >= user_capacity_; }

    size_t size() const override {
        const uint64_t head = head_ticket_.load(std::memory_order_acquire);
        const uint64_t tail = tail_ticket_.load(std::memory_order_acquire);
        return static_cast<size_t>(tail - head);
    }

    size_t capacity() const noexcept override { return user_capacity_; }
    size_t storageCapacity() const noexcept override { return usable_capacity_; }

    size_t available() const override {
        const size_t current_size = size();
        return current_size < user_capacity_ ? user_capacity_ - current_size : 0;
    }

    size_t admissionOccupancy(uint64_t send_cycle) const override {
        for (;;) {
            const uint64_t clear_sequence = beginAdmissionRead_();
            const uint64_t tail = tail_ticket_.load(std::memory_order_acquire);
            const uint64_t pushed = tail - producer_clear_tail_baseline_;
            uint64_t pop_head = producer_pop_head_;
            size_t popped_before = popped_before_front_;
            retireAdmissionHistoryBefore_(send_cycle, pop_head, popped_before);
            const size_t occupancy =
                pushed > popped_before ? static_cast<size_t>(pushed - popped_before) : 0;
            if (clear_sequence_.load(std::memory_order_acquire) == clear_sequence) {
                commitAdmissionCursor_(pop_head, popped_before);
                return occupancy;
            }
        }
    }

    std::optional<uint64_t> admissionMinArrivalCycle(uint64_t send_cycle) const override {
        for (;;) {
            const uint64_t clear_sequence = beginAdmissionRead_();

            // As in LockFreeQueueAdapter, observe the queue head before the
            // history tail so a concurrent pop is represented by one or both,
            // never by neither.
            std::optional<uint64_t> min = minArrivalCycle();
            uint64_t pop_head = producer_pop_head_;
            size_t popped_before = popped_before_front_;
            retireAdmissionHistoryBefore_(send_cycle, pop_head, popped_before);

            const uint64_t arrival_tail = pop_arrival_tail_.load(std::memory_order_acquire);
            for (uint64_t pos = pop_head; pos < arrival_tail; ++pos) {
                const PopEvent& event = pop_history_[pos & pop_history_mask_];
                if (event.cycle == send_cycle && (!min.has_value() || event.arrive_cycle < *min)) {
                    min = event.arrive_cycle;
                }
            }
            if (clear_sequence_.load(std::memory_order_acquire) == clear_sequence) {
                commitAdmissionCursor_(pop_head, popped_before);
                return min;
            }
        }
    }

    void setCapacity(size_t capacity) override {
        user_capacity_ = std::min(capacity, usable_capacity_);
    }

    void clear() override {
        // Consumer-side clear. The tail baseline replaces resetting a shared
        // push counter, so post-clear producer publications remain monotonic.
        clear_sequence_.fetch_add(1, std::memory_order_acq_rel);
        const uint64_t tail = tail_ticket_.load(std::memory_order_acquire);
        head_ticket_.store(tail, std::memory_order_release);
        cached_tail_ticket_ = tail;
        clear_tail_baseline_.store(tail, std::memory_order_release);

        const uint64_t discard_before = pop_arrival_tail_.load(std::memory_order_relaxed);
        pop_discard_before_.store(discard_before, std::memory_order_release);
        clear_sequence_.fetch_add(1, std::memory_order_release);
    }

private:
    uint64_t beginAdmissionRead_() const {
        uint64_t sequence;
        do {
            sequence = clear_sequence_.load(std::memory_order_acquire);
        } while ((sequence & 1U) != 0);

        if (sequence != producer_clear_sequence_) {
            const uint64_t discard_before = pop_discard_before_.load(std::memory_order_acquire);
            producer_pop_head_ = std::max(producer_pop_head_, discard_before);
            pop_history_head_.store(producer_pop_head_, std::memory_order_release);
            producer_clear_tail_baseline_ = clear_tail_baseline_.load(std::memory_order_acquire);
            popped_before_front_ = 0;
            producer_clear_sequence_ = sequence;
        }
        return sequence;
    }

    void retireAdmissionHistoryBefore_(uint64_t cycle, uint64_t& pop_head,
                                       size_t& popped_before) const {
        const uint64_t discard_before = pop_discard_before_.load(std::memory_order_acquire);
        pop_head = std::max(pop_head, discard_before);

        const uint64_t credit_tail = pop_credit_tail_.load(std::memory_order_acquire);
        while (pop_head < credit_tail) {
            const PopEvent& event = pop_history_[pop_head & pop_history_mask_];
            if (event.cycle >= cycle) {
                break;
            }
            ++popped_before;
            ++pop_head;
        }
    }

    void commitAdmissionCursor_(uint64_t pop_head, size_t popped_before) const {
        producer_pop_head_ = pop_head;
        popped_before_front_ = popped_before;
        pop_history_head_.store(pop_head, std::memory_order_release);
    }

    void recordPopArrivalForAdmission_(uint64_t cycle, uint64_t arrive_cycle) {
        const uint64_t tail = pop_arrival_tail_.load(std::memory_order_relaxed);
        if (tail - cached_pop_history_head_ >= pop_history_.size()) {
            cached_pop_history_head_ = pop_history_head_.load(std::memory_order_acquire);
            if (tail - cached_pop_history_head_ >= pop_history_.size()) {
                admission_history_overflow_events_.fetch_add(1, std::memory_order_relaxed);
                std::terminate();
            }
        }

        pop_history_[tail & pop_history_mask_] = PopEvent{cycle, arrive_cycle};
        pop_arrival_tail_.store(tail + 1, std::memory_order_release);
    }

    void recordPopCreditForAdmission_() {
        const uint64_t arrival_tail = pop_arrival_tail_.load(std::memory_order_relaxed);
        pop_credit_tail_.store(arrival_tail, std::memory_order_release);
    }

    const size_t ring_capacity_;
    const size_t ring_mask_;
    const size_t usable_capacity_;
    std::vector<Entry> buffer_;
    size_t user_capacity_;

    // Absolute tickets remove modulo ABA and let tail_ticket_ double as the
    // producer's admission publication.
    alignas(64) std::atomic<uint64_t> head_ticket_{0};
    uint64_t cached_tail_ticket_ = 0;
    bool track_admission_ = true;
    alignas(64) std::atomic<uint64_t> tail_ticket_{0};
    uint64_t cached_head_ticket_ = 0;

    std::vector<PopEvent> pop_history_;
    size_t pop_history_mask_;
    alignas(64) mutable std::atomic<uint64_t> pop_history_head_{0};
    alignas(64) std::atomic<uint64_t> pop_arrival_tail_{0};
    std::atomic<uint64_t> pop_credit_tail_{0};
    std::atomic<uint64_t> admission_history_overflow_events_{0};

    // Producer admission queries read these clear-only fields for every
    // first send in a simulated cycle. Isolate them from the consumer's hot
    // pop-arrival/credit publications to avoid cross-core false sharing.
    alignas(64) std::atomic<uint64_t> clear_sequence_{0};
    std::atomic<uint64_t> pop_discard_before_{0};
    std::atomic<uint64_t> clear_tail_baseline_{0};

    uint64_t cached_pop_history_head_ = 0;
    mutable uint64_t producer_pop_head_ = 0;
    mutable uint64_t producer_clear_sequence_ = 0;
    mutable uint64_t producer_clear_tail_baseline_ = 0;
    mutable size_t popped_before_front_ = 0;
};
