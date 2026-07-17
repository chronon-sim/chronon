// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

// Included by MessageQueue.hpp after IMessageQueue and LockFreeMessageQueue
// are defined, inside chronon::sender.

/**
 * MultiProducerQueueAdapter - deterministic MPSC built from independent SPSC
 * lanes.
 *
 * A Chronon Connection has exactly one producer and an InPort has exactly one
 * consumer.  Keeping one SPSC lane per Connection avoids the contended tail,
 * slot-sequence CAS loops, and reclamation machinery of a general MPMC queue.
 * The consumer merges lane heads by (arrive_cycle, sender_id, lane_id), which
 * supplies the total order required for cycle-count reproducibility.
 *
 * Small fan-in uses a branch-friendly linear scan.  At larger fan-in,
 * producers publish lane activity into cache-line-separated shards, and the
 * consumer maintains a private min-heap containing at most one head per active
 * lane.  The activity bits are notifications, not ownership flags:
 * exchange(0) coalesces repeated pushes, and a lane is reinserted after every
 * pop while it remains non-empty.  This avoids the producer-set versus
 * consumer-clear lost-wakeup race of a persistent active-bit protocol.
 */
template <typename T>
class MultiProducerQueueAdapter final : public IMessageQueue<T> {
public:
    static constexpr size_t kFrontierLaneThreshold = 32;
    static constexpr size_t kLanesPerSignalWord = 64;

    explicit MultiProducerQueueAdapter(size_t capacity = std::numeric_limits<size_t>::max(),
                                       size_t min_per_thread_usable_capacity = 0)
        : per_thread_queue_capacity_(
              initialPhysicalCapacity_(capacity, min_per_thread_usable_capacity)),
          user_capacity_(capacity) {}

    void ensurePerThreadUsableCapacity(size_t min_usable_capacity) {
        if (min_usable_capacity == std::numeric_limits<size_t>::max()) {
            throw std::length_error("MultiProducerQueueAdapter per-thread capacity is too large");
        }
        const size_t requested =
            LockFreeMessageQueue<T>::physicalCapacityForMinimumUsable(min_usable_capacity);
        if (requested <= per_thread_queue_capacity_) return;

        for (const auto& queue : thread_queues_) {
            if (!queue->empty()) {
                throw std::length_error(
                    "Cannot grow MultiProducerQueueAdapter while producer queues contain data");
            }
        }
        per_thread_queue_capacity_ = requested;
        for (size_t lane = 0; lane < thread_queues_.size(); ++lane) {
            thread_queues_[lane] = std::make_unique<DirectSPSCQueueAdapter<T>>(
                per_thread_queue_capacity_, 0, lane_tracks_admission_[lane]);
        }
        resetFrontier_();
    }

    /** Register one stable producer key and return its lane id. */
    size_t addProducerThread(size_t thread_id) {
        return addProducerThread(thread_id, user_capacity_ != std::numeric_limits<size_t>::max());
    }

    size_t addProducerThread(size_t thread_id, bool track_admission) {
        auto existing = thread_to_queue_id_.find(thread_id);
        if (existing != thread_to_queue_id_.end()) {
            const size_t lane = existing->second;
            if (track_admission && !lane_tracks_admission_[lane]) {
                lane_tracks_admission_[lane] = true;
                thread_queues_[lane]->enableAdmissionTracking();
            }
            return lane;
        }

        const size_t id = thread_queues_.size();
        thread_queues_.push_back(std::make_unique<DirectSPSCQueueAdapter<T>>(
            per_thread_queue_capacity_, 0, track_admission));
        thread_to_queue_id_[thread_id] = id;
        lane_tracks_admission_.push_back(track_admission);
        lane_in_frontier_.push_back(false);
        if (frontier_.capacity() < thread_queues_.size()) {
            frontier_.reserve(std::max<size_t>(32, frontier_.capacity() * 2));
        }
        ensureSignalWord_(id);
        // Registration is initialization-only in Chronon, but preserving any
        // pre-existing low-level test traffic makes the scan-to-frontier
        // transition self-contained and unsurprising.
        if (thread_queues_.size() == kFrontierLaneThreshold) {
            for (size_t lane = 0; lane < thread_queues_.size(); ++lane) {
                if (!thread_queues_[lane]->empty()) signalLane_(lane);
            }
        }
        return id;
    }

    size_t getQueueIdForThread(size_t thread_id) const {
        auto it = thread_to_queue_id_.find(thread_id);
        return it == thread_to_queue_id_.end() ? SIZE_MAX : it->second;
    }

    bool fullForThread(size_t queue_id) const {
        if (queue_id >= thread_queues_.size()) return true;
        const size_t lane_size = thread_queues_[queue_id]->size();
        return lane_size >= user_capacity_ ||
               lane_size >= thread_queues_[queue_id]->storageCapacity();
    }

    bool storageFullForThread(size_t queue_id) const {
        return queue_id >= thread_queues_.size() ||
               thread_queues_[queue_id]->size() >= thread_queues_[queue_id]->storageCapacity();
    }

    size_t admissionOccupancyForThread(size_t queue_id, uint64_t send_cycle) const noexcept {
        return queue_id < thread_queues_.size()
                   ? thread_queues_[queue_id]->admissionOccupancy(send_cycle)
                   : 0;
    }

    std::optional<uint64_t> admissionMinArrivalCycleForThread(size_t queue_id,
                                                              uint64_t send_cycle) const noexcept {
        return queue_id < thread_queues_.size()
                   ? thread_queues_[queue_id]->admissionMinArrivalCycle(send_cycle)
                   : std::nullopt;
    }

    bool pushFromThread(size_t queue_id, T data, uint64_t arrive_cycle, uint32_t sender_id = 0) {
        if (queue_id >= thread_queues_.size()) return false;
        const bool ok =
            thread_queues_[queue_id]->pushDirect(std::move(data), arrive_cycle, sender_id);
        if (ok) {
            // Linear-scan fan-in has no consumer index to maintain, so it
            // also avoids the notification RMW on the producer fast path.
            if (thread_queues_.size() >= kFrontierLaneThreshold) {
                signalLane_(queue_id);
            }
        } else {
            transport_overflow_events_.fetch_add(1, std::memory_order_relaxed);
        }
        return ok;
    }

    uint64_t transportOverflowEvents() const noexcept {
        return transport_overflow_events_.load(std::memory_order_relaxed);
    }

    bool push(T data, uint64_t arrive_cycle) override {
        return !thread_queues_.empty() && pushFromThread(0, std::move(data), arrive_cycle, 0);
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        std::optional<T> result;
        consumeReady(current_cycle, [&result](T& data) { result.emplace(std::move(data)); });
        return result;
    }

    /**
     * Consumer-only direct path. The visitor sees the selected lane head while
     * its slot is still consumer-owned, then the lane publishes its new head.
     * This lets InPort apply receiver filtering without type erasure or moving
     * a rejected envelope.
     */
    template <typename Visitor>
    bool consumeReady(uint64_t current_cycle, Visitor&& visitor) {
        if (thread_queues_.size() < kFrontierLaneThreshold) {
            return consumeReadyByScan_(current_cycle, visitor);
        }
        return consumeReadyByFrontier_(current_cycle, visitor);
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
        if (thread_queues_.size() >= kFrontierLaneThreshold) {
            refreshFrontier_();
            return !frontier_.empty() && frontier_.front().arrive_cycle <= current_cycle;
        }
        for (const auto& queue : thread_queues_) {
            const auto cycle = queue->minArrivalCycle();
            if (cycle && *cycle <= current_cycle) return true;
        }
        return false;
    }

    std::optional<uint64_t> minArrivalCycle() const override {
        if (thread_queues_.size() >= kFrontierLaneThreshold) {
            refreshFrontier_();
            return frontier_.empty() ? std::nullopt
                                     : std::optional<uint64_t>{frontier_.front().arrive_cycle};
        }
        std::optional<uint64_t> result;
        for (const auto& queue : thread_queues_) {
            const auto cycle = queue->minArrivalCycle();
            if (cycle && (!result || *cycle < *result)) result = cycle;
        }
        return result;
    }

    bool empty() const override {
        if (thread_queues_.size() >= kFrontierLaneThreshold) {
            refreshFrontier_();
            return frontier_.empty();
        }
        for (const auto& queue : thread_queues_) {
            if (!queue->empty()) return false;
        }
        return true;
    }

    bool full() const override { return thread_queues_.empty() || size() >= user_capacity_; }

    size_t size() const override {
        size_t total = 0;
        for (const auto& queue : thread_queues_) total += queue->size();
        return total;
    }

    size_t capacity() const noexcept override { return user_capacity_; }
    size_t storageCapacity() const noexcept override { return perThreadUsableCapacity_(); }

    size_t available() const override {
        const size_t current = size();
        return current < user_capacity_ ? user_capacity_ - current : 0;
    }

    void setCapacity(size_t capacity) override {
        if (capacity != std::numeric_limits<size_t>::max() &&
            capacity > perThreadUsableCapacity_()) {
            ensurePerThreadUsableCapacity(capacity);
        }
        if (capacity != std::numeric_limits<size_t>::max()) {
            // setCapacity() is an initialization-time operation.  A port that
            // was registered while unbounded may become bounded afterwards;
            // make sure its lanes then participate in simulated-cycle credit
            // accounting as well.
            for (size_t lane = 0; lane < thread_queues_.size(); ++lane) {
                if (!lane_tracks_admission_[lane]) {
                    lane_tracks_admission_[lane] = true;
                    thread_queues_[lane]->enableAdmissionTracking();
                }
            }
        }
        user_capacity_ = capacity;
    }

    void clear() override {
        for (auto& queue : thread_queues_) queue->clear();

        // Drop notifications that referred to the cleared prefix, then scan
        // once.  A producer publishing before the scan is found by the scan;
        // one publishing afterwards leaves a fresh notification bit.
        resetFrontier_();
        for (size_t lane = 0; lane < thread_queues_.size(); ++lane) {
            activateLane_(lane);
        }
    }

    [[nodiscard]] bool usesActiveFrontier() const noexcept {
        return thread_queues_.size() >= kFrontierLaneThreshold;
    }

private:
    struct alignas(64) SignalWord {
        std::atomic<uint64_t> bits{0};
    };

    struct FrontierNode {
        uint64_t arrive_cycle;
        uint32_t sender_id;
        size_t lane_id;
    };

    struct FrontierLater {
        bool operator()(const FrontierNode& lhs, const FrontierNode& rhs) const noexcept {
            if (lhs.arrive_cycle != rhs.arrive_cycle) {
                return lhs.arrive_cycle > rhs.arrive_cycle;
            }
            if (lhs.sender_id != rhs.sender_id) return lhs.sender_id > rhs.sender_id;
            return lhs.lane_id > rhs.lane_id;
        }
    };

    void ensureSignalWord_(size_t lane_id) {
        const size_t word = lane_id / kLanesPerSignalWord;
        while (signal_words_.size() <= word) {
            signal_words_.push_back(std::make_unique<SignalWord>());
        }
    }

    void signalLane_(size_t lane_id) noexcept {
        const size_t word_index = lane_id / kLanesPerSignalWord;
        const uint64_t mask = uint64_t{1} << (lane_id % kLanesPerSignalWord);
        signal_words_[word_index]->bits.fetch_or(mask, std::memory_order_release);
    }

    void activateLane_(size_t lane_id) const {
        if (lane_id >= thread_queues_.size() || lane_in_frontier_[lane_id]) return;
        const auto head = thread_queues_[lane_id]->peekHead();
        if (!head) return;
        frontier_.push_back(FrontierNode{head->first, head->second, lane_id});
        std::push_heap(frontier_.begin(), frontier_.end(), FrontierLater{});
        lane_in_frontier_[lane_id] = true;
    }

    void refreshFrontier_() const {
        for (size_t word_index = 0; word_index < signal_words_.size(); ++word_index) {
            activateSignalWord_(word_index);
        }
    }

    void activateSignalWord_(size_t word_index) const {
        uint64_t lanes = signal_words_[word_index]->bits.exchange(0, std::memory_order_acquire);
        while (lanes != 0) {
            const unsigned lane_bit = std::countr_zero(lanes);
            const size_t lane = word_index * kLanesPerSignalWord + lane_bit;
            if (lane < thread_queues_.size()) activateLane_(lane);
            lanes &= lanes - 1;
        }
    }

    template <typename Visitor>
    bool consumeReadyByScan_(uint64_t current_cycle, Visitor& visitor) {
        size_t best = SIZE_MAX;
        uint64_t best_cycle = std::numeric_limits<uint64_t>::max();
        uint32_t best_sender = std::numeric_limits<uint32_t>::max();
        for (size_t lane = 0; lane < thread_queues_.size(); ++lane) {
            const auto head = thread_queues_[lane]->peekHead();
            if (!head || head->first > current_cycle) continue;
            if (head->first < best_cycle ||
                (head->first == best_cycle &&
                 (head->second < best_sender || (head->second == best_sender && lane < best)))) {
                best = lane;
                best_cycle = head->first;
                best_sender = head->second;
            }
        }
        return best != SIZE_MAX && thread_queues_[best]->consumeReady(current_cycle, visitor);
    }

    template <typename Visitor>
    bool consumeReadyByFrontier_(uint64_t current_cycle, Visitor& visitor) {
        refreshFrontier_();
        while (!frontier_.empty()) {
            if (frontier_.front().arrive_cycle > current_cycle) return false;

            std::pop_heap(frontier_.begin(), frontier_.end(), FrontierLater{});
            const FrontierNode node = frontier_.back();
            frontier_.pop_back();
            lane_in_frontier_[node.lane_id] = false;

            const bool consumed =
                thread_queues_[node.lane_id]->consumeReady(current_cycle, visitor);
            activateLane_(node.lane_id);
            if (consumed) return true;
            // A stale notification is harmless. Continue to the next head.
        }
        return false;
    }

    void resetFrontier_() noexcept {
        frontier_.clear();
        std::fill(lane_in_frontier_.begin(), lane_in_frontier_.end(), false);
        for (auto& word : signal_words_) {
            word->bits.exchange(0, std::memory_order_acquire);
        }
    }

    static size_t initialPhysicalCapacity_(size_t capacity, size_t min_usable_capacity) {
        size_t requested = min_usable_capacity;
        if (capacity != std::numeric_limits<size_t>::max()) {
            requested = std::max(requested, capacity);
        }
        // Unbounded ports retain the historical 4096-entry default. Bounded
        // or registered edges allocate only the power-of-two lane they need.
        return LockFreeMessageQueue<T>::physicalCapacityForConfiguration(
            requested == 0 ? std::numeric_limits<size_t>::max() : requested, 0);
    }

    size_t perThreadUsableCapacity_() const noexcept { return per_thread_queue_capacity_; }

    std::vector<std::unique_ptr<DirectSPSCQueueAdapter<T>>> thread_queues_;
    std::unordered_map<size_t, size_t> thread_to_queue_id_;
    std::vector<bool> lane_tracks_admission_;
    std::vector<std::unique_ptr<SignalWord>> signal_words_;
    mutable std::vector<bool> lane_in_frontier_;
    mutable std::vector<FrontierNode> frontier_;
    size_t per_thread_queue_capacity_;
    size_t user_capacity_;
    std::atomic<uint64_t> transport_overflow_events_{0};
};
