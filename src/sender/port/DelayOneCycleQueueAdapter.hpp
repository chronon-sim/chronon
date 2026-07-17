// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include "MessageQueue.hpp"

namespace chronon::sender {

namespace queue_detail {

inline bool experimentalDelayOneCycleQueueEnabled() noexcept {
    const char* value = std::getenv("CHRONON_EXPERIMENTAL_DELAY_ONE_CYCLE_QUEUE");
    return value == nullptr || value[0] != '0' || value[1] != '\0';
}

}  // namespace queue_detail

/**
 * Single-thread queue for one fixed-delay Connection.
 *
 * A producer's local cycle is monotonic, so a fixed delay makes arrival cycles
 * monotonic too. A reusable power-of-two slab ring can therefore preserve the
 * same arrival-cycle/FIFO order without maintaining a heap.
 */
template <typename T>
class DelayOneCycleQueueAdapter final : public IMessageQueue<T> {
public:
    static constexpr size_t UNLIMITED_CAPACITY = std::numeric_limits<size_t>::max();

    explicit DelayOneCycleQueueAdapter(size_t capacity = UNLIMITED_CAPACITY)
        : capacity_(capacity) {}

    bool push(T data, uint64_t arrive_cycle) override {
        if (slots_.empty()) slots_.resize(kInitialSlots);
        if (size_ != 0) {
            const size_t back = (tail_ - 1) & (slots_.size() - 1);
            if (arrive_cycle < slots_[back]->arrive_cycle) {
                throw std::logic_error("delay-one queue received a non-monotonic arrival cycle");
            }
        }
        if (size_ == slots_.size()) grow_();
        slots_[tail_].emplace(std::move(data), arrive_cycle);
        tail_ = (tail_ + 1) & (slots_.size() - 1);
        ++size_;
        return true;
    }

    std::optional<T> tryPop(uint64_t current_cycle) override {
        if (!hasReady(current_cycle)) return std::nullopt;
        T data = std::move(slots_[head_]->data);
        popFront_();
        return data;
    }

    std::vector<T> popAll(uint64_t current_cycle) override {
        std::vector<T> result;
        result.reserve(size_);
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<T>& out, uint64_t current_cycle) override {
        out.clear();
        while (hasReady(current_cycle)) {
            out.push_back(std::move(slots_[head_]->data));
            popFront_();
        }
    }

    bool hasReady(uint64_t current_cycle) const override {
        return size_ != 0 && slots_[head_]->arrive_cycle <= current_cycle;
    }

    std::optional<uint64_t> minArrivalCycle() const override {
        if (size_ == 0) return std::nullopt;
        return slots_[head_]->arrive_cycle;
    }

    bool empty() const override { return size_ == 0; }
    bool full() const override { return size_ >= capacity_; }
    size_t size() const override { return size_; }
    size_t capacity() const noexcept override { return capacity_; }
    size_t storageCapacity() const noexcept override { return slots_.size(); }

    size_t available() const override {
        if (capacity_ == UNLIMITED_CAPACITY) return UNLIMITED_CAPACITY;
        return size_ < capacity_ ? capacity_ - size_ : 0;
    }

    void setCapacity(size_t capacity) override { capacity_ = capacity; }

    void clear() override {
        while (size_ != 0) popFront_();
        head_ = 0;
        tail_ = 0;
    }

private:
    struct Entry {
        Entry(T&& value, uint64_t arrival) : data(std::move(value)), arrive_cycle(arrival) {}
        T data;
        uint64_t arrive_cycle;
    };

    static constexpr size_t kInitialSlots = 2;

    void popFront_() {
        slots_[head_].reset();
        head_ = (head_ + 1) & (slots_.size() - 1);
        --size_;
    }

    void grow_() {
        std::vector<std::optional<Entry>> grown(slots_.size() * 2);
        for (size_t i = 0; i < size_; ++i) {
            auto& entry = slots_[(head_ + i) & (slots_.size() - 1)];
            grown[i].emplace(std::move(entry->data), entry->arrive_cycle);
        }
        slots_.swap(grown);
        head_ = 0;
        tail_ = size_;
    }

    std::vector<std::optional<Entry>> slots_;
    size_t capacity_ = UNLIMITED_CAPACITY;
    size_t head_ = 0;
    size_t tail_ = 0;
    size_t size_ = 0;
};

}  // namespace chronon::sender
