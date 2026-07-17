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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronon::sender::detail {

/**
 * One-producer, many-consumer message ring used by transparent Port fanout.
 *
 * The producer stores each payload once. Every outgoing Connection owns an
 * independent monotonically increasing cursor, so consumers replay the same
 * immutable entry without destructive queue pops. The producer only scans the
 * remote cursors when it is about to reuse the ring, keeping the normal send
 * path to one local write and one release publication.
 */
template <typename T>
class SharedBroadcastTransport {
    struct Entry {
        T data;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
    };

public:
    struct View {
        const T* data = nullptr;
        uint64_t sequence = 0;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
    };

    SharedBroadcastTransport(size_t headroom_cycles, size_t messages_per_cycle)
        : headroom_cycles_(headroom_cycles), messages_per_cycle_(messages_per_cycle) {
        const auto capacity = slotCapacityFor_(headroom_cycles, messages_per_cycle);
        if (!capacity || !automaticAllocationEligible(headroom_cycles, messages_per_cycle)) {
            throw std::length_error("shared broadcast capacity is too large");
        }
        slots_.resize(*capacity);
        mask_ = *capacity - 1;
    }

    [[nodiscard]] static bool automaticAllocationEligible(size_t headroom_cycles,
                                                          size_t messages_per_cycle) noexcept {
        const auto capacity = slotCapacityFor_(headroom_cycles, messages_per_cycle);
        if (!capacity) return false;
        constexpr size_t kMaxAutomaticSlots = 1u << 20;
        constexpr size_t kMaxAutomaticBytes = 64u << 20;
        return *capacity <= kMaxAutomaticSlots &&
               sizeof(std::optional<Entry>) <= kMaxAutomaticBytes / *capacity;
    }

    void registerConsumerCursor(std::atomic<uint64_t>* cursor) {
        if (!cursor) {
            throw std::invalid_argument("shared broadcast consumer cursor is null");
        }
        cursor->store(tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        consumer_cursors_.push_back(cursor);
    }

    [[nodiscard]] bool canPublish() const noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - cached_min_head_ < slots_.size()) {
            return true;
        }
        refreshMinHead_();
        return tail - cached_min_head_ < slots_.size();
    }

    template <typename U>
    bool publish(U&& value, uint64_t send_cycle, uint32_t delay) {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == std::numeric_limits<uint64_t>::max()) {
            throw std::overflow_error("shared broadcast sequence overflow");
        }
        if (tail - cached_min_head_ >= slots_.size()) {
            refreshMinHead_();
            if (tail - cached_min_head_ >= slots_.size()) {
                return false;
            }
        }
        if (send_cycle > std::numeric_limits<uint64_t>::max() - delay) {
            throw std::overflow_error("shared broadcast arrival cycle overflow");
        }

        auto& slot = slots_[tail & mask_];
        slot.emplace(Entry{.data = std::forward<U>(value),
                           .arrive_cycle = send_cycle + delay,
                           .enqueue_cycle = send_cycle});
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::optional<View> peek(uint64_t sequence) const noexcept {
        const uint64_t tail = tail_.load(std::memory_order_acquire);
        if (sequence >= tail) {
            return std::nullopt;
        }
        const auto& slot = slots_[sequence & mask_];
        if (!slot.has_value()) {
            return std::nullopt;
        }
        return View{.data = &slot->data,
                    .sequence = sequence,
                    .arrive_cycle = slot->arrive_cycle,
                    .enqueue_cycle = slot->enqueue_cycle};
    }

    [[nodiscard]] uint64_t publishedExclusive() const noexcept {
        return tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t headroomCycles() const noexcept { return headroom_cycles_; }
    [[nodiscard]] size_t messagesPerCycle() const noexcept { return messages_per_cycle_; }
    [[nodiscard]] size_t slotCapacity() const noexcept { return slots_.size(); }

private:
    [[nodiscard]] static std::optional<size_t> slotCapacityFor_(
        size_t headroom_cycles, size_t messages_per_cycle) noexcept {
        if (headroom_cycles == 0 || messages_per_cycle == 0 ||
            headroom_cycles > std::numeric_limits<size_t>::max() / messages_per_cycle) {
            return std::nullopt;
        }
        const size_t requested = headroom_cycles * messages_per_cycle;
        size_t capacity = 1;
        while (capacity < requested) {
            if (capacity > std::numeric_limits<size_t>::max() / 2) {
                return std::nullopt;
            }
            capacity <<= 1;
        }
        return capacity;
    }

    void refreshMinHead_() const noexcept {
        uint64_t min_head = tail_.load(std::memory_order_relaxed);
        for (const auto* cursor : consumer_cursors_) {
            min_head = std::min(min_head, cursor->load(std::memory_order_acquire));
        }
        cached_min_head_ = min_head;
    }

    size_t headroom_cycles_ = 0;
    size_t messages_per_cycle_ = 0;
    std::vector<std::optional<Entry>> slots_;
    size_t mask_ = 0;
    std::vector<std::atomic<uint64_t>*> consumer_cursors_;
    alignas(64) std::atomic<uint64_t> tail_{0};
    mutable uint64_t cached_min_head_ = 0;
};

}  // namespace chronon::sender::detail
