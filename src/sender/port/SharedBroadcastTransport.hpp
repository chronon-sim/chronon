// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronon::sender::detail {

/**
 * One-producer, many-consumer segmented queue used by transparent Port fanout.
 *
 * The producer stores each payload once. Every outgoing Connection owns an
 * independent cursor, so consumers replay the same immutable entry without
 * destructive queue pops. Full chunks are recycled after every consumer has
 * moved past them; if a consumer stops draining, new chunks are appended so an
 * unlimited InPort never gains model-visible backpressure.
 */
template <typename T>
class SharedBroadcastTransport {
    struct Entry {
        T data;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
    };

    struct Chunk {
        Chunk(size_t capacity, uint64_t base) : slots(capacity), base_sequence(base) {}

        void clear() {
            for (auto& slot : slots) slot.reset();
        }

        void reset(uint64_t base) {
            clear();
            base_sequence = base;
            next.store(nullptr, std::memory_order_relaxed);
        }

        std::vector<std::optional<Entry>> slots;
        uint64_t base_sequence = 0;
        std::atomic<Chunk*> next{nullptr};
    };

public:
    struct View {
        const T* data = nullptr;
        uint64_t sequence = 0;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
    };

    struct alignas(64) ConsumerCursor {
        // Only the owning InPort advances these hot fields. Producers need to
        // observe a cursor only when reclaiming a full chunk, so publishing
        // the chunk pointer at that rare boundary avoids two atomic cursor
        // operations per replayed message.
        uint64_t sequence = 0;
        uint64_t cached_tail = 0;
        Chunk* chunk = nullptr;
        std::atomic<Chunk*> published_chunk{nullptr};
    };

    SharedBroadcastTransport(size_t headroom_cycles, size_t messages_per_cycle)
        : headroom_cycles_(headroom_cycles), messages_per_cycle_(messages_per_cycle) {
        const auto capacity = slotCapacityFor_(headroom_cycles, messages_per_cycle);
        if (!capacity || !automaticAllocationEligible(headroom_cycles, messages_per_cycle)) {
            throw std::length_error("shared broadcast chunk capacity is too large");
        }
        chunk_capacity_ = *capacity;
        chunk_mask_ = chunk_capacity_ - 1;
        head_chunk_ = tail_chunk_ = new Chunk(chunk_capacity_, 0);
    }

    ~SharedBroadcastTransport() {
        Chunk* chunk = head_chunk_;
        while (chunk) {
            Chunk* next = chunk->next.load(std::memory_order_relaxed);
            delete chunk;
            chunk = next;
        }
    }

    SharedBroadcastTransport(const SharedBroadcastTransport&) = delete;
    SharedBroadcastTransport& operator=(const SharedBroadcastTransport&) = delete;

    [[nodiscard]] static bool automaticAllocationEligible(size_t headroom_cycles,
                                                          size_t messages_per_cycle) noexcept {
        const auto capacity = slotCapacityFor_(headroom_cycles, messages_per_cycle);
        if (!capacity) return false;
        constexpr size_t kMaxAutomaticSlotsPerChunk = 1u << 20;
        constexpr size_t kMaxAutomaticBytesPerChunk = 64u << 20;
        return *capacity <= kMaxAutomaticSlotsPerChunk &&
               sizeof(std::optional<Entry>) <= kMaxAutomaticBytesPerChunk / *capacity;
    }

    void registerConsumerCursor(ConsumerCursor* cursor) {
        if (!cursor) {
            throw std::invalid_argument("shared broadcast consumer cursor is null");
        }
        cursor->chunk = tail_chunk_;
        cursor->sequence = tail_.load(std::memory_order_relaxed);
        cursor->cached_tail = cursor->sequence;
        cursor->published_chunk.store(tail_chunk_, std::memory_order_relaxed);
        consumer_cursors_.push_back(cursor);
    }

    [[nodiscard]] bool canPublish() const noexcept { return true; }

    template <typename U>
    bool publish(U&& value, uint64_t send_cycle, uint32_t delay) {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == std::numeric_limits<uint64_t>::max()) {
            throw std::overflow_error("shared broadcast sequence overflow");
        }
        if (send_cycle > std::numeric_limits<uint64_t>::max() - delay) {
            throw std::overflow_error("shared broadcast arrival cycle overflow");
        }
        if (tail - tail_chunk_->base_sequence == chunk_capacity_) {
            appendChunk_(tail);
        }

        auto& slot = tail_chunk_->slots[tail & chunk_mask_];
        slot.emplace(Entry{.data = std::forward<U>(value),
                           .arrive_cycle = send_cycle + delay,
                           .enqueue_cycle = send_cycle});
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::optional<View> peek(ConsumerCursor& cursor) const noexcept {
        const uint64_t sequence = cursor.sequence;
        if (sequence >= cursor.cached_tail) {
            cursor.cached_tail = tail_.load(std::memory_order_acquire);
            if (sequence >= cursor.cached_tail) return std::nullopt;
        }

        Chunk* chunk = cursor.chunk;
        while (chunk) {
            if (sequence < chunk->base_sequence) return std::nullopt;
            if (sequence - chunk->base_sequence < chunk_capacity_) break;
            Chunk* next = chunk->next.load(std::memory_order_acquire);
            if (!next) return std::nullopt;
            cursor.chunk = next;
            cursor.published_chunk.store(next, std::memory_order_release);
            chunk = next;
        }
        if (!chunk) return std::nullopt;

        const auto& slot = chunk->slots[sequence & chunk_mask_];
        if (!slot.has_value()) return std::nullopt;
        return View{.data = &slot->data,
                    .sequence = sequence,
                    .arrive_cycle = slot->arrive_cycle,
                    .enqueue_cycle = slot->enqueue_cycle};
    }

    void advance(ConsumerCursor& cursor, uint64_t next_sequence) const noexcept {
        Chunk* chunk = cursor.chunk;
        while (chunk && next_sequence >= chunk->base_sequence &&
               next_sequence - chunk->base_sequence >= chunk_capacity_) {
            Chunk* next = chunk->next.load(std::memory_order_acquire);
            if (!next) break;
            cursor.chunk = next;
            cursor.published_chunk.store(next, std::memory_order_release);
            chunk = next;
        }
        cursor.sequence = next_sequence;
    }

    [[nodiscard]] uint64_t consumerSequence(const ConsumerCursor& cursor) const noexcept {
        return cursor.sequence;
    }

    [[nodiscard]] uint64_t publishedExclusive() const noexcept {
        return tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t headroomCycles() const noexcept { return headroom_cycles_; }
    [[nodiscard]] size_t messagesPerCycle() const noexcept { return messages_per_cycle_; }
    [[nodiscard]] size_t slotCapacity() const noexcept { return chunk_capacity_; }

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

    void appendChunk_(uint64_t base_sequence) {
        reclaimChunks_();
        Chunk* next;
        if (free_chunks_.empty()) {
            next = new Chunk(chunk_capacity_, base_sequence);
        } else {
            next = free_chunks_.back().release();
            free_chunks_.pop_back();
            next->reset(base_sequence);
        }
        tail_chunk_->next.store(next, std::memory_order_release);
        tail_chunk_ = next;
    }

    void reclaimChunks_() {
        while (head_chunk_ != tail_chunk_) {
            bool referenced = false;
            for (const auto* cursor : consumer_cursors_) {
                if (cursor->published_chunk.load(std::memory_order_acquire) == head_chunk_) {
                    referenced = true;
                    break;
                }
            }
            if (referenced) return;

            Chunk* retired = head_chunk_;
            head_chunk_ = retired->next.load(std::memory_order_acquire);
            retired->next.store(nullptr, std::memory_order_relaxed);
            retired->clear();
            free_chunks_.emplace_back(retired);
        }
    }

    size_t headroom_cycles_ = 0;
    size_t messages_per_cycle_ = 0;
    size_t chunk_capacity_ = 0;
    size_t chunk_mask_ = 0;
    Chunk* head_chunk_ = nullptr;
    Chunk* tail_chunk_ = nullptr;
    std::vector<std::unique_ptr<Chunk>> free_chunks_;
    std::vector<ConsumerCursor*> consumer_cursors_;
    alignas(64) std::atomic<uint64_t> tail_{0};
};

}  // namespace chronon::sender::detail
