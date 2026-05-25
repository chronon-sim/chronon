// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "FormatRegistry.hpp"
#include "ObservationQueue.hpp"

namespace chronon::observe {

/**
 * @brief A buffered observation event with extracted cycle for sorting.
 *
 * Stores an offset/size into a shared arena rather than owning heap memory;
 * the cycle is cached for O(1) comparisons.
 */
struct BufferedRecord {
    uint64_t cycle;
    ObservationQueue::EventType type;
    uint32_t data_offset;
    uint32_t data_size;

    BufferedRecord() = default;

    BufferedRecord(uint64_t c, ObservationQueue::EventType t, uint32_t offset,
                   uint32_t size) noexcept
        : cycle(c), type(t), data_offset(offset), data_size(size) {}

    bool operator<(const BufferedRecord& other) const noexcept { return cycle < other.cycle; }
};

/**
 * @brief Copy of the arena region referenced by a batch of flushed records.
 *
 * Enables double-buffered I/O: the drain thread snapshots referenced data so
 * compactArena() can run immediately while I/O proceeds on the snapshot.
 */
struct ArenaSnapshot {
    std::vector<std::byte> data;
    uint32_t base_offset = 0;  ///< Offset adjustment for records into @c data.
};

/** @brief ReorderBuffer configuration. */
struct ReorderBufferConfig {
    uint64_t watermark_cycles = 1000;   ///< Flush delay (cycles behind min).
    size_t max_buffer_events = 100000;  ///< Force-flush threshold.
    size_t initial_arena_size = 4 * 1024 * 1024;
};

/**
 * @brief Cycle-sorted buffer for observation events backed by an arena.
 *
 * No per-event heap allocations; flushes use a hybrid watermark strategy:
 * 1. Primary (cycle-based): events with cycle < (min_cycle - watermark_cycles) are
 *    safe to flush. During warm-up (min_cycle <= watermark_cycles) cycle-based
 *    flushing pauses to preserve the safety gap for out-of-order events.
 * 2. Secondary (count-based): once the buffer exceeds max_buffer_events, the oldest
 *    half is force-flushed regardless of cycle — a safety valve against runaway growth.
 * 3. Shutdown: flushAll() sorts and returns whatever remains.
 *
 * Producer hot path is untouched; all complexity lives on the consumer side.
 */
class ReorderBuffer {
public:
    using Config = ReorderBufferConfig;

    explicit ReorderBuffer(const Config& config = Config{}) : config_(config) {
        sorted_buffer_.reserve(config_.max_buffer_events / 2);
        unsorted_batch_.reserve(config_.max_buffer_events / 4);
        arena_.resize(config_.initial_arena_size);
    }

    /// Appends record into the arena. @return false if cycle extraction failed.
    bool bufferEvent(const ObservationQueue::RecordHeader* header, const std::byte* data,
                     size_t data_size);

    /**
     * @brief Update the minimum observed cycle across producer threads.
     *
     * Events with cycle < (min_cycle - watermark_cycles) become eligible for flush.
     */
    void updateMinCycle(uint64_t min_cycle) noexcept { min_cycle_ = min_cycle; }

    /**
     * @brief Flush events below the watermark, sorted by cycle.
     *
     * IMPORTANT: returned records reference arena data; the caller MUST call
     * compactArena() after fully processing them to reclaim arena space.
     */
    void flushReady(std::vector<BufferedRecord>& out);

    /**
     * @brief Flush every remaining event, sorted by cycle. Use at shutdown.
     *
     * Clears the internal buffer; arena data remains valid until the next
     * bufferEvent() or compactArena() call.
     */
    void flushAll(std::vector<BufferedRecord>& out);

    /// PRECONDITION: flushed records have been fully consumed.
    void compactArena();

    size_t size() const noexcept {
        return (sorted_buffer_.size() - sorted_start_) + unsorted_batch_.size();
    }

    bool empty() const noexcept {
        return sorted_start_ == sorted_buffer_.size() && unsorted_batch_.empty();
    }

    uint64_t minCycle() const noexcept { return min_cycle_; }

    const Config& config() const noexcept { return config_; }

    const std::byte* arenaData(uint32_t offset) const noexcept { return arena_.data() + offset; }

    /// Copies only the contiguous range [min_offset, max_end) referenced by @p records.
    ArenaSnapshot snapshotArena(const std::vector<BufferedRecord>& records) const;

private:
    /// @param[out] cycle Extracted cycle value. @return false on malformed data.
    static bool extractCycle(ObservationQueue::EventType type, const std::byte* data,
                             size_t data_size, uint64_t& cycle);

    /// Doubles capacity if needed.
    void ensureArenaCapacity_(size_t additional);

    void compactArenaImpl_();

    Config config_;
    std::vector<BufferedRecord> sorted_buffer_;
    /// Start index avoids erase-from-front cost.
    size_t sorted_start_ = 0;
    std::vector<BufferedRecord> unsorted_batch_;
    uint64_t min_cycle_ = 0;

    std::vector<std::byte> arena_;
    size_t arena_write_pos_ = 0;
    /// Tracked incrementally in bufferEvent() to avoid recomputing on every flush.
    uint32_t arena_max_end_ = 0;
};

}  // namespace chronon::observe
