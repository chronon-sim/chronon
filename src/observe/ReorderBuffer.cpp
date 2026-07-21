// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "ReorderBuffer.hpp"

// libstdc++12's std::stable_sort/inplace_merge use deprecated
// std::get_temporary_buffer; suppress for this TU until libstdc++13+ is required.
#if defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

#include <algorithm>
#include <cstring>

namespace chronon::observe {

bool ReorderBuffer::extractCycle(ObservationQueue::EventType type, const std::byte* data,
                                 size_t data_size, uint64_t& cycle) {
    switch (type) {
        case ObservationQueue::EventType::LOG_EVENT:
        case ObservationQueue::EventType::TIMELINE_EVENT:
        case ObservationQueue::EventType::COUNTER_SNAPSHOT: {
            // StructuredRecord, TimelineRecord, and counter snapshots all lead
            // with a uint64_t cycle.
            if (data_size >= sizeof(uint64_t)) {
                std::memcpy(&cycle, data, sizeof(uint64_t));
                return true;
            }
            return false;
        }

        case ObservationQueue::EventType::EPOCH_COMMIT:
        case ObservationQueue::EventType::EPOCH_ROLLBACK:
        case ObservationQueue::EventType::SHUTDOWN:
            // No cycle on these markers; sort them to the front.
            cycle = 0;
            return true;
    }

    return false;
}

void ReorderBuffer::ensureArenaCapacity_(size_t additional) {
    size_t required = arena_write_pos_ + additional;
    if (required <= arena_.size()) {
        return;
    }
    size_t new_size = arena_.size();
    while (new_size < required) {
        new_size *= 2;
    }
    arena_.resize(new_size);
}

void ReorderBuffer::compactArena() { compactArenaImpl_(); }

void ReorderBuffer::compactArenaImpl_() {
    // sorted_buffer_ AND unsorted_batch_ may reference arena data — the caller
    // can drain producer queues between flushReady() and compactArena().
    if (sorted_start_ == sorted_buffer_.size() && unsorted_batch_.empty()) {
        arena_write_pos_ = 0;
        arena_max_end_ = 0;
        return;
    }

    // sorted_buffer_ is sorted by cycle, not by data_offset, so we must scan.
    uint32_t min_offset = UINT32_MAX;
    for (size_t i = sorted_start_; i < sorted_buffer_.size(); ++i) {
        if (sorted_buffer_[i].data_offset < min_offset) {
            min_offset = sorted_buffer_[i].data_offset;
        }
    }
    for (const auto& rec : unsorted_batch_) {
        if (rec.data_offset < min_offset) {
            min_offset = rec.data_offset;
        }
    }

    if (min_offset == 0 || min_offset == UINT32_MAX) {
        return;
    }

    // Skip the memmove when dead space is small; the next call will reclaim more at once.
    if (min_offset < arena_write_pos_ / 2) {
        return;
    }

    size_t remaining_size = arena_max_end_ - min_offset;
    std::memmove(arena_.data(), arena_.data() + min_offset, remaining_size);

    for (size_t i = sorted_start_; i < sorted_buffer_.size(); ++i) {
        sorted_buffer_[i].data_offset -= min_offset;
    }
    for (auto& rec : unsorted_batch_) {
        rec.data_offset -= min_offset;
    }

    arena_write_pos_ = remaining_size;
    arena_max_end_ -= min_offset;
}

bool ReorderBuffer::bufferEvent(const ObservationQueue::RecordHeader* header, const std::byte* data,
                                size_t data_size) {
    uint64_t cycle = 0;
    if (!extractCycle(header->type, data, data_size, cycle)) {
        return false;
    }

    size_t total_size = sizeof(ObservationQueue::RecordHeader) + data_size;
    ensureArenaCapacity_(total_size);

    uint32_t offset = static_cast<uint32_t>(arena_write_pos_);

    std::memcpy(arena_.data() + arena_write_pos_, header, sizeof(ObservationQueue::RecordHeader));
    arena_write_pos_ += sizeof(ObservationQueue::RecordHeader);

    if (data_size > 0) {
        std::memcpy(arena_.data() + arena_write_pos_, data, data_size);
        arena_write_pos_ += data_size;
    }

    // Track incrementally so compactArena() doesn't need a full scan.
    uint32_t record_end = offset + static_cast<uint32_t>(total_size);
    if (record_end > arena_max_end_) {
        arena_max_end_ = record_end;
    }

    unsorted_batch_.emplace_back(cycle, header->type, offset, static_cast<uint32_t>(total_size));
    return true;
}

void ReorderBuffer::flushReady(std::vector<BufferedRecord>& out) {
    out.clear();

    bool has_sorted = sorted_start_ < sorted_buffer_.size();
    if (!has_sorted && unsorted_batch_.empty()) {
        return;
    }

    // Maintain a full safety window during warm-up by only flushing once
    // min_cycle has moved past the watermark.
    uint64_t flush_threshold = 0;
    if (min_cycle_ > config_.watermark_cycles) {
        flush_threshold = min_cycle_ - config_.watermark_cycles;
    }

    // Safety valve against unbounded accumulation.
    bool force_flush = size() > config_.max_buffer_events;

    if (flush_threshold == 0 && !force_flush) {
        return;
    }

    if (!unsorted_batch_.empty()) {
        std::stable_sort(unsorted_batch_.begin(), unsorted_batch_.end());

        if (!has_sorted) {
            sorted_buffer_.clear();
            sorted_start_ = 0;
            sorted_buffer_ = std::move(unsorted_batch_);
        } else {
            // Drop the dead prefix before merging to avoid stepping over garbage.
            if (sorted_start_ > 0) {
                sorted_buffer_.erase(
                    sorted_buffer_.begin(),
                    sorted_buffer_.begin() + static_cast<std::ptrdiff_t>(sorted_start_));
                sorted_start_ = 0;
            }
            auto merge_point = static_cast<ptrdiff_t>(sorted_buffer_.size());
            sorted_buffer_.insert(sorted_buffer_.end(),
                                  std::make_move_iterator(unsorted_batch_.begin()),
                                  std::make_move_iterator(unsorted_batch_.end()));
            std::inplace_merge(sorted_buffer_.begin(), sorted_buffer_.begin() + merge_point,
                               sorted_buffer_.end());
        }
        unsorted_batch_.clear();
    }

    auto begin_it = sorted_buffer_.begin() + static_cast<std::ptrdiff_t>(sorted_start_);
    size_t live_count = static_cast<size_t>(sorted_buffer_.end() - begin_it);

    size_t flush_count = 0;

    if (flush_threshold > 0) {
        auto threshold_it = std::lower_bound(
            begin_it, sorted_buffer_.end(), flush_threshold,
            [](const BufferedRecord& rec, uint64_t thresh) { return rec.cycle < thresh; });
        flush_count = static_cast<size_t>(std::distance(begin_it, threshold_it));
    }

    // When over capacity, drain at least half even if cycle threshold says less.
    if (force_flush && flush_count < live_count / 2) {
        flush_count = live_count > 1 ? live_count / 2 : live_count;
    }

    if (flush_count == 0) {
        return;
    }

    out.reserve(flush_count);
    out.assign(begin_it, begin_it + static_cast<std::ptrdiff_t>(flush_count));

    sorted_start_ += flush_count;

    if (sorted_start_ > sorted_buffer_.size() / 2) {
        sorted_buffer_.erase(sorted_buffer_.begin(),
                             sorted_buffer_.begin() + static_cast<std::ptrdiff_t>(sorted_start_));
        sorted_start_ = 0;
    }
}

void ReorderBuffer::flushAll(std::vector<BufferedRecord>& out) {
    out.clear();

    std::stable_sort(unsorted_batch_.begin(), unsorted_batch_.end());

    auto live_begin = sorted_buffer_.begin() + static_cast<std::ptrdiff_t>(sorted_start_);
    auto live_end = sorted_buffer_.end();
    bool has_sorted = live_begin != live_end;

    if (!has_sorted) {
        std::swap(out, unsorted_batch_);
    } else if (unsorted_batch_.empty()) {
        out.assign(live_begin, live_end);
    } else {
        if (sorted_start_ > 0) {
            sorted_buffer_.erase(
                sorted_buffer_.begin(),
                sorted_buffer_.begin() + static_cast<std::ptrdiff_t>(sorted_start_));
            sorted_start_ = 0;
        }
        auto merge_point = static_cast<ptrdiff_t>(sorted_buffer_.size());
        sorted_buffer_.insert(sorted_buffer_.end(),
                              std::make_move_iterator(unsorted_batch_.begin()),
                              std::make_move_iterator(unsorted_batch_.end()));
        std::inplace_merge(sorted_buffer_.begin(), sorted_buffer_.begin() + merge_point,
                           sorted_buffer_.end());
        std::swap(out, sorted_buffer_);
    }

    unsorted_batch_.clear();
    sorted_buffer_.clear();
    sorted_start_ = 0;

    // arena_ is intentionally NOT memset — the caller still reads it via
    // arenaData(). It is logically reset; the next bufferEvent() will overwrite.
    arena_write_pos_ = 0;
    arena_max_end_ = 0;
}

ArenaSnapshot ReorderBuffer::snapshotArena(const std::vector<BufferedRecord>& records) const {
    ArenaSnapshot snapshot;
    if (records.empty()) {
        return snapshot;
    }

    uint32_t min_offset = records[0].data_offset;
    uint32_t max_end = records[0].data_offset + records[0].data_size;
    for (size_t i = 1; i < records.size(); ++i) {
        if (records[i].data_offset < min_offset) {
            min_offset = records[i].data_offset;
        }
        uint32_t end = records[i].data_offset + records[i].data_size;
        if (end > max_end) {
            max_end = end;
        }
    }

    size_t region_size = max_end - min_offset;
    snapshot.data.resize(region_size);
    std::memcpy(snapshot.data.data(), arena_.data() + min_offset, region_size);
    snapshot.base_offset = min_offset;
    return snapshot;
}

}  // namespace chronon::observe
