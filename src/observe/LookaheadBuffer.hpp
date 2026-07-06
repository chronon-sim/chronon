// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <cstddef>
#include <cstring>

#include "ObservationQueue.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Per-unit buffer for speculative event storage during lookahead execution.
 *
 * Units running ahead of the global cycle buffer events locally; the buffer is
 * either committed to the global queue (on confirm) or discarded (on rollback,
 * which is O(1)). Fixed-size inline storage avoids hot-path allocation.
 */
class LookaheadBuffer {
public:
    static constexpr size_t BUFFER_SIZE = 4096;  ///< 4KB per unit.

    LookaheadBuffer() = default;

    /// @return true if buffered, false if buffer full.
    template <typename T>
    bool bufferEvent(ObservationQueue::EventType type, const T& data) {
        return bufferEventRaw(type, 0, reinterpret_cast<const std::byte*>(&data), sizeof(T));
    }

    bool bufferEventRaw(ObservationQueue::EventType type, const std::byte* data, size_t data_size) {
        return bufferEventRaw(type, 0, data, data_size);
    }

    bool bufferEventRaw(ObservationQueue::EventType type, uint8_t flags, const std::byte* data,
                        size_t data_size) {
        size_t total_size = 0;
        std::byte* payload = nullptr;
        if (!reserveRecord(type, flags, data_size, payload, total_size)) {
            return false;
        }

        std::memcpy(payload, data, data_size);
        commitReservedRecord(total_size);
        return true;
    }

    /// Reserve storage for a record and return writable payload pointer.
    bool reserveRecord(ObservationQueue::EventType type, uint8_t flags, size_t data_size,
                       std::byte*& payload_out, size_t& total_size_out) {
        size_t total_size = sizeof(ObservationQueue::RecordHeader) + data_size;
        total_size = (total_size + 7) & ~7;  // Align to 8 bytes.

        if (write_pos_ + total_size > BUFFER_SIZE) {
            payload_out = nullptr;
            total_size_out = 0;
            return false;
        }

        auto* header =
            reinterpret_cast<ObservationQueue::RecordHeader*>(buffer_.data() + write_pos_);
        header->total_size = static_cast<uint16_t>(total_size);
        header->type = type;
        header->flags = flags;
        header->padding = 0;

        payload_out = buffer_.data() + write_pos_ + sizeof(ObservationQueue::RecordHeader);
        total_size_out = total_size;
        return true;
    }

    void commitReservedRecord(size_t total_size) noexcept {
        write_pos_ += total_size;
        event_count_++;
    }

    /**
     * @brief Flush buffered events to the global queue.
     * @return Number of events successfully committed (others are dropped if queue full).
     */
    size_t commit(ObservationQueue& queue) {
        return commit(queue, [](std::byte*, size_t) noexcept {});
    }

    template <typename RewriteFn>
    size_t commit(ObservationQueue& queue, RewriteFn rewrite) {
        if (write_pos_ == 0) {
            return 0;
        }

        size_t events_committed = 0;
        size_t read_pos = 0;

        while (read_pos < write_pos_) {
            auto* header =
                reinterpret_cast<const ObservationQueue::RecordHeader*>(buffer_.data() + read_pos);

            auto* dest = queue.prepareWrite(header->total_size);
            if (dest) {
                std::memcpy(dest, buffer_.data() + read_pos, header->total_size);
                rewrite(dest, header->total_size);
                queue.finishAndCommitWrite(header->total_size);
                events_committed++;
            } else {
                queue.incrementDropped();
            }

            read_pos += header->total_size;
        }

        write_pos_ = 0;
        event_count_ = 0;

        return events_committed;
    }

    /// Discard all buffered events.
    void rollback() noexcept {
        write_pos_ = 0;
        event_count_ = 0;
    }

    bool hasEvents() const noexcept { return event_count_ > 0; }
    size_t eventCount() const noexcept { return event_count_; }
    size_t bytesUsed() const noexcept { return write_pos_; }
    size_t bytesAvailable() const noexcept { return BUFFER_SIZE - write_pos_; }

private:
    alignas(64) std::array<std::byte, BUFFER_SIZE> buffer_{};
    size_t write_pos_ = 0;
    size_t event_count_ = 0;
};

}  // namespace chronon::observe
