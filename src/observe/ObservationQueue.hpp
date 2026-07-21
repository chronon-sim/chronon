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
#include <cstring>
#include <memory>
#include <mutex>
#include <new>

#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Lock-free SPSC ring buffer for observability events.
 *
 * Cache-line aligned atomics, cached position reads, power-of-2 capacity for
 * bit-masking, batched commits, and 2x buffer mirroring (no wrap-around handling).
 * Writer side is protected by a mutex to support multiple producers (MPSC in practice);
 * reader side is single-consumer.
 */
class ObservationQueue {
public:
    enum class EventType : uint8_t {
        COUNTER_SNAPSHOT = 0,
        LOG_EVENT = 1,
        EPOCH_COMMIT = 2,
        EPOCH_ROLLBACK = 3,
        TIMELINE_EVENT = 4,  ///< TimelineRecord: span begin/end or lane instant.
        SHUTDOWN = 255
    };

    /** @brief Header prefixed to every event in the queue. */
    struct alignas(8) RecordHeader {
        uint16_t total_size;
        EventType type;
        uint8_t flags;
        uint32_t padding;
    };

    static_assert(sizeof(RecordHeader) == 8, "RecordHeader must be 8 bytes");

    /// @param capacity Queue capacity in bytes; rounded up to a power of 2.
    explicit ObservationQueue(size_t capacity = 256 * 1024)
        : capacity_(roundUpToPowerOf2(std::max(capacity, size_t{4096}))),
          mask_(capacity_ - 1),
          bytes_per_batch_(capacity_ / 16) {
        buffer_ = std::make_unique<std::byte[]>(capacity_ * 2);
    }

    ObservationQueue(const ObservationQueue&) = delete;
    ObservationQueue& operator=(const ObservationQueue&) = delete;
    ObservationQueue(ObservationQueue&&) = delete;
    ObservationQueue& operator=(ObservationQueue&&) = delete;

    /**
     * @brief Reserve n bytes and acquire the writer lock.
     * @return Pointer to write location, or nullptr if full (lock released on failure).
     */
    [[nodiscard]] std::byte* prepareWrite(size_t n) noexcept {
        writer_mutex_.lock();

        size_t available = capacity_ - (writer_pos_ - reader_pos_cache_);
        if (OBSERVE_LIKELY(available >= n)) {
            return buffer_.get() + (writer_pos_ & mask_);
        }

        reader_pos_cache_ = atomic_reader_pos_.load(std::memory_order_acquire);
        available = capacity_ - (writer_pos_ - reader_pos_cache_);
        if (available < n) {
            writer_mutex_.unlock();
            return nullptr;
        }

        return buffer_.get() + (writer_pos_ & mask_);
    }

    /// PRECONDITION: writer lock held (from prepareWrite).
    void finishWrite(size_t n) noexcept {
        writer_pos_ += n;

        // Mirror wrapped bytes into the linear region.
        if ((writer_pos_ & mask_) < n) {
            std::memcpy(buffer_.get(), buffer_.get() + capacity_, writer_pos_ & mask_);
        }
    }

    /// Publishes the write and releases the writer lock.
    void commitWrite() noexcept {
        atomic_writer_pos_.store(writer_pos_, std::memory_order_release);
        writer_mutex_.unlock();
    }

    void finishAndCommitWrite(size_t n) noexcept {
        finishWrite(n);
        commitWrite();
    }

    [[nodiscard]] bool hasSpace(size_t n) const noexcept {
        size_t available = capacity_ - (writer_pos_ - reader_pos_cache_);
        return available >= n;
    }

    void incrementDropped() noexcept { dropped_count_.fetch_add(1, std::memory_order_relaxed); }

    /// @return Pointer to next record, or nullptr if empty.
    [[nodiscard]] std::byte* prepareRead() noexcept {
        if (OBSERVE_LIKELY(reader_pos_ < writer_pos_cache_)) {
            return buffer_.get() + (reader_pos_ & mask_);
        }

        writer_pos_cache_ = atomic_writer_pos_.load(std::memory_order_acquire);
        if (reader_pos_ >= writer_pos_cache_) {
            return nullptr;
        }

        return buffer_.get() + (reader_pos_ & mask_);
    }

    void finishRead(size_t n) noexcept {
        reader_pos_ += n;
        bytes_since_commit_ += n;
    }

    void commitRead() noexcept {
        if (bytes_since_commit_ >= bytes_per_batch_) {
            atomic_reader_pos_.store(reader_pos_, std::memory_order_release);
            bytes_since_commit_ = 0;
        }
    }

    void forceCommitRead() noexcept {
        atomic_reader_pos_.store(reader_pos_, std::memory_order_release);
        bytes_since_commit_ = 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        return reader_pos_ >= writer_pos_cache_ &&
               reader_pos_ >= atomic_writer_pos_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] uint64_t droppedCount() const noexcept {
        return dropped_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t bytesWritten() const noexcept {
        return atomic_writer_pos_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t bytesRead() const noexcept {
        return atomic_reader_pos_.load(std::memory_order_relaxed);
    }

private:
    static size_t roundUpToPowerOf2(size_t n) {
        if (n == 0) return 1;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    std::unique_ptr<std::byte[]> buffer_;
    const size_t capacity_;
    const size_t mask_;
    const size_t bytes_per_batch_;

    // Writer state. writer_mutex_ protects writer_pos_ and reader_pos_cache_
    // to support multiple producer threads.
    alignas(128) std::mutex writer_mutex_;
    std::atomic<size_t> atomic_writer_pos_{0};
    size_t writer_pos_{0};
    size_t reader_pos_cache_{0};

    // Reader state. Cache-line separated to avoid false sharing with writer state.
    alignas(128) std::atomic<size_t> atomic_reader_pos_{0};
    size_t reader_pos_{0};
    size_t writer_pos_cache_{0};
    size_t bytes_since_commit_{0};

    alignas(64) std::atomic<uint64_t> dropped_count_{0};
};

}  // namespace chronon::observe
