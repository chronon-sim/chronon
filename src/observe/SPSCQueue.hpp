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
#include <new>

#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Lock-free single-producer single-consumer ring buffer.
 *
 * Cache-line-aligned atomics, cached position reads, power-of-2 capacity for
 * bit-masking, batched commits, and 2x buffer mirroring to eliminate wrap-around
 * handling. Synchronization via acquire/release atomics; no mutex.
 *
 * Producer:
 * @code
 *   auto* ptr = queue.prepareWrite(size);
 *   if (ptr) { memcpy(ptr, data, size); queue.finishAndCommitWrite(size); }
 * @endcode
 *
 * Consumer:
 * @code
 *   while (auto* ptr = queue.prepareRead()) {
 *       auto* h = reinterpret_cast<const RecordHeader*>(ptr);
 *       processEvent(h, ptr + sizeof(RecordHeader));
 *       queue.finishRead(h->total_size);
 *   }
 *   queue.commitRead();
 * @endcode
 */
class SPSCQueue {
public:
    /// Default 128KB per thread; overridden by YAML queue_capacity when configured.
    static constexpr size_t DEFAULT_CAPACITY = 128 * 1024;

    /// Update writer/reader atomics every N bytes to amortize cache-coherency traffic.
    static constexpr size_t DEFAULT_BYTES_PER_BATCH = 4096;

    /// @param capacity Queue capacity in bytes; rounded up to a power of 2.
    explicit SPSCQueue(size_t capacity = DEFAULT_CAPACITY)
        : capacity_(roundUpToPowerOf2(std::max(capacity, size_t{4096}))),
          mask_(capacity_ - 1),
          bytes_per_batch_(std::min(capacity_ / 16, DEFAULT_BYTES_PER_BATCH)) {
        // 2x capacity provides a mirror region that eliminates wrap-around handling.
        buffer_ = std::make_unique<std::byte[]>(capacity_ * 2);
    }

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&) = delete;
    SPSCQueue& operator=(SPSCQueue&&) = delete;

    /**
     * @brief Reserve n bytes for the producer.
     * @return Pointer to write location, or nullptr if full.
     * @note Single producer thread only.
     */
    [[nodiscard]] [[gnu::always_inline]] std::byte* prepareWrite(size_t n) noexcept {
        size_t available = capacity_ - (writer_pos_ - cached_reader_pos_);
        if (OBSERVE_LIKELY(available >= n)) {
            return buffer_.get() + (writer_pos_ & mask_);
        }

        cached_reader_pos_ = atomic_reader_pos_.load(std::memory_order_acquire);
        available = capacity_ - (writer_pos_ - cached_reader_pos_);
        if (available < n) {
            return nullptr;
        }

        return buffer_.get() + (writer_pos_ & mask_);
    }

    [[gnu::always_inline]] void finishWrite(size_t n) noexcept {
        writer_pos_ += n;

        // Mirror any wrapped bytes into the linear region so consumers always
        // see a contiguous record regardless of wrap position.
        size_t pos_in_buffer = writer_pos_ & mask_;
        if (pos_in_buffer < n) {
            std::memcpy(buffer_.get(), buffer_.get() + capacity_, pos_in_buffer);
        }

        uncommitted_bytes_ += n;
    }

    /// Publish to consumer once batch threshold reached.
    [[gnu::always_inline]] void commitWrite() noexcept {
        if (uncommitted_bytes_ >= bytes_per_batch_) {
            atomic_writer_pos_.store(writer_pos_, std::memory_order_release);
            uncommitted_bytes_ = 0;
        }
    }

    /// Publish unconditionally; use for shutdown or explicit flush.
    void forceCommitWrite() noexcept {
        atomic_writer_pos_.store(writer_pos_, std::memory_order_release);
        uncommitted_bytes_ = 0;
    }

    [[gnu::always_inline]] void finishAndCommitWrite(size_t n) noexcept {
        finishWrite(n);
        commitWrite();
    }

    [[nodiscard]] bool hasSpace(size_t n) const noexcept {
        size_t available = capacity_ - (writer_pos_ - cached_reader_pos_);
        return available >= n;
    }

    /**
     * @return Pointer to next record, or nullptr if empty.
     * @note Single consumer thread only.
     */
    [[nodiscard]] [[gnu::always_inline]] std::byte* prepareRead() noexcept {
        if (OBSERVE_LIKELY(reader_pos_ < cached_writer_pos_)) {
            return buffer_.get() + (reader_pos_ & mask_);
        }

        cached_writer_pos_ = atomic_writer_pos_.load(std::memory_order_acquire);
        if (reader_pos_ >= cached_writer_pos_) {
            return nullptr;
        }

        return buffer_.get() + (reader_pos_ & mask_);
    }

    [[gnu::always_inline]] void finishRead(size_t n) noexcept {
        reader_pos_ += n;
        bytes_since_commit_ += n;
    }

    [[gnu::always_inline]] void commitRead() noexcept {
        if (bytes_since_commit_ >= bytes_per_batch_) {
            atomic_reader_pos_.store(reader_pos_, std::memory_order_release);
            bytes_since_commit_ = 0;
        }
    }

    /// Eagerly publish so freed space becomes visible to producers without waiting for batch.
    [[gnu::always_inline]] void eagerCommitRead() noexcept {
        if (bytes_since_commit_ > 0) {
            atomic_reader_pos_.store(reader_pos_, std::memory_order_release);
            bytes_since_commit_ = 0;
        }
    }

    void forceCommitRead() noexcept {
        atomic_reader_pos_.store(reader_pos_, std::memory_order_release);
        bytes_since_commit_ = 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        return reader_pos_ >= cached_writer_pos_ &&
               reader_pos_ >= atomic_writer_pos_.load(std::memory_order_acquire);
    }

    [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] size_t bytesWritten() const noexcept {
        return atomic_writer_pos_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t bytesRead() const noexcept {
        return atomic_reader_pos_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t bytesAvailable() const noexcept {
        size_t written = atomic_writer_pos_.load(std::memory_order_relaxed);
        size_t read = atomic_reader_pos_.load(std::memory_order_relaxed);
        return written - read;
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

    // Writer state: producer-only. Cache-line aligned to avoid false sharing
    // with reader state and the shared atomics.
    alignas(64) size_t writer_pos_{0};
    size_t cached_reader_pos_{0};
    size_t uncommitted_bytes_{0};

    // Shared atomics: each on its own cache line.
    alignas(64) std::atomic<size_t> atomic_writer_pos_{0};
    alignas(64) std::atomic<size_t> atomic_reader_pos_{0};

    // Reader state: consumer-only. Cache-line aligned.
    alignas(64) size_t reader_pos_{0};
    size_t cached_writer_pos_{0};
    size_t bytes_since_commit_{0};
};

}  // namespace chronon::observe
