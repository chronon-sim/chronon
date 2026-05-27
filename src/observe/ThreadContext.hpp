// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// ThreadContext.hpp
//
// Per-thread state container for high-performance observability.
// Each simulation thread owns a ThreadContext with a dedicated SPSC queue.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>

#include "SPSCQueue.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * SizeCacheVector - Thread-local cache for string lengths.
 *
 * Avoids redundant strlen() calls by caching lengths computed during
 * size calculation phase for use during encoding phase.
 *
 * Usage:
 *   Phase 1 (size calculation):
 *     cache.clear();
 *     size_t len = strlen(str);
 *     cache.push(len);
 *     total += len;
 *
 *   Phase 2 (encoding):
 *     size_t len = cache.pop();  // Get cached length, no strlen!
 *     memcpy(dest, str, len);
 */
struct SizeCacheVector {
    static constexpr size_t MAX_CACHED = 16;

    std::array<size_t, MAX_CACHED> lengths{};
    size_t write_idx = 0;  // Write index (for push)
    size_t read_idx = 0;   // Read index (for pop)

    [[gnu::always_inline]] void clear() noexcept {
        write_idx = 0;
        read_idx = 0;
    }

    [[gnu::always_inline]] void push(size_t len) noexcept {
        if (write_idx < MAX_CACHED) {
            lengths[write_idx++] = len;
        }
    }

    [[gnu::always_inline]] size_t pop() noexcept {
        // Pop in FIFO order (matching push order)
        return read_idx < write_idx ? lengths[read_idx++] : 0;
    }

    [[nodiscard]] bool empty() const noexcept { return read_idx >= write_idx; }

    [[nodiscard]] size_t size() const noexcept { return write_idx - read_idx; }
};

/**
 * ThreadContext - Per-thread state for observability.
 *
 * Each simulation thread that emits traces/logs gets its own ThreadContext
 * with a dedicated SPSC queue. This eliminates mutex contention between threads.
 *
 * The ThreadContext is allocated once per thread (on first use) and reused
 * for the lifetime of the thread.
 */
class ThreadContext {
public:
    /**
     * Construct a thread context with the given ID.
     *
     * @param id Unique thread context ID (assigned by ThreadContextManager)
     * @param queue_capacity Capacity of the SPSC queue in bytes
     */
    explicit ThreadContext(uint32_t id, size_t queue_capacity = SPSCQueue::DEFAULT_CAPACITY)
        : id_(id), queue_(queue_capacity) {}

    // Non-copyable, non-movable
    ThreadContext(const ThreadContext&) = delete;
    ThreadContext& operator=(const ThreadContext&) = delete;
    ThreadContext(ThreadContext&&) = delete;
    ThreadContext& operator=(ThreadContext&&) = delete;

    [[nodiscard]] uint32_t id() const noexcept { return id_; }

    // Hot path - direct access, no synchronization needed since queue is
    // owned by this thread.
    [[nodiscard]] [[gnu::always_inline]] SPSCQueue& queue() noexcept { return queue_; }
    [[nodiscard]] const SPSCQueue& queue() const noexcept { return queue_; }

    /**
     * String length cache for two-phase encoding:
     * 1. Size calculation phase: compute and cache string lengths
     * 2. Encoding phase: use cached lengths (no redundant strlen)
     */
    [[nodiscard]] [[gnu::always_inline]] SizeCacheVector& sizeCache() noexcept {
        return size_cache_;
    }

    void incrementDropped() noexcept { dropped_count_.fetch_add(1, std::memory_order_relaxed); }

    [[nodiscard]] uint64_t droppedCount() const noexcept {
        return dropped_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool isActive() const noexcept { return queue_.bytesWritten() > 0; }

private:
    uint32_t id_;                             // Thread context ID
    SPSCQueue queue_;                         // Dedicated lock-free queue
    SizeCacheVector size_cache_;              // String length cache
    std::atomic<uint64_t> dropped_count_{0};  // Dropped events counter
};

}  // namespace chronon::observe
