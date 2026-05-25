// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>

#include "ThreadContext.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Singleton managing per-thread observability contexts.
 *
 * Pre-allocated context pool, thread-local pointer cache, lock-free hot path.
 */
class ThreadContextManager {
public:
    static constexpr size_t MAX_THREADS = 64;

    static ThreadContextManager& instance() {
        static ThreadContextManager manager;
        return manager;
    }

    /**
     * @brief Get or create the calling thread's context.
     * @return Thread context, or nullptr if MAX_THREADS exceeded.
     */
    [[nodiscard]] [[gnu::always_inline]] ThreadContext* getContext() noexcept {
        if (OBSERVE_LIKELY(tls_context_ != nullptr)) {
            return tls_context_;
        }
        return allocateContext();
    }

    /// Thread-safe; safe to call while other threads are using their contexts.
    template <typename Fn>
    void forEachContext(Fn&& fn) {
        uint32_t count = active_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            ThreadContext* ctx = contexts_[i].get();
            if (ctx) {
                fn(ctx);
            }
        }
    }

    [[nodiscard]] size_t activeThreadCount() const noexcept {
        return active_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t totalDroppedCount() const noexcept {
        uint64_t total = 0;
        uint32_t count = active_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            if (contexts_[i]) {
                total += contexts_[i]->droppedCount();
            }
        }
        return total;
    }

    [[nodiscard]] bool isInitialized() const noexcept { return initialized_; }

    /// PRECONDITION: must be called before any thread calls getContext().
    void setQueueCapacity(size_t capacity) noexcept {
        if (active_count_.load(std::memory_order_relaxed) == 0) {
            queue_capacity_ = capacity;
        }
    }

    [[nodiscard]] size_t queueCapacity() const noexcept { return queue_capacity_; }

    /// PRECONDITION: must be called before any thread calls getContext().
    void setBackpressurePolicy(BackpressurePolicy policy) noexcept {
        for (auto& p : backpressure_policy_) {
            p = policy;
        }
    }

    [[nodiscard]] BackpressurePolicy backpressurePolicy() const noexcept {
        return backpressure_policy_[0];
    }

    void setBackpressureMaxSpins(uint32_t max_spins) noexcept {
        for (auto& s : backpressure_max_spins_) {
            s = max_spins;
        }
    }

    [[nodiscard]] uint32_t backpressureMaxSpins() const noexcept {
        return backpressure_max_spins_[0];
    }

    void setBackpressurePolicy(ObservationChannel ch, BackpressurePolicy policy) noexcept {
        backpressure_policy_[static_cast<size_t>(ch)] = policy;
    }

    [[nodiscard]] BackpressurePolicy backpressurePolicy(ObservationChannel ch) const noexcept {
        return backpressure_policy_[static_cast<size_t>(ch)];
    }

    void setBackpressureMaxSpins(ObservationChannel ch, uint32_t max_spins) noexcept {
        backpressure_max_spins_[static_cast<size_t>(ch)] = max_spins;
    }

    [[nodiscard]] uint32_t backpressureMaxSpins(ObservationChannel ch) const noexcept {
        return backpressure_max_spins_[static_cast<size_t>(ch)];
    }

    /**
     * @brief Register a callback the backend uses to be woken by producers.
     *
     * Indirected through atomics to avoid a circular header dependency.
     * Written once at startup, read on hot path — acquire/release suffices.
     */
    void setBackendWakeup(void (*fn)(void*), void* ctx) noexcept {
        wakeup_ctx_.store(ctx, std::memory_order_release);
        wakeup_fn_.store(fn, std::memory_order_release);
    }

    /// Safe to call from any producer thread.
    [[nodiscard]] bool wakeBackend() noexcept {
        auto fn = wakeup_fn_.load(std::memory_order_acquire);
        if (fn) {
            fn(wakeup_ctx_.load(std::memory_order_acquire));
            return true;
        }
        return false;
    }

    /// Force-publish writer positions on all queues. Used during shutdown.
    void flushAll() noexcept {
        uint32_t count = active_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            if (contexts_[i]) {
                contexts_[i]->queue().forceCommitWrite();
            }
        }
    }

private:
    ThreadContextManager() : initialized_(true), queue_capacity_(SPSCQueue::DEFAULT_CAPACITY) {}

    ThreadContextManager(const ThreadContextManager&) = delete;
    ThreadContextManager& operator=(const ThreadContextManager&) = delete;

    ThreadContext* allocateContext() noexcept {
        uint32_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
        if (id >= MAX_THREADS) {
            next_id_.fetch_sub(1, std::memory_order_relaxed);
            return nullptr;
        }

        try {
            contexts_[id] = std::make_unique<ThreadContext>(id, queue_capacity_);
        } catch (...) {
            return nullptr;
        }

        // Monotonic max via CAS loop: concurrent allocations may get IDs out of
        // order, so a plain store(id+1) could shrink active_count_ and hide a
        // higher-id context from forEachContext().
        uint32_t expected = active_count_.load(std::memory_order_relaxed);
        uint32_t desired = id + 1;
        while (desired > expected) {
            if (active_count_.compare_exchange_weak(expected, desired, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                break;
            }
        }

        tls_context_ = contexts_[id].get();
        return tls_context_;
    }

    static inline thread_local ThreadContext* tls_context_ = nullptr;

    std::array<std::unique_ptr<ThreadContext>, MAX_THREADS> contexts_;
    std::atomic<uint32_t> next_id_{0};
    std::atomic<uint32_t> active_count_{0};

    std::atomic<void (*)(void*)> wakeup_fn_{nullptr};
    std::atomic<void*> wakeup_ctx_{nullptr};

    bool initialized_ = false;
    size_t queue_capacity_ = SPSCQueue::DEFAULT_CAPACITY;
    BackpressurePolicy backpressure_policy_[static_cast<size_t>(ObservationChannel::NumChannels)] =
        {BackpressurePolicy::BoundedWait, BackpressurePolicy::BoundedWait,
         BackpressurePolicy::BoundedWait, BackpressurePolicy::BoundedWait,
         BackpressurePolicy::BoundedWait};
    uint32_t backpressure_max_spins_[static_cast<size_t>(ObservationChannel::NumChannels)] = {
        4096, 4096, 4096, 4096, 4096};
};

}  // namespace chronon::observe
