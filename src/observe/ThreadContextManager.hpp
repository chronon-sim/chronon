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
#include <mutex>

#include "../chronon/HostServices.hpp"
#include "ThreadContext.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Singleton managing per-thread observability contexts.
 *
 * Bounded, reusable context pool with a thread-local pointer cache. Queue
 * addresses and IDs stay stable for backend readers. Only producer handoff
 * and backend wakeups take locks; event writes and consumer scans do not.
 */
class ThreadContextManager {
public:
    static constexpr size_t MAX_THREADS = 64;

    static ThreadContextManager& instance() {
        static ThreadContextManager manager;
        return manager;
    }

    /// Lifecycle control: event writers must be quiescent. Pool locking also
    /// serializes publication by a retiring TLS producer.
    void setService(std::shared_ptr<HostServiceRegistration> service) {
        std::lock_guard lock(pool_mutex_);
        service_ = std::move(service);
        forEachContext([&](ThreadContext* ctx) {
            ctx->queue().setPublicationSignal(service_ ? &service_->ready : nullptr);
        });
        assistance_.store(service_, std::memory_order_release);
    }

    void helpService() noexcept {
        if (auto service = assistance_.load(std::memory_order_acquire)) service->poll(true);
    }

    template <typename Fn>
    void forEachContextFrom(size_t first, Fn&& fn) {
        const size_t count = allocated_count_.load(std::memory_order_acquire);
        for (size_t n = 0; n < count; ++n) {
            if (auto* ctx = contexts_[(first + n) % count].load(std::memory_order_acquire)) fn(ctx);
        }
    }

    /**
     * @brief Get or create the calling thread's context.
     * @return Thread context, or nullptr if all MAX_THREADS slots have a live
     * producer or unconsumed records from an exited producer, or the calling
     * thread has already retired its producer during TLS destruction.
     * The producer pointer is valid for the calling thread's lifetime only.
     */
    [[nodiscard]] [[gnu::always_inline]] ThreadContext* getContext() noexcept {
        if (OBSERVE_LIKELY(tls_context_ != nullptr)) {
            return tls_context_;
        }
        return allocateContext();
    }

    /// Visits allocated queues, including exited producers' unread records.
    /// Addresses remain stable across producer handoff. Queue reads still
    /// require a single consumer, which must commit reads before reuse.
    template <typename Fn>
    void forEachContext(Fn&& fn) {
        uint32_t count = allocated_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            ThreadContext* ctx = contexts_[i].load(std::memory_order_acquire);
            if (ctx) {
                fn(ctx);
            }
        }
    }

    [[nodiscard]] size_t activeThreadCount() const noexcept {
        return active_count_.load(std::memory_order_relaxed);
    }

    /// Pool high-water allocation, bounded by MAX_THREADS and reused after drain.
    [[nodiscard]] size_t allocatedContextCount() const noexcept {
        return allocated_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t totalDroppedCount() const noexcept {
        uint64_t total = 0;
        uint32_t count = allocated_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            if (ThreadContext* ctx = contexts_[i].load(std::memory_order_acquire)) {
                total += ctx->droppedCount();
            }
        }
        return total;
    }

    [[nodiscard]] bool isInitialized() const noexcept { return initialized_; }

    /// PRECONDITION: must be called before any thread calls getContext().
    void setQueueCapacity(size_t capacity) noexcept {
        if (allocated_count_.load(std::memory_order_relaxed) == 0) {
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
     * Removing/replacing the callback waits for in-flight wakeups, including
     * exiting producers, before the backend can be destroyed. The callback
     * must not recursively change or invoke this notification registration.
     */
    void setBackendWakeup(void (*fn)(void*), void* ctx) noexcept {
        std::lock_guard<std::mutex> lock(wakeup_mutex_);
        wakeup_ctx_ = ctx;
        wakeup_fn_ = fn;
    }

    /// Safe to call from any producer thread.
    [[nodiscard]] bool wakeBackend() noexcept {
        std::lock_guard<std::mutex> lock(wakeup_mutex_);
        if (wakeup_fn_) {
            wakeup_fn_(wakeup_ctx_);
            return true;
        }
        return false;
    }

    /// Event writers must be quiescent. Registration and TLS retirement may
    /// still run: serialize their writer-state handoff with this final flush.
    void flushAll() noexcept {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        uint32_t count = allocated_count_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < count && i < MAX_THREADS; ++i) {
            if (ThreadContext* ctx = contexts_[i].load(std::memory_order_acquire)) {
                ctx->queue().forceCommitWrite();
            }
        }
    }

private:
    ThreadContextManager() : initialized_(true), queue_capacity_(SPSCQueue::DEFAULT_CAPACITY) {}

    ThreadContextManager(const ThreadContextManager&) = delete;
    ThreadContextManager& operator=(const ThreadContextManager&) = delete;

    // Keep handoff/TLS setup out of the hot caller's register allocation.
    [[gnu::noinline]] ThreadContext* allocateContext() noexcept {
        // Another TLS object's destructor can emit after our lease destructor.
        // Do not resurrect an attachment whose exit callback cannot run again.
        if (tls_retired_) return nullptr;
        std::lock_guard<std::mutex> lock(pool_mutex_);
        const uint32_t count = allocated_count_.load(std::memory_order_relaxed);
        uint32_t id = 0;
        for (; id < count; ++id) {
            if (!producer_attached_[id] && owned_contexts_[id]->queue().isDrained()) {
                break;
            }
        }
        if (id == count) {
            if (count == MAX_THREADS) return nullptr;
            try {
                owned_contexts_[id] = std::make_unique<ThreadContext>(id, queue_capacity_);
                owned_contexts_[id]->queue().setPublicationSignal(service_ ? &service_->ready
                                                                           : nullptr);
            } catch (...) {
                // Failed allocations do not consume a slot.
                return nullptr;
            }
            contexts_[id].store(owned_contexts_[id].get(), std::memory_order_release);
            allocated_count_.store(count + 1, std::memory_order_release);
        }

        // The mutex transfers the old producer's private queue state to its
        // successor. Keep all cursors, storage, IDs and cumulative drop counts:
        // backend readers may still hold the stable context address.
        producer_attached_[id] = true;
        active_count_.fetch_add(1, std::memory_order_relaxed);
        tls_context_ = owned_contexts_[id].get();
        tls_lease_.manager = this;
        return tls_context_;
    }

    void retireContext(ThreadContext* context) noexcept {
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            // Publish even a sub-batch tail before making the slot reusable.
            context->queue().forceCommitWrite();
            producer_attached_[context->id()] = false;
            active_count_.fetch_sub(1, std::memory_order_relaxed);
        }
        // The backend can sleep indefinitely. Notify after publication and
        // outside the pool lock; callback removal synchronizes with this call.
        (void)wakeBackend();
    }

    struct ContextLease {
        ThreadContextManager* manager;

        ContextLease() noexcept : manager(nullptr) {}

        ~ContextLease() {
            tls_retired_ = true;
            if (manager && tls_context_) {
                ThreadContext* context = tls_context_;
                tls_context_ = nullptr;
                manager->retireContext(context);
            }
        }
    };

    // Keep TLS destructor registration off the already-attached hot path.
    static inline thread_local ThreadContext* tls_context_ = nullptr;
    static inline thread_local bool tls_retired_ = false;
    static inline thread_local ContextLease tls_lease_{};

    std::mutex pool_mutex_;
    std::shared_ptr<HostServiceRegistration> service_;
    std::atomic<std::shared_ptr<HostServiceRegistration>> assistance_;
    std::array<bool, MAX_THREADS> producer_attached_{};  // protected by pool_mutex_
    std::array<std::unique_ptr<ThreadContext>, MAX_THREADS> owned_contexts_;
    std::array<std::atomic<ThreadContext*>, MAX_THREADS> contexts_{};
    std::atomic<uint32_t> allocated_count_{0};
    std::atomic<uint32_t> active_count_{0};

    std::mutex wakeup_mutex_;
    void (*wakeup_fn_)(void*) = nullptr;
    void* wakeup_ctx_ = nullptr;

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
