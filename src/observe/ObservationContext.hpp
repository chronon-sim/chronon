// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <thread>

#include "Counter.hpp"
#include "DerivedCounter.hpp"
#include "FormatRegistry.hpp"
#include "LookaheadBuffer.hpp"
#include "ObservationFilter.hpp"
#include "ObservationQueue.hpp"
#include "ThreadContextManager.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Per-unit central context for counters, tracing, logging, and epochs.
 *
 * Hot paths are inline and allocation-free. Format strings are pre-registered:
 * only IDs and args traverse the queue. Filtering uses bitmasks for O(1) checks;
 * events flow into lock-free SPSC queues, one per producer thread.
 */
class ObservationContext {
public:
    using CycleProvider = std::function<uint64_t()>;

    /**
     * @param queue          Shared observation queue (owned by simulation).
     * @param cycle_provider Returns current cycle when no per-thread override is set.
     * @param thread_id      Thread ID (0..MAX_THREADS-1).
     * @param unit_name      Name of the unit this context belongs to.
     * @param source_id      Unique unit ID in the source-name registry.
     */
    ObservationContext(ObservationQueue* queue, CycleProvider cycle_provider,
                       uint32_t thread_id = 0, std::string unit_name = "", uint16_t source_id = 0)
        : queue_(queue),
          cycle_provider_(std::move(cycle_provider)),
          thread_id_(thread_id),
          unit_name_(std::move(unit_name)),
          source_id_(source_id) {
        // Most simulations use fewer than 64 counters; pre-allocating allows
        // getUnchecked() on the hot path without resize churn.
        counters_.ensureCapacity(64);
    }

    /**
     * @brief Increment a counter.
     *
     * PRECONDITION: Counter must have been added via addCounter() (no bounds check).
     */
    [[gnu::always_inline]] void count(CounterId id, uint64_t delta = 1) noexcept {
        if (OBSERVE_LIKELY(counters_enabled_)) {
            counters_.getUnchecked(id).increment(delta);
        }
    }

    bool countersEnabled() const noexcept { return counters_enabled_; }
    void setCountersEnabled(bool enabled) noexcept { counters_enabled_ = enabled; }

    FixedCounterStorage& counters() noexcept { return counters_; }
    const FixedCounterStorage& counters() const noexcept { return counters_; }

    [[gnu::always_inline]] bool shouldTrace(CategoryMask category) const noexcept {
        return filter_.shouldObserve(category | category::TRACE, currentCycle());
    }

    [[gnu::always_inline]] void trace(CategoryMask category, FormatId fmt_id) noexcept {
        if (OBSERVE_UNLIKELY(shouldTrace(category))) {
            emitEvent<ObservationChannel::Trace>(ObservationQueue::EventType::TRACE_EVENT, category,
                                                 fmt_id);
        }
    }

    /// Arguments are packed raw; backend reconstructs the message from the format ID.
    template <typename... Args>
    [[gnu::always_inline]] void trace(CategoryMask category, FormatId fmt_id,
                                      Args&&... args) noexcept {
        if (OBSERVE_UNLIKELY(shouldTrace(category))) {
            emitEvent<ObservationChannel::Trace>(ObservationQueue::EventType::TRACE_EVENT, category,
                                                 fmt_id, std::forward<Args>(args)...);
        }
    }

    template <LogLevel Level>
    [[gnu::always_inline]] bool shouldLog() const noexcept {
        return filter_.shouldObserve(logLevelToCategory(Level), currentCycle());
    }

    template <LogLevel Level>
    [[gnu::always_inline]] void log(FormatId fmt_id) noexcept {
        if (shouldLog<Level>()) {
            emitEvent<log_level_channel_v<Level>>(ObservationQueue::EventType::LOG_EVENT,
                                                  logLevelToCategory(Level), fmt_id);
        }
    }

    template <LogLevel Level, typename... Args>
    [[gnu::always_inline]] void log(FormatId fmt_id, Args&&... args) noexcept {
        if (shouldLog<Level>()) {
            emitEvent<log_level_channel_v<Level>>(ObservationQueue::EventType::LOG_EVENT,
                                                  logLevelToCategory(Level), fmt_id,
                                                  std::forward<Args>(args)...);
        }
    }

    ObservationFilter& filter() noexcept { return filter_; }
    const ObservationFilter& filter() const noexcept { return filter_; }

    void enableCategory(CategoryMask mask) noexcept { filter_.enableCategory(mask); }
    void disableCategory(CategoryMask mask) noexcept { filter_.disableCategory(mask); }

    /// Confirms a speculative epoch: commits counters and flushes buffered events.
    void commitEpoch() noexcept {
        counters_.commitAllEpochs();
        if (lookahead_buffer_.hasEvents() && queue_) {
            lookahead_buffer_.commit(*queue_);
        }
    }

    /// Discards a speculative epoch: rolls back counters, drops buffered events.
    void rollbackEpoch() noexcept {
        counters_.rollbackAllEpochs();
        lookahead_buffer_.rollback();
    }

    /// When enabled, events buffer locally rather than going to the global queue.
    void setLookaheadMode(bool enabled) noexcept { lookahead_mode_ = enabled; }
    bool isLookaheadMode() const noexcept { return lookahead_mode_; }

    /// Registers counter addresses with the manager for the pull-model snapshot.
    void registerAllCounters(class ObservationManager* manager);

    /// Called by DerivedCounter::onContextAttached() during unit initialization.
    void addDerivedCounterDef(DerivedCounterDef def) {
        derived_counter_defs_.push_back(std::move(def));
    }

    const std::vector<DerivedCounterDef>& derivedCounterDefs() const noexcept {
        return derived_counter_defs_;
    }

    void setThreadId(uint32_t id) noexcept { thread_id_ = id; }
    uint32_t threadId() const noexcept { return thread_id_; }
    const std::string& unitName() const noexcept { return unit_name_; }
    uint16_t sourceId() const noexcept { return source_id_; }

    void setCycleProvider(CycleProvider provider) { cycle_provider_ = std::move(provider); }

    /**
     * @brief Override the current-cycle value on this thread.
     *
     * Used in parallel execution so each worker can stamp events with its own
     * local cycle without races on the shared cycle_provider_.
     */
    void setCurrentCycleValue(uint64_t cycle) noexcept {
        tls_cycle_override_ = cycle;
        tls_use_cycle_override_ = true;
    }

    void clearCycleOverride() noexcept { tls_use_cycle_override_ = false; }

    uint64_t currentCycle() const {
        if (tls_use_cycle_override_) {
            return tls_cycle_override_;
        }
        return cycle_provider_ ? cycle_provider_() : 0;
    }

private:
    static inline thread_local uint64_t tls_cycle_override_ = 0;
    static inline thread_local bool tls_use_cycle_override_ = false;

public:
    void setQueue(ObservationQueue* queue) noexcept { queue_ = queue; }
    ObservationQueue* queue() noexcept { return queue_; }

private:
    /// Phase 1 of two-phase encoding: compute size and cache string lengths so
    /// phase 2 doesn't need to call strlen() again.
    template <typename T>
    static size_t argSizeAndCache(SizeCacheVector& cache, const T& arg) noexcept {
        if constexpr (std::is_same_v<std::decay_t<T>, const char*> ||
                      std::is_same_v<std::decay_t<T>, char*>) {
            size_t len = std::strlen(arg) + 1;
            cache.push(len);
            return len;
        } else if constexpr (std::is_same_v<std::decay_t<T>, std::string> ||
                             std::is_same_v<std::decay_t<T>, std::string_view>) {
            size_t len = arg.size() + 1;
            cache.push(len);
            return len;
        } else {
            return sizeof(T);
        }
    }

    /// Phase 2 of two-phase encoding: pack using cached string lengths.
    template <typename T>
    static size_t packArgCached(std::byte* dest, SizeCacheVector& cache, const T& arg) noexcept {
        if constexpr (std::is_same_v<std::decay_t<T>, const char*> ||
                      std::is_same_v<std::decay_t<T>, char*>) {
            size_t len = cache.pop();
            std::memcpy(dest, arg, len);
            return len;
        } else if constexpr (std::is_same_v<std::decay_t<T>, std::string> ||
                             std::is_same_v<std::decay_t<T>, std::string_view>) {
            size_t len = cache.pop();
            std::memcpy(dest, arg.data(), len - 1);
            dest[len - 1] = std::byte{0};
            return len;
        } else {
            std::memcpy(dest, &arg, sizeof(T));
            return sizeof(T);
        }
    }

    /// Emit a structured event with no arguments. Layout: [RecordHeader][StructuredRecord].
    template <ObservationChannel Ch>
    void emitEvent(ObservationQueue::EventType type, CategoryMask category,
                   FormatId fmt_id) noexcept {
        constexpr size_t record_size =
            sizeof(ObservationQueue::RecordHeader) + sizeof(StructuredRecord);
        constexpr size_t aligned_size = (record_size + 7) & ~7;

        if (lookahead_mode_) {
            StructuredRecord rec{};
            rec.cycle = currentCycle();
            rec.format_id = fmt_id;
            rec.category = static_cast<uint32_t>(category);
            rec.source_id = source_id_;
            rec.arg_count = 0;
            if (!lookahead_buffer_.bufferEventRaw(type, 1, reinterpret_cast<const std::byte*>(&rec),
                                                  sizeof(rec)) &&
                queue_) {
                queue_->incrementDropped();
                stats_.recordDrop<Ch>();
            } else {
                stats_.recordEmit<Ch>();
            }
            return;
        }

        {
            ThreadContext* ctx = ThreadContextManager::instance().getContext();
            if (!ctx) {
                stats_.recordDrop<Ch>();
                return;
            }

            auto* ptr = ctx->queue().prepareWrite(aligned_size);
            if (!ptr) {
                auto policy = ThreadContextManager::instance().backpressurePolicy(Ch);
                if (policy == BackpressurePolicy::Drop) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                if (OBSERVE_UNLIKELY(aligned_size > ctx->queue().capacity())) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                // Force-publish uncommitted writes: without this, batched commits keep
                // atomic_writer_pos_ stale and the drain thread sees the queue as empty,
                // deadlocking the producer.
                ctx->queue().forceCommitWrite();
                if (!ThreadContextManager::instance().wakeBackend()) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                const uint32_t max_spins =
                    (policy == BackpressurePolicy::SpinWait)
                        ? UINT32_MAX
                        : ThreadContextManager::instance().backpressureMaxSpins(Ch);
                uint32_t spins = 0;
                do {
                    if (++spins > 64) {
                        std::this_thread::yield();
                        spins = (spins > max_spins) ? max_spins : spins;
                    } else {
#if defined(__x86_64__) || defined(_M_X64)
                        __builtin_ia32_pause();
#elif defined(__aarch64__)
                        asm volatile("yield" ::: "memory");
#endif
                    }
                    ptr = ctx->queue().prepareWrite(aligned_size);
                    if (ptr) break;
                    // Backend may be stopping concurrently; periodically retry wakeup.
                    // If the callback has been deregistered, give up and drop.
                    if ((spins & 0xFFu) == 0u && !ThreadContextManager::instance().wakeBackend()) {
                        break;
                    }
                } while (spins < max_spins);
                if (!ptr) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
            }

            auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(ptr);
            header->total_size = static_cast<uint16_t>(aligned_size);
            header->type = type;
            header->flags = 1;  ///< Flag 1 = structured format.
            header->padding = 0;

            auto* rec =
                reinterpret_cast<StructuredRecord*>(ptr + sizeof(ObservationQueue::RecordHeader));
            rec->cycle = currentCycle();
            rec->format_id = fmt_id;
            rec->category = static_cast<uint32_t>(category);
            rec->source_id = source_id_;
            rec->arg_count = 0;

            ctx->queue().finishAndCommitWrite(aligned_size);
            stats_.recordEmit<Ch>();
        }
    }

    /// Emit a structured event with arguments via two-phase encoding (size, then pack).
    template <ObservationChannel Ch, typename... Args>
    void emitEvent(ObservationQueue::EventType type, CategoryMask category, FormatId fmt_id,
                   Args&&... args) noexcept {
        constexpr size_t arg_count = sizeof...(Args);

        if (lookahead_mode_) {
            // Speculative lookahead buffer; rollback discards events emitted while running ahead.
            SizeCacheVector cache;
            cache.clear();
            const size_t args_size = (argSizeAndCache(cache, args) + ...);

            const size_t payload_size = sizeof(StructuredRecord) + args_size;
            size_t total_size = 0;
            std::byte* payload = nullptr;
            if (!lookahead_buffer_.reserveRecord(type, 1, payload_size, payload, total_size)) {
                if (queue_) {
                    queue_->incrementDropped();
                }
                stats_.recordDrop<Ch>();
                return;
            }

            auto* rec = reinterpret_cast<StructuredRecord*>(payload);
            rec->cycle = currentCycle();
            rec->format_id = fmt_id;
            rec->category = static_cast<uint32_t>(category);
            rec->source_id = source_id_;
            rec->arg_count = static_cast<uint8_t>(arg_count);

            std::byte* args_ptr = payload + sizeof(StructuredRecord);
            size_t offset = 0;
            ((offset += packArgCached(args_ptr + offset, cache, args)), ...);

            lookahead_buffer_.commitReservedRecord(total_size);
            stats_.recordEmit<Ch>();
            return;
        }

        {
            ThreadContext* ctx = ThreadContextManager::instance().getContext();
            if (!ctx) {
                stats_.recordDrop<Ch>();
                return;
            }

            SizeCacheVector& cache = ctx->sizeCache();
            cache.clear();
            const size_t args_size = (argSizeAndCache(cache, args) + ...);

            const size_t record_size =
                sizeof(ObservationQueue::RecordHeader) + sizeof(StructuredRecord) + args_size;
            const size_t aligned_size = (record_size + 7) & ~7;

            auto* ptr = ctx->queue().prepareWrite(aligned_size);
            if (!ptr) {
                auto policy = ThreadContextManager::instance().backpressurePolicy(Ch);
                if (policy == BackpressurePolicy::Drop) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                if (OBSERVE_UNLIKELY(aligned_size > ctx->queue().capacity())) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                // Force-publish uncommitted writes: without this, batched commits keep
                // atomic_writer_pos_ stale and the drain thread sees the queue as empty,
                // deadlocking the producer.
                ctx->queue().forceCommitWrite();
                if (!ThreadContextManager::instance().wakeBackend()) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
                const uint32_t max_spins =
                    (policy == BackpressurePolicy::SpinWait)
                        ? UINT32_MAX
                        : ThreadContextManager::instance().backpressureMaxSpins(Ch);
                uint32_t spins = 0;
                do {
                    if (++spins > 64) {
                        std::this_thread::yield();
                        spins = (spins > max_spins) ? max_spins : spins;
                    } else {
#if defined(__x86_64__) || defined(_M_X64)
                        __builtin_ia32_pause();
#elif defined(__aarch64__)
                        asm volatile("yield" ::: "memory");
#endif
                    }
                    ptr = ctx->queue().prepareWrite(aligned_size);
                    if (ptr) break;
                    if ((spins & 0xFFu) == 0u && !ThreadContextManager::instance().wakeBackend()) {
                        break;
                    }
                } while (spins < max_spins);
                if (!ptr) {
                    ctx->incrementDropped();
                    stats_.recordDrop<Ch>();
                    return;
                }
            }

            auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(ptr);
            header->total_size = static_cast<uint16_t>(aligned_size);
            header->type = type;
            header->flags = 1;  ///< Flag 1 = structured format.
            header->padding = 0;

            auto* rec =
                reinterpret_cast<StructuredRecord*>(ptr + sizeof(ObservationQueue::RecordHeader));
            rec->cycle = currentCycle();
            rec->format_id = fmt_id;
            rec->category = static_cast<uint32_t>(category);
            rec->source_id = source_id_;
            rec->arg_count = static_cast<uint8_t>(arg_count);

            std::byte* args_ptr =
                ptr + sizeof(ObservationQueue::RecordHeader) + sizeof(StructuredRecord);
            size_t offset = 0;
            ((offset += packArgCached(args_ptr + offset, cache, args)), ...);

            ctx->queue().finishAndCommitWrite(aligned_size);
            stats_.recordEmit<Ch>();
        }
    }

    ObservationQueue* queue_ = nullptr;  ///< Used for counter snapshots and lookahead commits.
    CycleProvider cycle_provider_;
    uint32_t thread_id_ = 0;
    std::string unit_name_;
    uint16_t source_id_ = 0;

    FixedCounterStorage counters_{"unit"};
    bool counters_enabled_ = true;

    std::vector<DerivedCounterDef> derived_counter_defs_;

    ObservationFilter filter_;

    LookaheadBuffer lookahead_buffer_;
    bool lookahead_mode_ = false;

    ObservationStats stats_{};

public:
    const ObservationStats& observationStats() const noexcept { return stats_; }

    [[deprecated("Use observationStats()")]]
    const ObservationStats& observeStats() const noexcept {
        return observationStats();
    }
};

}  // namespace chronon::observe
