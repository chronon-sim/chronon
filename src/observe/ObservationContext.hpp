// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "../chronon/CpuPause.hpp"
#include "Counter.hpp"
#include "DerivedCounter.hpp"
#include "FormatRegistry.hpp"
#include "LookaheadBuffer.hpp"
#include "ObservationFilter.hpp"
#include "ObservationQueue.hpp"
#include "ThreadContextManager.hpp"
#include "TimelineApi.hpp"
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
    enum class LookaheadTransition : uint8_t { None, Commit, Rollback };

    struct LookaheadSyncNode {
        using ApplyFn = void (*)(void*, LookaheadTransition) noexcept;

        void* owner = nullptr;
        ApplyFn apply = nullptr;
        ObservationContext* context = nullptr;
        LookaheadSyncNode* prev = nullptr;
        LookaheadSyncNode* next = nullptr;
    };

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

    ~ObservationContext() noexcept { detachLookaheadSyncNodes_(); }

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
        recordLookaheadTransition_(LookaheadTransition::Commit);
        ++lookahead_epoch_generation_;
        applyLookaheadTransition_(LookaheadTransition::Commit);
    }

    /// Discards a speculative epoch: rolls back counters, drops buffered events.
    void rollbackEpoch() noexcept {
        counters_.rollbackAllEpochs();
        lookahead_buffer_.rollback();
        recordLookaheadTransition_(LookaheadTransition::Rollback);
        ++lookahead_epoch_generation_;
        applyLookaheadTransition_(LookaheadTransition::Rollback);
    }

    /// When enabled, events buffer locally rather than going to the global queue.
    void setLookaheadMode(bool enabled) noexcept { lookahead_mode_ = enabled; }
    bool isLookaheadMode() const noexcept { return lookahead_mode_; }
    uint64_t lookaheadEpochGeneration() const noexcept { return lookahead_epoch_generation_; }
    LookaheadTransition firstLookaheadTransitionAfter(uint64_t generation) const noexcept {
        if (generation >= lookahead_epoch_generation_) {
            return LookaheadTransition::None;
        }
        if (generation < lookahead_history_begin_generation_ ||
            generation >= lookahead_history_begin_generation_ + lookahead_history_size_) {
            return LookaheadTransition::Rollback;
        }
        const size_t offset = static_cast<size_t>(generation - lookahead_history_begin_generation_);
        const size_t slot =
            (lookahead_history_start_slot_ + offset) % LOOKAHEAD_TRANSITION_HISTORY_CAPACITY;
        return lookahead_transition_history_[slot];
    }

    void registerLookaheadSyncNode(LookaheadSyncNode& node) noexcept {
        if (node.context == this) {
            return;
        }
        if (node.context) {
            node.context->unregisterLookaheadSyncNode(node);
        }

        node.context = this;
        node.prev = nullptr;
        node.next = lookahead_sync_head_;
        if (lookahead_sync_head_) {
            lookahead_sync_head_->prev = &node;
        }
        lookahead_sync_head_ = &node;
    }

    void unregisterLookaheadSyncNode(LookaheadSyncNode& node) noexcept {
        if (node.context != this) {
            return;
        }
        if (node.prev) {
            node.prev->next = node.next;
        } else if (lookahead_sync_head_ == &node) {
            lookahead_sync_head_ = node.next;
        }
        if (node.next) {
            node.next->prev = node.prev;
        }

        node.context = nullptr;
        node.prev = nullptr;
        node.next = nullptr;
    }

    /// Registers counter addresses with the registry for the pull-model snapshot.
    void registerAllCounters(class CounterRegistry* registry);

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

    /**
     * @brief Emit a timeline event (span begin/end, lane instant, counter sample).
     *
     * Producer-side cost matches the instant trace path: one filter check, a
     * fixed-size record fill, and a bounded arg memcpy into the SPSC queue.
     * SpanEnd events skip temporal filtering (only the trace-channel category
     * gate applies): a span begun inside an observation window must still
     * close when its end falls outside it. Ends whose begin WAS suppressed
     * are dropped by the backend's open-span table.
     */
    bool timelineEvent(CategoryMask category, TimelineEventKind kind, uint32_t track_id,
                       uint16_t slot, uint16_t name_id, uint64_t payload,
                       const TimelineArgValue* args, size_t arg_count, uint8_t flags = 0) noexcept {
        const bool allowed =
            (kind == TimelineEventKind::SpanEnd)
                ? filter_.shouldObserve(category | category::TRACE)
                : filter_.shouldObserve(category | category::TRACE, currentCycle());
        if (OBSERVE_LIKELY(!allowed)) {
            return false;
        }

        const size_t payload_size = sizeof(TimelineRecord) + arg_count * TIMELINE_ARG_SIZE;

        if (lookahead_mode_) {
            std::byte* dest = nullptr;
            size_t total_size = 0;
            if (!lookahead_buffer_.reserveRecord(ObservationQueue::EventType::TIMELINE_EVENT, 1,
                                                 payload_size, dest, total_size)) {
                if (queue_) {
                    queue_->incrementDropped();
                }
                stats_.recordDrop<ObservationChannel::Trace>();
                return false;
            }
            fillTimelineRecord_(dest, category, kind, track_id, slot, name_id, payload, args,
                                arg_count, flags);
            lookahead_buffer_.commitReservedRecord(total_size);
            stats_.recordEmit<ObservationChannel::Trace>();
            return true;
        }

        ThreadContext* ctx = ThreadContextManager::instance().getContext();
        if (!ctx) {
            stats_.recordDrop<ObservationChannel::Trace>();
            return false;
        }

        const size_t record_size = sizeof(ObservationQueue::RecordHeader) + payload_size;
        const size_t aligned_size = (record_size + 7) & ~7;

        auto* ptr = acquireQueueSpace_<ObservationChannel::Trace>(ctx, aligned_size);
        if (!ptr) {
            ctx->incrementDropped();
            stats_.recordDrop<ObservationChannel::Trace>();
            return false;
        }

        auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(ptr);
        header->total_size = static_cast<uint16_t>(aligned_size);
        header->type = ObservationQueue::EventType::TIMELINE_EVENT;
        header->flags = 1;  ///< Flag 1 = structured format.
        header->padding = 0;

        fillTimelineRecord_(ptr + sizeof(ObservationQueue::RecordHeader), category, kind, track_id,
                            slot, name_id, payload, args, arg_count, flags);

        ctx->queue().finishAndCommitWrite(aligned_size);
        stats_.recordEmit<ObservationChannel::Trace>();
        return true;
    }

private:
    void fillTimelineRecord_(std::byte* dest, CategoryMask category, TimelineEventKind kind,
                             uint32_t track_id, uint16_t slot, uint16_t name_id, uint64_t payload,
                             const TimelineArgValue* args, size_t arg_count,
                             uint8_t flags) noexcept {
        auto* rec = reinterpret_cast<TimelineRecord*>(dest);
        rec->cycle = currentCycle();
        rec->payload = payload;
        rec->track_id = track_id;
        rec->name_id = name_id;
        rec->slot = slot;
        rec->kind = static_cast<uint8_t>(kind);
        rec->arg_count = static_cast<uint8_t>(arg_count);
        // Only the lowest user bit travels (for backend name resolution);
        // filtering already ran on the full 64-bit mask.
        const CategoryMask user_bits = category & category::USER_CATEGORY_MASK;
        rec->category_bit = user_bits != 0 ? static_cast<uint8_t>(std::countr_zero(user_bits))
                                           : TIMELINE_NO_CATEGORY;
        std::memset(rec->padding, 0, sizeof(rec->padding));
        rec->padding[0] = flags;

        std::byte* arg_dest = dest + sizeof(TimelineRecord);
        for (size_t i = 0; i < arg_count; ++i) {
            packTimelineArg(arg_dest + i * TIMELINE_ARG_SIZE, args[i]);
        }
    }

    /**
     * @brief Reserve queue space, applying the channel's backpressure policy.
     *
     * Fast path: a single prepareWrite. On full queue: drop, or wake the
     * backend and spin (bounded or unbounded per policy). @return Write
     * pointer, or nullptr when the record must be dropped (caller accounts).
     */
    template <ObservationChannel Ch>
    std::byte* acquireQueueSpace_(ThreadContext* ctx, size_t aligned_size) noexcept {
        auto* ptr = ctx->queue().prepareWrite(aligned_size);
        if (OBSERVE_LIKELY(ptr != nullptr)) {
            return ptr;
        }

        auto policy = ThreadContextManager::instance().backpressurePolicy(Ch);
        if (policy == BackpressurePolicy::Drop) {
            return nullptr;
        }
        if (OBSERVE_UNLIKELY(aligned_size > ctx->queue().capacity())) {
            return nullptr;
        }
        // Force-publish uncommitted writes: without this, batched commits keep
        // atomic_writer_pos_ stale and the drain thread sees the queue as empty,
        // deadlocking the producer.
        ctx->queue().forceCommitWrite();
        if (!ThreadContextManager::instance().wakeBackend()) {
            return nullptr;
        }
        const uint32_t max_spins = (policy == BackpressurePolicy::SpinWait)
                                       ? UINT32_MAX
                                       : ThreadContextManager::instance().backpressureMaxSpins(Ch);
        uint32_t spins = 0;
        do {
            if (++spins > 64) {
                std::this_thread::yield();
                spins = (spins > max_spins) ? max_spins : spins;
            } else {
                cpuPause();
            }
            ptr = ctx->queue().prepareWrite(aligned_size);
            if (ptr) break;
            // Backend may be stopping concurrently; periodically retry wakeup.
            // If the callback has been deregistered, give up and drop.
            if ((spins & 0xFFu) == 0u && !ThreadContextManager::instance().wakeBackend()) {
                break;
            }
        } while (spins < max_spins);
        return ptr;
    }

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
            rec.category = category;
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

            auto* ptr = acquireQueueSpace_<Ch>(ctx, aligned_size);
            if (!ptr) {
                ctx->incrementDropped();
                stats_.recordDrop<Ch>();
                return;
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
            rec->category = category;
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
            rec->category = category;
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

            auto* ptr = acquireQueueSpace_<Ch>(ctx, aligned_size);
            if (!ptr) {
                ctx->incrementDropped();
                stats_.recordDrop<Ch>();
                return;
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
            rec->category = category;
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
    static constexpr size_t LOOKAHEAD_TRANSITION_HISTORY_CAPACITY = 64;
    std::array<LookaheadTransition, LOOKAHEAD_TRANSITION_HISTORY_CAPACITY>
        lookahead_transition_history_{};
    uint64_t lookahead_history_begin_generation_ = 0;
    size_t lookahead_history_start_slot_ = 0;
    size_t lookahead_history_size_ = 0;
    uint64_t lookahead_epoch_generation_ = 0;
    LookaheadSyncNode* lookahead_sync_head_ = nullptr;

    ObservationStats stats_{};

    void recordLookaheadTransition_(LookaheadTransition transition) noexcept {
        if (lookahead_history_size_ < LOOKAHEAD_TRANSITION_HISTORY_CAPACITY) {
            const size_t slot = (lookahead_history_start_slot_ + lookahead_history_size_) %
                                LOOKAHEAD_TRANSITION_HISTORY_CAPACITY;
            lookahead_transition_history_[slot] = transition;
            ++lookahead_history_size_;
            return;
        }

        lookahead_transition_history_[lookahead_history_start_slot_] = transition;
        lookahead_history_start_slot_ =
            (lookahead_history_start_slot_ + 1) % LOOKAHEAD_TRANSITION_HISTORY_CAPACITY;
        ++lookahead_history_begin_generation_;
    }

    void applyLookaheadTransition_(LookaheadTransition transition) noexcept {
        for (auto* node = lookahead_sync_head_; node;) {
            auto* next = node->next;
            if (node->apply) {
                node->apply(node->owner, transition);
            }
            node = next;
        }
    }

    void detachLookaheadSyncNodes_() noexcept {
        while (lookahead_sync_head_) {
            auto* node = lookahead_sync_head_;
            lookahead_sync_head_ = node->next;
            node->context = nullptr;
            node->prev = nullptr;
            node->next = nullptr;
        }
    }

public:
    const ObservationStats& observationStats() const noexcept { return stats_; }
};

}  // namespace chronon::observe
