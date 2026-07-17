// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

// Template implementation detail. Include through sender/port/InPort.hpp.

namespace chronon::sender {

template <typename T>
template <auto KeyFn, typename K>
    requires detail::SelectiveCancelKeyFn<KeyFn, T>
void InPort<T>::cancelOlderThan(K watermark) {
    if (policy_ != PortPolicy::LegacyFastPath) {
        // StageSelective: install the lower half of a timestamp-scoped keep
        // range. A same-cycle cancelYoungerThan call intersects with this
        // predicate in-place.
        if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) return;
        const uint64_t cycle = getCurrentCycle();
        const uint64_t threshold = static_cast<uint64_t>(watermark);
        stage_state_.install(cycle, threshold, std::numeric_limits<uint64_t>::max());
        traceStageInstall_(cycle);
        return;
    }
    if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) return;
    const uint64_t threshold = static_cast<uint64_t>(watermark);
    auto current = receiver_min_keep_key_.load(std::memory_order_relaxed);
    while (current < threshold &&
           !receiver_min_keep_key_.compare_exchange_weak(
               current, threshold, std::memory_order_release, std::memory_order_relaxed));
}

template <typename T>
template <auto KeyFn, typename K>
    requires detail::SelectiveCancelKeyFn<KeyFn, T>
void InPort<T>::cancelYoungerThan(K watermark) {
    if (policy_ != PortPolicy::LegacyFastPath) {
        // StageSelective predicates are independent and retired pop-driven.
        // The key extractor is shared with LegacyFastPath, while generation
        // state is ignored by the StageSelective receive path.
        if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) return;
        const uint64_t cycle = getCurrentCycle();
        const uint64_t threshold = static_cast<uint64_t>(watermark);
        stage_state_.install(cycle, 0, threshold);
        traceStageInstall_(cycle);
        return;
    }
    if (!configureReceiverSelectiveExtractorAndScope_<KeyFn>()) return;
    const uint64_t threshold = static_cast<uint64_t>(watermark);
    auto current = receiver_max_keep_key_.load(std::memory_order_relaxed);
    while (current > threshold &&
           !receiver_max_keep_key_.compare_exchange_weak(
               current, threshold, std::memory_order_release, std::memory_order_relaxed));
}

template <typename T>
template <auto KeyFn, typename MinK, typename MaxK>
    requires detail::SelectiveCancelKeyFn<KeyFn, T>
void InPort<T>::cancelOutsideInclusive(MinK min_keep, MaxK max_keep) {
    cancelOlderThan<KeyFn>(min_keep);
    cancelYoungerThan<KeyFn>(max_keep);
}

template <typename T>
void InPort<T>::resetSelectiveCancellation() {
    if (policy_ != PortPolicy::LegacyFastPath) return;
    std::lock_guard<std::mutex> lock(receiver_cancel_mutex_);
    receiver_min_keep_key_.store(0, std::memory_order_release);
    receiver_max_keep_key_.store(std::numeric_limits<uint64_t>::max(), std::memory_order_release);
    receiver_key_extractor_.store(nullptr, std::memory_order_release);
    receiver_filter_generation_.store(std::numeric_limits<uint64_t>::max(),
                                      std::memory_order_release);
}

template <typename T>
void InPort<T>::traceStageInstall_(uint64_t cycle) const noexcept {
    if (!stageTraceEnabled_() || !stageTracePortMatches_(name_)) return;
    uint64_t min_keep = 0;
    uint64_t max_keep = std::numeric_limits<uint64_t>::max();
    for (const auto& predicate : stage_state_.slots) {
        if (predicate.flush_cycle == cycle) {
            min_keep = predicate.min_keep;
            max_keep = predicate.max_keep;
            break;
        }
    }
    std::fprintf(stderr, "[STAGE] install port=%s cycle=%lu keep=[%lu,%lu] live=%zu hwm=%zu\n",
                 name_.c_str(), static_cast<unsigned long>(cycle),
                 static_cast<unsigned long>(min_keep), static_cast<unsigned long>(max_keep),
                 stage_state_.active_slot_count(), stage_state_.high_water);
}

template <typename T>
template <auto KeyFn>
    requires detail::SelectiveCancelKeyFn<KeyFn, T>
bool InPort<T>::configureReceiverSelectiveExtractorAndScope_() {
    constexpr ReceiverSelectiveKeyExtractor new_extractor =
        +[](const T& data) -> uint64_t { return static_cast<uint64_t>(std::invoke(KeyFn, data)); };

    std::lock_guard<std::mutex> lock(receiver_cancel_mutex_);
    auto existing = receiver_key_extractor_.load(std::memory_order_acquire);
    if (!existing) {
        receiver_key_extractor_.store(new_extractor, std::memory_order_release);
        existing = new_extractor;
    }
    if (existing != new_extractor) return false;

    ensureReceiverScopeToInFlightLocked_();
    return true;
}

template <typename T>
void InPort<T>::ensureReceiverScopeToInFlightLocked_() {
    const uint64_t all_generations = std::numeric_limits<uint64_t>::max();
    if (receiver_filter_generation_.load(std::memory_order_acquire) != all_generations) return;

    // Future messages use a new generation and remain outside the current
    // cancellation scope, which is required for flush recovery.
    const uint64_t generation =
        receiver_enqueue_generation_.fetch_add(1, std::memory_order_acq_rel);
    receiver_filter_generation_.store(generation, std::memory_order_release);
    if constexpr (std::is_copy_constructible_v<T>) {
        for (auto* connection : shared_broadcast_connections_) {
            connection->captureSharedReceiverCancellationScope(generation);
        }
    }
}

template <typename T>
bool InPort<T>::isReceiverCanceled_(const StoredMessage& message) noexcept {
    if (policy_ != PortPolicy::LegacyFastPath) {
        auto key_fn = receiver_key_extractor_.load(std::memory_order_acquire);
        if (!key_fn || stage_state_.slots.empty()) return false;
        const uint64_t key = key_fn(message.data);
        const bool cancel = stage_state_.shouldCancel(message.enqueue_cycle, key);
        if (stageTraceEnabled_() && stageTracePortMatches_(name_)) {
            char slots_buffer[512];
            int offset = 0;
            for (size_t index = 0; index < stage_state_.slots.size() && offset < 400; ++index) {
                offset +=
                    std::snprintf(slots_buffer + offset, sizeof(slots_buffer) - offset,
                                  " s%zu{fc=%lu,keep=[%lu,%lu]}", index,
                                  static_cast<unsigned long>(stage_state_.slots[index].flush_cycle),
                                  static_cast<unsigned long>(stage_state_.slots[index].min_keep),
                                  static_cast<unsigned long>(stage_state_.slots[index].max_keep));
            }
            if (offset == 0) slots_buffer[0] = '\0';
            std::fprintf(stderr,
                         "[STAGE] check   port=%s enq_cyc=%lu key=%lu result=%s live=%zu%s\n",
                         name_.c_str(), static_cast<unsigned long>(message.enqueue_cycle),
                         static_cast<unsigned long>(key), cancel ? "CANCEL" : "pass ",
                         stage_state_.active_slot_count(), slots_buffer);
        }
        return cancel;
    }

    auto key_fn = receiver_key_extractor_.load(std::memory_order_acquire);
    if (!key_fn) return false;
    const uint64_t filter_generation = receiver_filter_generation_.load(std::memory_order_acquire);
    if (filter_generation != std::numeric_limits<uint64_t>::max() &&
        message.receiver_generation_snapshot > filter_generation) {
        return false;
    }

    const uint64_t key = key_fn(message.data);
    const uint64_t min_keep = receiver_min_keep_key_.load(std::memory_order_acquire);
    const uint64_t max_keep = receiver_max_keep_key_.load(std::memory_order_acquire);
    return key < min_keep || key > max_keep;
}

}  // namespace chronon::sender
