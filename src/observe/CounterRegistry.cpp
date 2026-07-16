// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "CounterRegistry.hpp"

#include <cstring>
#include <thread>
#include <unordered_set>

#include "../chronon/CpuPause.hpp"
#include "ObservationContext.hpp"
#include "ObservationQueue.hpp"
#include "ThreadContext.hpp"
#include "ThreadContextManager.hpp"

namespace chronon::observe {

void CounterRegistry::reregisterAll(
    const std::vector<std::unique_ptr<ObservationContext>>& contexts) {
    counters_.clear();
    derived_defs_.clear();
    for (const auto& ctx : contexts) {
        if (ctx) {
            ctx->registerAllCounters(this);
            for (const auto& def : ctx->derivedCounterDefs()) {
                derived_defs_.push_back(def);
            }
        }
    }
    rebuildOwnerSnapshotPlans_(contexts);
}

namespace {

constexpr CategoryMask observationStatsCategory(ObservationChannel channel) noexcept {
    switch (channel) {
        case ObservationChannel::Trace:
            return category::TRACE | category::USER_CATEGORY_MASK;
        case ObservationChannel::Debug:
            return category::LOG_DEBUG;
        case ObservationChannel::Info:
            return category::LOG_INFO;
        case ObservationChannel::Warn:
            return category::LOG_WARN;
        case ObservationChannel::Error:
            return category::LOG_ERROR;
        case ObservationChannel::NumChannels:
            return category::NONE;
    }
    return category::NONE;
}

constexpr size_t batchRecordSize(size_t entry_count) noexcept {
    return sizeof(ObservationQueue::RecordHeader) + sizeof(CounterSnapshotBatchHeader) +
           entry_count * sizeof(uint64_t);
}

constexpr size_t maxBatchEntries() noexcept {
    return (UINT16_MAX - sizeof(ObservationQueue::RecordHeader) -
            sizeof(CounterSnapshotBatchHeader)) /
           sizeof(uint64_t);
}

template <typename Callback>
void forEachSnapshotEntry(ObservationContext& ctx, Callback&& callback) {
    auto& counters = ctx.counters().counters();
    for (size_t i = 0; i < counters.size(); ++i) {
        const auto id = makeCounterId(static_cast<uint32_t>(i));
        const auto& info = ctx.counters().info(id);
        if (info.name.empty()) continue;
        callback(ctx.unitName(), info.name, &counters[i], nullptr);
    }

    constexpr size_t num_channels = static_cast<size_t>(ObservationChannel::NumChannels);
    const auto& stats = ctx.observationStats();
    for (size_t i = 0; i < num_channels; ++i) {
        const auto channel = static_cast<ObservationChannel>(i);
        if (!ctx.filter().shouldObserve(observationStatsCategory(channel))) continue;
        const auto& channel_stats = stats.get(channel);
        const std::string prefix = std::string("obs_") + ObservationStats::channelName(channel);
        callback(ctx.unitName(), prefix + "_emitted", nullptr, &channel_stats.emitted);
        callback(ctx.unitName(), prefix + "_dropped", nullptr, &channel_stats.dropped);
    }
}

}  // namespace

void CounterRegistry::rebuildOwnerSnapshotPlans_(
    const std::vector<std::unique_ptr<ObservationContext>>& contexts) {
    size_t max_owner = 0;
    bool has_owner = false;
    for (const auto& ctx : contexts) {
        if (ctx && ctx->counterOwnerId() != SIZE_MAX) {
            max_owner = std::max(max_owner, ctx->counterOwnerId());
            has_owner = true;
        }
    }

    owner_snapshot_plans_.clear();
    snapshot_plan_metadata_.clear();
    counter_column_metadata_.clear();
    if (has_owner) owner_snapshot_plans_.resize(max_owner + 1);

    for (const auto& ctx : contexts) {
        if (!ctx) continue;
        OwnerSnapshotPlan* owner_plan = nullptr;
        if (ctx->counterOwnerId() != SIZE_MAX) {
            owner_plan = &owner_snapshot_plans_[ctx->counterOwnerId()];
        }
        forEachSnapshotEntry(
            *ctx, [&](const std::string& unit_name, const std::string& counter_name,
                      SimpleCounter* counter, const uint64_t* scalar) {
                counter_column_metadata_.push_back({unit_name, counter_name});
                if (owner_plan) {
                    owner_plan->entries.push_back({unit_name, counter_name, counter, scalar});
                }
            });
    }

    // Split only to respect RecordHeader::total_size. Normal owner plans fit
    // in one record, so a periodic boundary publishes one queue event rather
    // than one event per counter.
    for (auto& plan : owner_snapshot_plans_) {
        size_t begin = 0;
        while (begin < plan.entries.size()) {
            const size_t count = std::min(maxBatchEntries(), plan.entries.size() - begin);
            const uint32_t plan_id = static_cast<uint32_t>(snapshot_plan_metadata_.size());
            const size_t record_size = batchRecordSize(count);

            CounterSnapshotPlanMetadata metadata;
            metadata.entries.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                const auto& entry = plan.entries[begin + i];
                metadata.entries.push_back({entry.unit_name, entry.counter_name});
            }
            snapshot_plan_metadata_.push_back(std::move(metadata));
            plan.batches.push_back({plan_id, begin, count, record_size});
            plan.total_size += record_size;
            begin += count;
        }
    }
}

bool CounterRegistry::pushOwnerSnapshots(uint64_t cycle, std::span<const size_t> owner_ids,
                                         ThreadContext& thread_context) noexcept {
    auto& queue = thread_context.queue();
    bool all_pushed = true;
    for (size_t owner : owner_ids) {
        if (owner >= owner_snapshot_plans_.size()) continue;
        auto& plan = owner_snapshot_plans_[owner];
        if (plan.total_size == 0) continue;

        if (plan.total_size > queue.capacity()) {
            thread_context.incrementDropped();
            all_pushed = false;
            continue;
        }

        // Ownership can migrate at the same boundary that two workers notice
        // it. Claim the nominal cycle before touching the counter storage so
        // exactly one SPSC producer snapshots and resets this cluster.
        uint64_t prior = plan.last_pushed_cycle->load(std::memory_order_relaxed);
        while ((prior == UINT64_MAX || prior < cycle) &&
               !plan.last_pushed_cycle->compare_exchange_weak(
                   prior, cycle, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }
        if (prior != UINT64_MAX && prior >= cycle) continue;
        const uint64_t claimed_from = prior;

        std::byte* ptr = queue.prepareWrite(plan.total_size);
        if (!ptr) {
            queue.forceCommitWrite();
            (void)ThreadContextManager::instance().wakeBackend();
            constexpr uint32_t max_spins = 4096;
            for (uint32_t spin = 0; spin < max_spins && !ptr; ++spin) {
                if (spin > 64) {
                    std::this_thread::yield();
                } else {
                    cpuPause();
                }
                ptr = queue.prepareWrite(plan.total_size);
            }
        }
        if (!ptr) {
            uint64_t expected = cycle;
            (void)plan.last_pushed_cycle->compare_exchange_strong(
                expected, claimed_from, std::memory_order_relaxed, std::memory_order_relaxed);
            thread_context.incrementDropped();
            all_pushed = false;
            continue;
        }

        std::byte* write_pos = ptr;
        for (const auto& batch : plan.batches) {
            auto* record = reinterpret_cast<ObservationQueue::RecordHeader*>(write_pos);
            record->total_size = static_cast<uint16_t>(batch.record_size);
            record->type = ObservationQueue::EventType::COUNTER_SNAPSHOT;
            record->flags = COUNTER_SNAPSHOT_BATCH_FLAG;
            record->padding = 0;

            CounterSnapshotBatchHeader batch_header{cycle, batch.plan_id,
                                                    static_cast<uint32_t>(batch.entry_count)};
            std::byte* data = write_pos + sizeof(ObservationQueue::RecordHeader);
            std::memcpy(data, &batch_header, sizeof(batch_header));
            auto* values = reinterpret_cast<uint64_t*>(data + sizeof(batch_header));
            for (size_t i = 0; i < batch.entry_count; ++i) {
                auto& entry = plan.entries[batch.entry_begin + i];
                values[i] = entry.value();
                // The scheduler owner is the sole writer. Fold capture and
                // interval reset into one cache-friendly pass.
                if (entry.counter) entry.counter->reset();
            }
            write_pos += batch.record_size;
        }
        queue.finishAndCommitWrite(plan.total_size);
    }
    queue.forceCommitWrite();
    (void)ThreadContextManager::instance().wakeBackend();
    return all_pushed;
}

uint64_t CounterRegistry::nextOwnerSnapshotCycle(size_t owner_id, uint64_t run_start,
                                                 uint64_t period) const noexcept {
    if (period == 0 || owner_id >= owner_snapshot_plans_.size()) return UINT64_MAX;
    const auto& plan = owner_snapshot_plans_[owner_id];
    if (plan.total_size == 0) return UINT64_MAX;

    uint64_t prior = plan.last_pushed_cycle->load(std::memory_order_acquire);
    if (prior == UINT64_MAX || prior <= run_start) prior = run_start;
    const uint64_t quotient = prior / period;
    if (quotient >= UINT64_MAX / period) return UINT64_MAX;
    return (quotient + 1) * period;
}

void CounterRegistry::dumpFinalSnapshot(
    uint64_t cycle, ObservationQueue* queue,
    const std::vector<std::unique_ptr<ObservationContext>>& contexts) {
    if (!queue) {
        return;
    }

    // A run may end exactly on a periodic boundary. The owner snapshots have
    // already reset those counters, so the final dump must not overwrite the
    // interval row with zero-valued records at the same nominal cycle.
    std::unordered_set<const SimpleCounter*> counters_already_pushed;
    std::unordered_set<const uint64_t*> scalars_already_pushed;
    for (const auto& plan : owner_snapshot_plans_) {
        if (plan.last_pushed_cycle->load(std::memory_order_relaxed) != cycle) continue;
        for (const auto& entry : plan.entries) {
            if (entry.counter) counters_already_pushed.insert(entry.counter);
            if (entry.scalar) scalars_already_pushed.insert(entry.scalar);
        }
    }

    struct SnapshotEntry {
        const std::string* unit_name;
        std::string counter_name;
        uint64_t value;
        size_t aligned_size;
    };

    auto alignedRecordSize = [](size_t unit_len, size_t ctr_len) -> size_t {
        size_t data_size = 8 + 2 + unit_len + 2 + ctr_len + 8;
        size_t record_size = sizeof(ObservationQueue::RecordHeader) + data_size;
        return (record_size + 7) & ~7;
    };

    std::vector<SnapshotEntry> entries;
    entries.reserve(counters_.size());
    size_t total_size = 0;

    for (const auto& [key, counter_ptr] : counters_) {
        if (key.counter_name.empty() || counters_already_pushed.contains(counter_ptr)) {
            continue;
        }
        size_t aligned = alignedRecordSize(key.unit_name.length(), key.counter_name.length());
        entries.push_back({&key.unit_name, key.counter_name, counter_ptr->get(), aligned});
        total_size += aligned;
    }

    constexpr size_t num_ch = static_cast<size_t>(ObservationChannel::NumChannels);
    for (const auto& ctx : contexts) {
        if (!ctx) {
            continue;
        }
        const auto& stats = ctx->observationStats();
        if (stats.totalEmitted() == 0 && stats.totalDropped() == 0) {
            continue;
        }
        const std::string& unit_name = ctx->unitName();
        for (size_t i = 0; i < num_ch; ++i) {
            auto ch = static_cast<ObservationChannel>(i);
            const auto& ch_stats = stats.get(ch);
            const char* ch_name = ObservationStats::channelName(ch);

            if (ch_stats.emitted > 0 && !scalars_already_pushed.contains(&ch_stats.emitted)) {
                std::string ctr_name = std::string("obs_") + ch_name + "_emitted";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.emitted, aligned});
                total_size += aligned;
            }
            if (ch_stats.dropped > 0 && !scalars_already_pushed.contains(&ch_stats.dropped)) {
                std::string ctr_name = std::string("obs_") + ch_name + "_dropped";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.dropped, aligned});
                total_size += aligned;
            }
        }
    }

    if (entries.empty()) {
        return;
    }

    auto* ptr = queue->prepareWrite(total_size);
    if (ptr == nullptr) {
        if (total_size > queue->capacity()) {
            queue->incrementDropped();
            return;
        }
        constexpr uint32_t max_spins = 4096;
        uint32_t spins = 0;
        do {
            if (++spins > 64) {
                std::this_thread::yield();
            } else {
                cpuPause();
            }
            if ((spins & 0xFFu) == 0u) {
                (void)ThreadContextManager::instance().wakeBackend();
            }
            ptr = queue->prepareWrite(total_size);
        } while (ptr == nullptr && spins < max_spins);

        if (ptr == nullptr) {
            queue->incrementDropped();
            return;
        }
    }

    std::byte* write_pos = ptr;
    for (const auto& entry : entries) {
        auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(write_pos);
        header->total_size = static_cast<uint16_t>(entry.aligned_size);
        header->type = ObservationQueue::EventType::COUNTER_SNAPSHOT;
        header->flags = 0;
        header->padding = 0;

        std::byte* data = write_pos + sizeof(ObservationQueue::RecordHeader);
        size_t offset = 0;

        std::memcpy(data + offset, &cycle, 8);
        offset += 8;

        uint16_t unit_len = static_cast<uint16_t>(entry.unit_name->length());
        std::memcpy(data + offset, &unit_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.unit_name->data(), unit_len);
        offset += unit_len;

        uint16_t ctr_len = static_cast<uint16_t>(entry.counter_name.length());
        std::memcpy(data + offset, &ctr_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.counter_name.data(), ctr_len);
        offset += ctr_len;

        std::memcpy(data + offset, &entry.value, 8);

        write_pos += entry.aligned_size;
    }

    queue->finishAndCommitWrite(total_size);

    for (const auto& [key, counter_ptr] : counters_) {
        counter_ptr->reset();
    }
}

}  // namespace chronon::observe
