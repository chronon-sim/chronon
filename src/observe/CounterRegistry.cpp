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

size_t alignedCounterRecordSize(size_t unit_len, size_t counter_len) noexcept {
    const size_t data_size = 8 + 2 + unit_len + 2 + counter_len + 8;
    const size_t record_size = sizeof(ObservationQueue::RecordHeader) + data_size;
    return (record_size + 7) & ~size_t{7};
}

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

void encodeCounterRecord(std::byte* dest, uint64_t cycle, std::string_view unit_name,
                         std::string_view counter_name, uint64_t value,
                         size_t aligned_size) noexcept {
    auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(dest);
    header->total_size = static_cast<uint16_t>(aligned_size);
    header->type = ObservationQueue::EventType::COUNTER_SNAPSHOT;
    header->flags = 0;
    header->padding = 0;

    std::byte* data = dest + sizeof(ObservationQueue::RecordHeader);
    size_t offset = 0;
    std::memcpy(data + offset, &cycle, sizeof(cycle));
    offset += sizeof(cycle);

    const auto unit_len = static_cast<uint16_t>(unit_name.size());
    std::memcpy(data + offset, &unit_len, sizeof(unit_len));
    offset += sizeof(unit_len);
    std::memcpy(data + offset, unit_name.data(), unit_len);
    offset += unit_len;

    const auto counter_len = static_cast<uint16_t>(counter_name.size());
    std::memcpy(data + offset, &counter_len, sizeof(counter_len));
    offset += sizeof(counter_len);
    std::memcpy(data + offset, counter_name.data(), counter_len);
    offset += counter_len;
    std::memcpy(data + offset, &value, sizeof(value));
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
    if (!has_owner) return;
    owner_snapshot_plans_.resize(max_owner + 1);

    for (const auto& ctx : contexts) {
        if (!ctx || ctx->counterOwnerId() == SIZE_MAX) continue;
        auto& plan = owner_snapshot_plans_[ctx->counterOwnerId()];
        auto& counters = ctx->counters().counters();
        for (size_t i = 0; i < counters.size(); ++i) {
            const auto id = makeCounterId(static_cast<uint32_t>(i));
            const auto& info = ctx->counters().info(id);
            if (info.name.empty()) continue;
            const size_t aligned =
                alignedCounterRecordSize(ctx->unitName().size(), info.name.size());
            plan.entries.push_back({ctx->unitName(), info.name, &counters[i], nullptr, aligned});
            plan.total_size += aligned;
        }

        constexpr size_t num_channels = static_cast<size_t>(ObservationChannel::NumChannels);
        const auto& stats = ctx->observationStats();
        for (size_t i = 0; i < num_channels; ++i) {
            const auto channel = static_cast<ObservationChannel>(i);
            if (!ctx->filter().shouldObserve(observationStatsCategory(channel))) continue;
            const auto& channel_stats = stats.get(channel);
            const std::string prefix = std::string("obs_") + ObservationStats::channelName(channel);
            const std::string emitted_name = prefix + "_emitted";
            const size_t emitted_size =
                alignedCounterRecordSize(ctx->unitName().size(), emitted_name.size());
            plan.entries.push_back(
                {ctx->unitName(), emitted_name, nullptr, &channel_stats.emitted, emitted_size});
            plan.total_size += emitted_size;

            const std::string dropped_name = prefix + "_dropped";
            const size_t dropped_size =
                alignedCounterRecordSize(ctx->unitName().size(), dropped_name.size());
            plan.entries.push_back(
                {ctx->unitName(), dropped_name, nullptr, &channel_stats.dropped, dropped_size});
            plan.total_size += dropped_size;
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
        for (const auto& entry : plan.entries) {
            encodeCounterRecord(write_pos, cycle, entry.unit_name, entry.counter_name,
                                entry.value(), entry.aligned_size);
            write_pos += entry.aligned_size;
        }
        queue.finishAndCommitWrite(plan.total_size);
        for (const auto& entry : plan.entries) {
            if (entry.counter) entry.counter->reset();
        }
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
